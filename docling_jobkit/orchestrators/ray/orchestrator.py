"""Ray Orchestrator - Main orchestrator class with fair task scheduling."""

import asyncio
import base64
import logging
import time
import uuid
import warnings
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Optional

import ray
from ray import serve
from ray.serve.schema import ApplicationStatus

from docling.datamodel.base_models import DocumentStream
from docling.datamodel.service.callbacks import CallbackSpec
from docling.datamodel.service.chunking import BaseChunkerOptions
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.sources import FileSource, HttpSource
from docling.datamodel.service.targets import (
    AzureBlobTarget,
    GoogleCloudStorageTarget,
    GoogleDriveTarget,
    S3Target,
)
from docling.datamodel.service.tasks import TaskType

from docling_jobkit.convert.manager import DoclingConverterManager
from docling_jobkit.convert.source_expansion import _EXPANDABLE_SOURCE_TYPES
from docling_jobkit.datamodel.chunking import ChunkingExportOptions
from docling_jobkit.datamodel.result import DoclingTaskResult, TaskOutcome
from docling_jobkit.datamodel.stored_outcome import (
    StoredSuccessOutcome,
)
from docling_jobkit.datamodel.task import Task, TaskSource
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.datamodel.task_targets import TaskTarget
from docling_jobkit.orchestrators._redis_gate import RedisCallerGate
from docling_jobkit.orchestrators.base_orchestrator import (
    BaseOrchestrator,
    OrchestratorError,
    TaskNotFoundError,
)
from docling_jobkit.orchestrators.ray.config import (
    RayOrchestratorConfig,
    parse_memory_bytes,
)
from docling_jobkit.orchestrators.ray.dispatcher import RayTaskDispatcher
from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager
from docling_jobkit.orchestrators.ray.serve_deployment import (
    DEFAULT_SERVE_APP_NAME,
    deploy_processor,
)

_log = logging.getLogger(__name__)


_STORAGE_TARGET_TYPES = (
    S3Target,
    AzureBlobTarget,
    GoogleCloudStorageTarget,
    GoogleDriveTarget,
)


def _validate_expandable_source_targets(
    sources: list[TaskSource], target: TaskTarget, task_type: TaskType
) -> None:
    has_expandable_source = any(
        isinstance(source, _EXPANDABLE_SOURCE_TYPES) for source in sources
    )
    if not has_expandable_source:
        return

    if task_type != TaskType.CONVERT or not isinstance(target, _STORAGE_TARGET_TYPES):
        raise ValueError(
            "Tasks containing an expandable storage source require a storage target."
        )


class QueueLimitExceededError(OrchestratorError):
    """Raised when tenant queue limit is exceeded and rejection is enabled."""


class DispatcherUnavailableError(OrchestratorError):
    """Raised when the shared Ray dispatcher cannot accept new work."""


class RayOrchestrator(BaseOrchestrator):
    """Ray + Redis orchestrator with fair task distribution.

    Features:
    - Fair round-robin scheduling at task level
    - Per-tenant task queues in Redis
    - Configurable resource limits (concurrent + optional queue limits)
    - Optional 429 rejection when queue limits exceeded
    - Ray Serve for autoscaling document processing with persistent converters
    - Redis for state management (supports HA Redis with Sentinel/Cluster)
    - Fault tolerance with automatic retries and recovery

    Architecture:
    1. Tasks are enqueued to per-tenant Redis queues
    2. Ray Task Dispatcher (Ray Actor) pulls one task per tenant per round
    3. Tasks are processed using Ray Serve with autoscaling replicas
    4. Results are stored in Redis with configurable TTL
    5. Updates are published via Redis pub/sub
    """

    def __init__(
        self,
        config: RayOrchestratorConfig,
        converter_manager: DoclingConverterManager,
    ):
        """Initialize Ray Orchestrator.

        Args:
            config: Orchestrator configuration
            converter_manager: Document converter manager (config used for Ray Serve deployment)
        """
        super().__init__()
        self.config = config
        self.allowed_target_kinds = config.allowed_target_kinds
        self.cm = converter_manager
        assert self.config.redis_gate_concurrency is not None
        self._redis_gate = RedisCallerGate(self.config.redis_gate_concurrency)

        # Initialize Redis state manager
        self.redis_manager = RedisStateManager(
            redis_url=config.redis_url,
            results_ttl=config.results_ttl,
            results_prefix=config.results_prefix,
            sub_channel=config.sub_channel,
            max_connections=config.redis_max_connections,
            socket_timeout=config.redis_socket_timeout,
            socket_connect_timeout=config.redis_socket_connect_timeout,
            max_concurrent_tasks=config.max_concurrent_tasks,
            max_queued_tasks=config.max_queued_tasks,
            max_documents=config.max_documents,
            task_timeout=config.task_timeout,
            dispatcher_interval=config.dispatcher_interval,
            log_level=config.log_level,
        )

        # Pub/sub listener task
        self._pubsub_task: Optional[asyncio.Task] = None
        self._dispatcher_supervisor_task: Optional[asyncio.Task] = None
        self.deployment_handle: Optional[Any] = None
        self.dispatcher: Optional[Any] = None
        self.dispatcher_name = "docling_task_dispatcher"
        self.serve_app_name = DEFAULT_SERVE_APP_NAME
        self._ray_admin_executor: Optional[ThreadPoolExecutor] = None

        # Configure logging level
        _log.setLevel(config.log_level.upper())
        logging.getLogger("docling_jobkit.orchestrators.ray").setLevel(
            config.log_level.upper()
        )

        self._unhealthy_since: Optional[float] = None
        self._ray_session_needs_restart: bool = False
        _log.info("RayOrchestrator initialized without connecting to Ray")

    def _get_ray_admin_executor(self) -> ThreadPoolExecutor:
        executor = self._ray_admin_executor
        if executor is None:
            executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="docling-ray-admin",
            )
            self._ray_admin_executor = executor
        return executor

    async def _run_ray_admin(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        bound = partial(func, *args, **kwargs)
        return await loop.run_in_executor(self._get_ray_admin_executor(), bound)

    def _build_ray_init_kwargs(self) -> dict[str, Any]:
        """Build Ray init kwargs and perform mTLS setup when enabled."""
        config = self.config

        _log.info(f"Initializing Ray with address: {config.ray_address}")

        if config.enable_mtls:
            if not config.ray_cluster_name:
                raise ValueError(
                    "ray_cluster_name must be provided when enable_mtls is True"
                )

            _log.info(
                f"Generating mTLS certificates for cluster: {config.ray_cluster_name}, "
                f"namespace: {config.ray_namespace}"
            )

            try:
                import os
                from pathlib import Path

                from codeflare_sdk import generate_cert

                _log.info("=== mTLS Certificate Generation Starting ===")
                _log.info(f"Cluster Name: {config.ray_cluster_name}")
                _log.info(f"Namespace: {config.ray_namespace}")

                _log.info("Calling generate_tls_cert()...")
                # WARNING: force_regenerate=True is intentional. The default (False) skips
                # regeneration if cert files already exist on disk, which causes a permanent
                # TLS handshake failure (CERTIFICATE_VERIFY_FAILED) when the RayCluster is
                # recreated: the operator rotates the CA secret while docling-serve may have
                # cached certs from the previous CA. Force-regenerating on every reconnect
                # ensures we always read the current CA from Kubernetes. If the secret does
                # not exist yet (mid-rotation), the call raises and the supervisor retries.
                generate_cert.generate_tls_cert(
                    config.ray_cluster_name,
                    config.ray_namespace,
                    force_regenerate=True,
                )
                _log.info("✓ TLS certificates generated successfully")

                _log.info("Calling export_env()...")
                generate_cert.export_env(
                    config.ray_cluster_name,
                    config.ray_namespace,
                )
                _log.info("✓ Environment variables exported")

                _log.info("=== mTLS Environment Variables ===")
                ray_use_tls = os.environ.get("RAY_USE_TLS", "NOT SET")
                ray_tls_server_cert = os.environ.get("RAY_TLS_SERVER_CERT", "NOT SET")
                ray_tls_server_key = os.environ.get("RAY_TLS_SERVER_KEY", "NOT SET")
                ray_tls_ca_cert = os.environ.get("RAY_TLS_CA_CERT", "NOT SET")

                _log.info(f"RAY_USE_TLS: {ray_use_tls}")
                _log.info(f"RAY_TLS_SERVER_CERT: {ray_tls_server_cert}")
                _log.info(f"RAY_TLS_SERVER_KEY: {ray_tls_server_key}")
                _log.info(f"RAY_TLS_CA_CERT: {ray_tls_ca_cert}")

                _log.info("=== Verifying Certificate Files ===")
                cert_files = {
                    "Server Cert": ray_tls_server_cert,
                    "Server Key": ray_tls_server_key,
                    "CA Cert": ray_tls_ca_cert,
                }

                all_files_exist = True
                for name, path_str in cert_files.items():
                    if path_str and path_str != "NOT SET":
                        cert_path = Path(path_str)
                        exists = cert_path.exists()
                        if exists:
                            size = cert_path.stat().st_size
                            _log.info(f"✓ {name}: {path_str} (size: {size} bytes)")
                        else:
                            _log.error(f"✗ {name}: {path_str} (FILE NOT FOUND)")
                            all_files_exist = False
                    else:
                        _log.error(f"✗ {name}: Environment variable not set")
                        all_files_exist = False

                if not all_files_exist:
                    raise RuntimeError(
                        "mTLS certificate files are missing or environment variables not set properly"
                    )

                _log.info("=== mTLS Setup Complete ===")

            except ImportError as exc:
                raise ImportError(
                    "codeflare-sdk is required for mTLS support. "
                    f"Install with: pip install docling-jobkit[ray]. Error: {exc}"
                ) from exc
            except Exception as exc:
                _log.error(
                    f"Failed to generate mTLS certificates. "
                    f"Cluster: {config.ray_cluster_name}, "
                    f"Namespace: {config.ray_namespace}. "
                    f"Error: {exc}"
                )
                raise RuntimeError(
                    f"Failed to generate mTLS certificates: {exc}. "
                    f"Ensure the Ray cluster '{config.ray_cluster_name}' exists in namespace "
                    f"'{config.ray_namespace}' and has TLS enabled with a CA secret."
                ) from exc

        init_kwargs: dict[str, Any] = {
            "address": config.ray_address,
            "namespace": config.ray_namespace,
            "runtime_env": config.ray_runtime_env,
        }

        if config.ray_object_store_memory:
            try:
                memory_bytes = parse_memory_bytes(config.ray_object_store_memory)
                init_kwargs["object_store_memory"] = memory_bytes
                _log.info(
                    f"Setting Ray object store memory to {config.ray_object_store_memory}"
                )
            except ValueError as exc:
                _log.warning(f"Invalid ray_object_store_memory format: {exc}")

        return init_kwargs

    def _bind_dispatcher(self) -> Any:
        """Bind to the named detached dispatcher actor for this namespace."""
        if self.deployment_handle is None:
            raise DispatcherUnavailableError("Ray runtime is not initialized")

        dispatcher_kwargs: dict[str, Any] = {
            "name": self.dispatcher_name,
            "lifetime": "detached",
            "get_if_exists": True,
            "num_cpus": self.config.dispatcher_num_cpus,
            "max_restarts": self.config.dispatcher_max_restarts,
            "max_task_retries": self.config.dispatcher_max_task_retries,
        }
        if self.config.dispatcher_memory_request is not None:
            dispatcher_kwargs["memory"] = parse_memory_bytes(
                self.config.dispatcher_memory_request
            )

        _log.info("Binding to named Ray Task Dispatcher actor")
        return RayTaskDispatcher.options(  # type: ignore[attr-defined]
            **dispatcher_kwargs
        ).remote(self.config, self.deployment_handle)

    async def _get_existing_serve_app_state(
        self, app_name: str
    ) -> tuple[bool, ApplicationStatus | None]:
        """Query the Serve control plane to determine app presence and lifecycle state.

        Returns (False, None) if the app is absent.
        Returns (True, status) if the app exists.
        Raises DispatcherUnavailableError if the Serve control plane is unreachable.
        """
        try:
            serve_status = await self._run_ray_admin(serve.status)
        except Exception as exc:
            _log.warning("serve_control_plane_unavailable: %s", exc)
            raise DispatcherUnavailableError(
                f"Ray Serve control plane unavailable: {exc}"
            ) from exc

        app_overview = serve_status.applications.get(app_name)
        if app_overview is None:
            return (False, None)
        return (True, app_overview.status)

    async def _initialize_ray_runtime(self) -> None:
        """Initialize Ray client then attach to an existing Serve app or deploy if absent.

        Uses serve.status() to disambiguate control-plane failure from app absence so
        that a pod restart never triggers serve.run() when Serve is merely unavailable.
        Decision table:
          - control plane unreachable  → DispatcherUnavailableError
          - app absent                 → deploy once
          - app DEPLOYING/RUNNING/DEPLOY_FAILED/UNHEALTHY → attach only
          - app DELETING               → DispatcherUnavailableError (retry later)
          - app present, attach fails  → DispatcherUnavailableError (no deploy fallback)
        """
        if self.dispatcher is not None and self.deployment_handle is not None:
            return

        config = self.config
        app_name = self.serve_app_name

        try:
            if self._ray_session_needs_restart:
                # The supervisor confirmed the Serve control plane was unreachable, meaning
                # the Ray client session is pointing at a dead cluster. Shut it down so
                # ray.init() below reconnects to the (possibly new) cluster.
                _log.info(
                    "Stale Ray session confirmed by supervisor; restarting for fresh reconnect"
                )
                self._ray_session_needs_restart = False
                self.dispatcher = None
                self.deployment_handle = None
                try:
                    await self._run_ray_admin(ray.shutdown)
                except Exception as exc:
                    _log.warning(
                        "ray.shutdown() during recovery failed (continuing): %s", exc
                    )

            if not await self._run_ray_admin(ray.is_initialized):
                init_kwargs = await asyncio.to_thread(self._build_ray_init_kwargs)

                _log.info("=== Ray Initialization Starting ===")
                _log.info(f"Ray Address: {config.ray_address}")
                _log.info(f"Ray Namespace: {config.ray_namespace}")
                _log.info(f"mTLS Enabled: {config.enable_mtls}")
                if config.ray_runtime_env:
                    _log.info(f"Runtime Env: {config.ray_runtime_env}")
                if config.ray_object_store_memory:
                    _log.info(f"Object Store Memory: {config.ray_object_store_memory}")

                _log.info("Calling ray.init()...")
                await self._run_ray_admin(ray.init, **init_kwargs)
                _log.info("✓ Ray initialized successfully")
                _log.info(f"Ray Version: {ray.__version__}")
                try:
                    get_dashboard_url = ray.get_dashboard_url  # type: ignore[attr-defined]
                    dashboard_url = await self._run_ray_admin(get_dashboard_url)
                    if dashboard_url:
                        _log.info(f"Ray Dashboard: {dashboard_url}")
                except Exception:
                    pass
            else:
                _log.info("Ray already initialized")

            try:
                await self._run_ray_admin(serve.start, detached=True)
                _log.info("Ray Serve started")
            except RuntimeError:
                _log.info("Ray Serve already running")

            app_present, app_status = await self._get_existing_serve_app_state(app_name)

            if not app_present:
                _log.info("serve_app_absent_deploying: app=%s", app_name)
                self.deployment_handle = await self._run_ray_admin(
                    deploy_processor,
                    converter_manager_config=self.cm.config,
                    config=config,
                    redis_url=config.redis_url,
                    app_name=app_name,
                )
            elif app_status == ApplicationStatus.DELETING:
                _log.warning("serve_app_deleting_retry_later: app=%s", app_name)
                raise DispatcherUnavailableError(
                    f"Serve app '{app_name}' is being deleted; will retry"
                )
            else:
                _log.info(
                    "serve_app_present_attaching: app=%s status=%s",
                    app_name,
                    app_status,
                )
                try:
                    self.deployment_handle = await self._run_ray_admin(
                        serve.get_app_handle, app_name
                    )
                except Exception as exc:
                    _log.warning(
                        "serve_app_attach_failed: app=%s error=%s", app_name, exc
                    )
                    raise DispatcherUnavailableError(
                        f"Serve app '{app_name}' exists but attach failed: {exc}"
                    ) from exc

            self.dispatcher = await self._run_ray_admin(self._bind_dispatcher)
            _log.info("Ray runtime initialized")
        except asyncio.CancelledError:
            raise
        except DispatcherUnavailableError:
            self.dispatcher = None
            self.deployment_handle = None
            raise
        except BaseException as exc:
            self.dispatcher = None
            self.deployment_handle = None
            try:
                await self._run_ray_admin(ray.shutdown)
            except Exception as shutdown_exc:
                _log.warning(
                    "ray.shutdown() in init cleanup failed (continuing): %s",
                    shutdown_exc,
                )
            raise DispatcherUnavailableError(
                f"Ray runtime initialization failed: {exc}"
            ) from exc

    async def _refresh_dispatcher_runtime(self) -> None:
        """Refresh dispatcher runtime state without allowing the supervisor to hang forever."""
        rpc_timeout = self.config.dispatcher_rpc_timeout
        try:
            dispatcher = self.dispatcher
            if dispatcher is None:
                dispatcher = self._bind_dispatcher()
                self.dispatcher = dispatcher

            await asyncio.wait_for(
                dispatcher.refresh_runtime.remote(self.deployment_handle, self.config),
                timeout=rpc_timeout,
            )
        except asyncio.TimeoutError as exc:
            self.dispatcher = None
            raise DispatcherUnavailableError(
                f"Ray dispatcher runtime refresh timed out after {rpc_timeout}s"
            ) from exc
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self.dispatcher = None
            raise DispatcherUnavailableError(
                f"Ray dispatcher runtime refresh failed: {exc}"
            ) from exc

    async def ensure_dispatcher_ready(self) -> None:
        """Verify that the named dispatcher actor is reachable and healthy.

        The health-check RPC is bounded by config.dispatcher_rpc_timeout so this
        method never hangs when the Ray head is gone.

        """
        rpc_timeout = self.config.dispatcher_rpc_timeout
        try:
            dispatcher = self.dispatcher
            if dispatcher is None:
                raise DispatcherUnavailableError("Ray dispatcher not yet initialized")
            loop_running = await asyncio.wait_for(
                dispatcher.get_health.remote(), timeout=rpc_timeout
            )
        except asyncio.TimeoutError as exc:
            raise DispatcherUnavailableError(
                f"Ray dispatcher health check timed out after {rpc_timeout}s"
            ) from exc
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise DispatcherUnavailableError(
                f"Ray dispatcher is unavailable: {exc}"
            ) from exc

        if not loop_running:
            raise DispatcherUnavailableError("Ray dispatcher loop is not running")

    async def _supervise_dispatcher(self) -> None:
        """Keep Ray runtime initialized, dispatcher binding refreshed, and health verified.

        Handles the full lifecycle from initial Ray init through steady-state health
        checks, so process_queue() can start the supervisor without waiting for Ray to
        be available. Tracks continuous unhealthiness duration so is_liveness_healthy()
        can report failure after the configured deadline.
        """
        poll_interval = self.config.supervisor_poll_interval

        while True:
            try:
                if self.deployment_handle is None:
                    await self._initialize_ray_runtime()
                if self.dispatcher is None:
                    await self._refresh_dispatcher_runtime()
                await self.ensure_dispatcher_ready()
                if self._unhealthy_since is not None:
                    _log.info(
                        "Ray dispatcher recovered after %.1fs of unhealthiness",
                        time.monotonic() - self._unhealthy_since,
                    )
                    self._unhealthy_since = None
                await asyncio.sleep(poll_interval)
            except asyncio.CancelledError:
                raise
            except DispatcherUnavailableError as exc:
                if self._unhealthy_since is None:
                    self._unhealthy_since = time.monotonic()
                    _log.warning(
                        "Ray dispatcher became unhealthy; liveness deadline in %.0fs: %s",
                        self.config.liveness_fail_after,
                        exc,
                    )
                else:
                    _log.warning("Dispatcher supervisor retrying after error: %s", exc)
                self.dispatcher = None
                try:
                    app_present, _ = await asyncio.wait_for(
                        self._get_existing_serve_app_state(self.serve_app_name),
                        timeout=self.config.dispatcher_rpc_timeout,
                    )
                    if not app_present:
                        _log.warning(
                            "Serve app absent on reachable cluster; clearing handle to re-deploy"
                        )
                        self.deployment_handle = None
                except (DispatcherUnavailableError, asyncio.TimeoutError):
                    _log.warning(
                        "Serve control plane unreachable; will restart Ray session for re-init"
                    )
                    self.deployment_handle = None
                    self._ray_session_needs_restart = True
                await asyncio.sleep(1.0)

    def is_liveness_healthy(self) -> bool:
        """Return False once the dispatcher has been continuously unhealthy past the deadline."""
        if self._unhealthy_since is None:
            return True
        return (
            time.monotonic() - self._unhealthy_since
        ) < self.config.liveness_fail_after

    async def _task_from_redis(self, task_id: str) -> Optional[Task]:
        """Rebuild or refresh a task from durable Redis metadata."""
        metadata = await self.redis_manager.get_task_metadata_model(task_id)
        if metadata is None:
            return None

        task = self.tasks.get(metadata.task_id)
        if task is None:
            task = metadata.to_task()
            await self.init_task_tracking(task)
            return task

        task.task_type = metadata.task_type
        task.task_status = metadata.status
        task.metadata["tenant_id"] = metadata.tenant_id
        task.error_message = metadata.error_message
        task.failure = metadata.failure
        task.created_at = metadata.created_at
        task.started_at = metadata.started_at
        task.finished_at = metadata.finished_at
        task.last_update_at = metadata.last_update_at
        return task

    async def enqueue(
        self,
        sources: list[TaskSource],
        target: TaskTarget,
        task_type: TaskType = TaskType.CONVERT,
        options: ConvertDocumentsOptions | None = None,
        convert_options: ConvertDocumentsOptions | None = None,
        chunking_options: BaseChunkerOptions | None = None,
        chunking_export_options: ChunkingExportOptions | None = None,
        callbacks: list[CallbackSpec] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Task:
        """Enqueue a task for processing.

        Args:
            sources: List of document sources to process
            target: Target for processed documents
            task_type: Type of task (CONVERT or CHUNK)
            options: Deprecated, use convert_options
            convert_options: Conversion options
            chunking_options: Chunking options (for CHUNK tasks)
            chunking_export_options: Chunking export options
            callbacks: List of callback specifications
            metadata: Optional metadata dict (e.g., {"tenant_id": "tenant123"})

        Returns:
            Created task

        Raises:
            QueueLimitExceededError: If queue limit exceeded and rejection enabled
            TargetNotAllowedError: If the target kind is not permitted
        """
        self._validate_target(target)
        async with self._redis_gate.acquire(self.config.redis_gate_wait_timeout):
            # Ensure Redis is connected
            await self.redis_manager.connect()

            if options is not None and convert_options is None:
                convert_options = options
                warnings.warn(
                    "'options' is deprecated and will be removed in a future version. "
                    "Use 'convert_options' instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )

            # Create task
            task_id = str(uuid.uuid4())
            chunking_export_options = chunking_export_options or ChunkingExportOptions()

            _validate_expandable_source_targets(sources, target, task_type)

            # Convert DocumentStream sources to FileSource for JSON serialization
            ray_sources: list[TaskSource] = []
            for source in sources:
                if isinstance(source, DocumentStream):
                    encoded_doc = base64.b64encode(source.stream.read()).decode()
                    ray_sources.append(
                        FileSource(filename=source.name, base64_string=encoded_doc)
                    )
                elif isinstance(
                    source, (HttpSource, FileSource, *_EXPANDABLE_SOURCE_TYPES)
                ):
                    ray_sources.append(source)

            task = Task(
                task_id=task_id,
                task_type=task_type,
                sources=ray_sources,
                target=target,
                convert_options=convert_options,
                chunking_options=chunking_options,
                chunking_export_options=chunking_export_options,
                callbacks=callbacks or [],
                metadata=metadata or {},
            )

            tenant_id = task.metadata.get("tenant_id", "default")
            _log.info(
                f"Enqueueing task {task_id} for tenant {tenant_id} with {len(sources)} documents"
            )

            can_enqueue, reason = await self.redis_manager.check_tenant_can_enqueue(
                tenant_id, len(sources)
            )
            if not can_enqueue:
                if self.config.enable_queue_limit_rejection:
                    _log.warning(f"Rejecting task for tenant {tenant_id}: {reason}")
                    raise QueueLimitExceededError(
                        f"Queue limit exceeded for tenant {tenant_id}: {reason}"
                    )
                _log.warning(
                    f"Tenant {tenant_id} exceeding limits but enqueueing: {reason}"
                )

            # Admission does NOT perform a live Ray RPC. Redis is the durable
            # queue: the task is safely enqueued the moment enqueue_task()
            # returns, and the supervisor (_supervise_dispatcher) owns dispatcher
            # health, rebind, and dispatch-loop restart. A per-request
            # get_health.remote() converged every replica's storm onto the single
            # detached actor, and a slow probe both manufactured 503s and (by
            # holding the Redis gate across the RPC) throttled enqueue throughput.
            # We instead refuse new work only once the dispatcher has been
            # continuously unavailable past the liveness deadline, using a cheap
            # in-memory check (no RPC, no lock, no actor round-trip).
            if not self.is_liveness_healthy():
                raise DispatcherUnavailableError(
                    "Ray dispatcher has been unavailable past the liveness "
                    "deadline; refusing new work until it recovers."
                )
            await self.redis_manager.set_task_metadata(
                task_id=task_id,
                tenant_id=tenant_id,
                task_type=task_type,
                task_size=len(sources),
                status=TaskStatus.PENDING,
            )
            await self.redis_manager.enqueue_task(tenant_id, task)
            await self.init_task_tracking(task)

            if self.dispatcher is not None:
                self.dispatcher.wake.remote(tenant_id=tenant_id)

            _log.debug(f"Task {task_id} enqueued successfully")
            return task

    async def queue_size(self) -> int:
        """Get total queue size across all tenants.

        Returns:
            Total number of queued tasks
        """
        tenants = await self.redis_manager.get_all_tenants_with_tasks()
        total_size = 0

        for tenant_id in tenants:
            size = await self.redis_manager.get_tenant_queue_size(tenant_id)
            total_size += size

        return total_size

    async def get_queue_position(self, task_id: str) -> Optional[int]:
        """Get position in queue for a specific task.

        Note: This is approximate due to fair scheduling - tasks from different
        tenants are interleaved, so position depends on other tenants' queues.

        Args:
            task_id: Task identifier

        Returns:
            Approximate queue position or None if not found
        """
        async with self._redis_gate.acquire(self.config.redis_gate_wait_timeout):
            metadata = await self.redis_manager.get_task_metadata_model(task_id)
            if metadata is None:
                return None

            queue_size = await self.redis_manager.get_tenant_queue_size(
                metadata.tenant_id
            )
            return queue_size if queue_size > 0 else None

    async def get_raw_task(self, task_id: str) -> Task:
        """Get a task from memory first, then fall back to durable Redis state."""
        task = self.tasks.get(task_id)
        if task is not None:
            return task

        redis_task = await self._task_from_redis(task_id)
        if redis_task is None:
            raise TaskNotFoundError()
        return redis_task

    async def task_status(self, task_id: str, wait: float = 0.0) -> Task:
        """Get task status, preferring durable Redis state when available.

        Redis is checked first because a restarted API process may not yet have
        reconstructed the task into local memory. If `wait` is provided, poll
        until completion or until the timeout expires.
        """
        deadline = asyncio.get_running_loop().time() + max(wait, 0.0)

        while True:
            task: Optional[Task]

            # Prefer Redis-backed task state so status remains visible across
            # API/orchestrator restarts before local in-memory maps warm up.
            redis_task = await self._task_from_redis(task_id)
            if redis_task is not None:
                task = redis_task
            else:
                task = self.tasks.get(task_id)
                if task is None:
                    raise TaskNotFoundError()

            if wait <= 0.0 or task.is_completed():
                return task

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0.0:
                return task

            # Use short polling so waiters see state transitions quickly without
            # turning task_status() into a hot loop.
            await asyncio.sleep(min(0.25, remaining))

    async def task_outcome(self, task_id: str) -> Optional[TaskOutcome]:
        async with self._redis_gate.acquire(self.config.redis_gate_wait_timeout):
            return await self.redis_manager.get_task_outcome(task_id)

    async def task_result(self, task_id: str) -> Optional[DoclingTaskResult]:
        outcome = await self.task_outcome(task_id)
        if isinstance(outcome, StoredSuccessOutcome):
            return outcome.result
        if isinstance(outcome, DoclingTaskResult):
            return outcome
        return None

    async def process_queue(self):
        """Start local supervision and pub/sub handling for the shared dispatcher.

        Ray runtime initialization is handled lazily inside the supervisor loop, so
        this method returns quickly even when the Ray head is unavailable at startup.
        """
        await self.redis_manager.connect()

        _log.info("Starting Ray orchestrator queue processing")
        self._dispatcher_supervisor_task = asyncio.create_task(
            self._supervise_dispatcher()
        )
        self._pubsub_task = asyncio.create_task(self._listen_for_updates())

        await asyncio.gather(
            self._dispatcher_supervisor_task,
            self._pubsub_task,
        )

    async def _listen_for_updates(self):
        """Listen for task updates via Redis pub/sub.

        Updates local task tracking based on pub/sub messages.
        """
        try:
            async for update in self.redis_manager.subscribe_to_updates():
                task_id = update.task_id

                try:
                    task = await self.get_raw_task(task_id)
                except TaskNotFoundError:
                    _log.warning(
                        "Dropping update for unknown task %s with no Redis metadata",
                        task_id,
                    )
                    continue

                task.set_status(update.task_status)

                if update.error_message:
                    task.error_message = update.error_message
                if update.failure is not None:
                    task.failure = update.failure

                if update.progress:
                    task.processing_meta = update.progress

                if self.notifier:
                    try:
                        await self.notifier.notify_task_subscribers(task_id)
                        await self.notifier.notify_queue_positions()
                    except Exception as exc:
                        _log.error("Notifier error for task %s: %s", task_id, exc)

                _log.debug("Updated task %s status to %s", task_id, update.task_status)

        except Exception as e:
            _log.error(f"Pub/sub listener failed: {e}", exc_info=True)
            raise

    async def warm_up_caches(self):
        """Warm up Ray actors with DocumentConverter instances.

        This pre-loads models on Ray actors for faster first-task processing.
        """
        _log.info("Warming up Ray actor caches")
        # Implementation depends on Ray Data warm pool configuration
        # For now, this is a placeholder

    async def clear_converters(self):
        """Clear converter caches across Ray actors.

        This can be used to free memory or reload models.
        """
        _log.info("Clearing converter caches on Ray actors")
        # Implementation depends on Ray Data actor management
        # For now, this is a placeholder

    async def check_connection(self):
        """Check Redis and Ray connections.

        Raises:
            OrchestratorError: If connections are not healthy
        """
        # Ensure Redis is connected
        await self.redis_manager.connect()

        # Check Redis
        redis_ok = await self.redis_manager.ping()
        if not redis_ok:
            raise OrchestratorError("Redis connection failed")

        # Check Ray
        if not await self._run_ray_admin(ray.is_initialized):
            raise OrchestratorError("Ray is not initialized")

        try:
            await self.ensure_dispatcher_ready()
        except DispatcherUnavailableError as exc:
            raise OrchestratorError(str(exc)) from exc

        _log.info("All connections healthy")

    async def shutdown(self):
        """Shutdown the orchestrator gracefully.

        Normal shutdown is local-only and intentionally leaves shared Ray
        resources running.
        """
        _log.info("Shutting down RayOrchestrator")

        if self._pubsub_task:
            self._pubsub_task.cancel()
            try:
                await self._pubsub_task
            except asyncio.CancelledError:
                pass

        if self._dispatcher_supervisor_task:
            self._dispatcher_supervisor_task.cancel()
            try:
                await self._dispatcher_supervisor_task
            except asyncio.CancelledError:
                pass

        await self.redis_manager.disconnect()
        executor = self._ray_admin_executor
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
            self._ray_admin_executor = None
        _log.info("RayOrchestrator shutdown complete")

    async def cleanup_shared_runtime_for_tests(self) -> None:
        """Explicit destructive cleanup for test environments only."""
        dispatcher = self.dispatcher
        if dispatcher is not None:
            try:
                await dispatcher.stop_dispatching.remote()
            except Exception as exc:
                _log.warning(
                    "Error stopping shared dispatcher in test cleanup: %s", exc
                )

            try:
                ray.kill(dispatcher, no_restart=True)
            except Exception as exc:
                _log.warning("Error killing shared dispatcher in test cleanup: %s", exc)

            self.dispatcher = None

        try:
            serve.delete(self.serve_app_name)
        except Exception as exc:
            _log.warning("Error deleting Ray Serve deployment in test cleanup: %s", exc)

    async def get_stats(self) -> dict:
        """Get orchestrator statistics.

        Returns:
            Dictionary with orchestrator stats
        """
        tenants = await self.redis_manager.get_all_tenants_with_tasks()

        # Get dispatcher stats
        dispatcher = self.dispatcher
        try:
            if dispatcher is None:
                raise RuntimeError("dispatcher not bound")
            dispatcher_stats = await dispatcher.get_stats.remote()
        except Exception as e:
            _log.error(f"Failed to get dispatcher stats: {e}")
            dispatcher_stats = {}

        stats = {
            "orchestrator_type": "ray",
            "total_tasks": len(self.tasks),
            "tenants_with_tasks": len(tenants),
            "queue_size": await self.queue_size(),
            "dispatcher": dispatcher_stats,
            "config": {
                "max_concurrent_tasks": self.config.max_concurrent_tasks,
                "max_queued_tasks": self.config.max_queued_tasks,
                "enable_queue_limit_rejection": self.config.enable_queue_limit_rejection,
            },
        }

        return stats

    async def on_result_fetched(self, task_id: str) -> None:
        """Set Redis EXPIRE on the result key for crash-safe single-use deletion.

        The base class delete_task() only removes in-memory tracking and closes
        WebSocket connections — it does NOT delete the Redis result key. This
        override corrects that by expiring the key via Redis, with no sleeping
        coroutine.
        """
        result_key = f"{self.config.results_prefix}:task:{task_id}:result"
        await self.redis_manager.expire_result(
            result_key, self.config.result_removal_delay
        )
        await super().delete_task(task_id)
