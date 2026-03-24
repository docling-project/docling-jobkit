"""Ray Orchestrator - Main orchestrator class with fair task scheduling."""

import asyncio
import base64
import logging
import uuid
import warnings
from typing import Any, Optional

import ray
from ray import serve

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.convert.manager import DoclingConverterManager
from docling_jobkit.datamodel.callback import CallbackSpec
from docling_jobkit.datamodel.chunking import BaseChunkerOptions, ChunkingExportOptions
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
from docling_jobkit.datamodel.result import DoclingTaskResult
from docling_jobkit.datamodel.s3_coords import S3Coordinates
from docling_jobkit.datamodel.task import Task, TaskSource
from docling_jobkit.datamodel.task_meta import TaskStatus, TaskType
from docling_jobkit.datamodel.task_targets import TaskTarget
from docling_jobkit.orchestrators._redis_gate import RedisCallerGate
from docling_jobkit.orchestrators.base_orchestrator import (
    BaseOrchestrator,
    OrchestratorError,
)
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.dispatcher import RayTaskDispatcher
from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager
from docling_jobkit.orchestrators.ray.serve_deployment import deploy_processor

_log = logging.getLogger(__name__)


def _parse_memory_string(memory_str: str) -> int:
    """Parse memory string like '10GB' to bytes.

    Args:
        memory_str: Memory string (e.g., "10GB", "512MB", "1024KB")

    Returns:
        Memory size in bytes

    Raises:
        ValueError: If format is invalid
    """
    import re

    match = re.match(r"(\d+(?:\.\d+)?)\s*([KMGT]?B?)", memory_str.upper())
    if not match:
        raise ValueError(f"Invalid memory format: {memory_str}")

    value, unit = match.groups()
    value = float(value)

    multipliers = {
        "B": 1,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
    }

    return int(value * multipliers.get(unit, 1))


class QueueLimitExceededError(OrchestratorError):
    """Raised when user queue limit is exceeded and rejection is enabled."""


class RayOrchestrator(BaseOrchestrator):
    """Ray + Redis orchestrator with fair task distribution.

    Features:
    - Fair round-robin scheduling at task level
    - Per-user task queues in Redis
    - Configurable resource limits (concurrent + optional queue limits)
    - Optional 429 rejection when queue limits exceeded
    - Ray Serve for autoscaling document processing with persistent converters
    - Redis for state management (supports HA Redis with Sentinel/Cluster)
    - Fault tolerance with automatic retries and recovery

    Architecture:
    1. Tasks are enqueued to per-user Redis queues
    2. Ray Task Dispatcher (Ray Actor) pulls one task per user per round
    3. Tasks are processed using Ray Serve with autoscaling replicas
    4. Results are stored in Redis with configurable TTL
    5. Updates are published via Redis pub/sub
    """

    def __init__(  # noqa: C901
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
            log_level=config.log_level,
        )

        # Initialize Ray if not already initialized
        if not ray.is_initialized():
            _log.info(f"Initializing Ray with address: {config.ray_address}")

            # Handle mTLS certificate generation if enabled
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

                    # Generate TLS certificates
                    # Note: This requires the Ray cluster to be deployed with TLS enabled
                    # and the CA secret to exist in the namespace
                    _log.info("Calling generate_tls_cert()...")
                    generate_cert.generate_tls_cert(
                        config.ray_cluster_name,
                        config.ray_namespace,
                    )
                    _log.info("✓ TLS certificates generated successfully")

                    # Export environment variables for Ray mTLS
                    _log.info("Calling export_env()...")
                    generate_cert.export_env(
                        config.ray_cluster_name,
                        config.ray_namespace,
                    )
                    _log.info("✓ Environment variables exported")

                    # Log and verify the environment variables that were set
                    _log.info("=== mTLS Environment Variables ===")
                    ray_use_tls = os.environ.get("RAY_USE_TLS", "NOT SET")
                    ray_tls_server_cert = os.environ.get(
                        "RAY_TLS_SERVER_CERT", "NOT SET"
                    )
                    ray_tls_server_key = os.environ.get("RAY_TLS_SERVER_KEY", "NOT SET")
                    ray_tls_ca_cert = os.environ.get("RAY_TLS_CA_CERT", "NOT SET")

                    _log.info(f"RAY_USE_TLS: {ray_use_tls}")
                    _log.info(f"RAY_TLS_SERVER_CERT: {ray_tls_server_cert}")
                    _log.info(f"RAY_TLS_SERVER_KEY: {ray_tls_server_key}")
                    _log.info(f"RAY_TLS_CA_CERT: {ray_tls_ca_cert}")

                    # Verify certificate files exist
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

                except ImportError as e:
                    raise ImportError(
                        "codeflare-sdk is required for mTLS support. "
                        f"Install with: pip install docling-jobkit[ray]. Error: {e}"
                    )
                except Exception as e:
                    _log.error(
                        f"Failed to generate mTLS certificates. "
                        f"Cluster: {config.ray_cluster_name}, "
                        f"Namespace: {config.ray_namespace}. "
                        f"Error: {e}"
                    )
                    raise RuntimeError(
                        f"Failed to generate mTLS certificates: {e}. "
                        f"Ensure the Ray cluster '{config.ray_cluster_name}' exists in namespace "
                        f"'{config.ray_namespace}' and has TLS enabled with a CA secret."
                    )

            # Prepare init kwargs
            init_kwargs: dict[str, Any] = {
                "address": config.ray_address,
                "namespace": config.ray_namespace,
                "runtime_env": config.ray_runtime_env,
            }

            # Add object store memory if configured
            if config.ray_object_store_memory:
                try:
                    memory_bytes = _parse_memory_string(config.ray_object_store_memory)
                    init_kwargs["object_store_memory"] = memory_bytes
                    _log.info(
                        f"Setting Ray object store memory to {config.ray_object_store_memory}"
                    )
                except ValueError as e:
                    _log.warning(f"Invalid ray_object_store_memory format: {e}")

            _log.info("=== Ray Initialization Starting ===")
            _log.info(f"Ray Address: {config.ray_address}")
            _log.info(f"Ray Namespace: {config.ray_namespace}")
            _log.info(f"mTLS Enabled: {config.enable_mtls}")
            if config.ray_runtime_env:
                _log.info(f"Runtime Env: {config.ray_runtime_env}")
            if config.ray_object_store_memory:
                _log.info(f"Object Store Memory: {config.ray_object_store_memory}")

            _log.info("Calling ray.init()...")
            try:
                ray.init(**init_kwargs)
                _log.info("✓ Ray initialized successfully")
                _log.info(f"Ray Version: {ray.__version__}")
                try:
                    dashboard_url = ray.get_dashboard_url()  # type: ignore
                    if dashboard_url:
                        _log.info(f"Ray Dashboard: {dashboard_url}")
                except Exception:
                    pass  # Dashboard URL not available in all configurations
            except Exception as e:
                _log.error("=" * 60)
                _log.error("✗ FAILED TO INITIALIZE RAY")
                _log.error("=" * 60)
                _log.error(f"Error Type: {type(e).__name__}")
                _log.error(f"Error Message: {e}")
                _log.error(f"Ray Address: {config.ray_address}")
                _log.error(f"Ray Namespace: {config.ray_namespace}")

                if config.enable_mtls:
                    import os
                    from pathlib import Path

                    _log.error("--- mTLS Configuration ---")
                    _log.error(f"Cluster Name: {config.ray_cluster_name}")
                    _log.error(
                        f"RAY_USE_TLS: {os.environ.get('RAY_USE_TLS', 'NOT SET')}"
                    )

                    for env_var in [
                        "RAY_TLS_SERVER_CERT",
                        "RAY_TLS_SERVER_KEY",
                        "RAY_TLS_CA_CERT",
                    ]:
                        path_str = os.environ.get(env_var, "NOT SET")
                        if path_str and path_str != "NOT SET":
                            exists = Path(path_str).exists()
                            _log.error(f"{env_var}: {path_str} (exists: {exists})")
                        else:
                            _log.error(f"{env_var}: NOT SET")

                _log.error("=" * 60)
                _log.error("Troubleshooting Tips:")
                _log.error(
                    "1. Verify Ray cluster is running: kubectl get raycluster -n <namespace>"
                )
                _log.error(
                    "2. Check Ray head service: kubectl get svc <cluster>-head-svc"
                )
                _log.error("3. Test connectivity: nc -zv <ray-address> <port>")
                if config.enable_mtls:
                    _log.error("4. Verify Ray cluster has TLS enabled (RAY_USE_TLS=1)")
                    _log.error("5. Check certificate files exist and are readable")
                _log.error("=" * 60)
                raise
        else:
            _log.info("Ray already initialized")

        # Initialize Ray Serve if not already running
        try:
            serve.start(detached=True)
            _log.info("Ray Serve started")
        except RuntimeError:
            _log.info("Ray Serve already running")

        # Deploy document processor with Ray Serve
        _log.info("Deploying document processor with Ray Serve")
        self.deployment_handle = deploy_processor(
            converter_manager_config=converter_manager.config,
            config=config,
            redis_url=config.redis_url,
            deployment_name="docling_processor",
        )

        # Deploy Ray Task Dispatcher as Ray Actor
        _log.info("Deploying Ray Task Dispatcher as Ray Actor")
        self.dispatcher = RayTaskDispatcher.options(  # type: ignore[attr-defined]
            max_restarts=config.dispatcher_max_restarts,
            max_task_retries=config.dispatcher_max_task_retries,
        ).remote(config, self.deployment_handle)

        # Pub/sub listener task
        self._pubsub_task: Optional[asyncio.Task] = None
        self._dispatcher_task: Optional[asyncio.Task] = None

        # Configure logging level
        _log.setLevel(config.log_level.upper())
        logging.getLogger("docling_jobkit.orchestrators.ray").setLevel(
            config.log_level.upper()
        )

        _log.info("RayOrchestrator initialized with Ray Serve")

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
            metadata: Optional metadata dict (e.g., {"user_id": "user123"})

        Returns:
            Created task

        Raises:
            QueueLimitExceededError: If queue limit exceeded and rejection enabled
        """
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

            # Convert DocumentStream sources to FileSource for JSON serialization
            ray_sources: list[TaskSource] = []
            for source in sources:
                if isinstance(source, DocumentStream):
                    encoded_doc = base64.b64encode(source.stream.read()).decode()
                    ray_sources.append(
                        FileSource(filename=source.name, base64_string=encoded_doc)
                    )
                elif isinstance(source, (HttpSource, FileSource, S3Coordinates)):
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

            user_id = task.metadata.get("user_id", "default")
            _log.info(
                f"Enqueueing task {task_id} for user {user_id} with {len(sources)} documents"
            )

            can_enqueue, reason = await self.redis_manager.check_user_can_enqueue(
                user_id, len(sources)
            )
            if not can_enqueue:
                if self.config.enable_queue_limit_rejection:
                    _log.warning(f"Rejecting task for user {user_id}: {reason}")
                    raise QueueLimitExceededError(
                        f"Queue limit exceeded for user {user_id}: {reason}"
                    )
                _log.warning(
                    f"User {user_id} exceeding limits but enqueueing: {reason}"
                )

            await self.redis_manager.set_task_metadata(
                task_id=task_id,
                user_id=user_id,
                status=TaskStatus.PENDING,
            )
            await self.redis_manager.set_task_dispatch_state(task_id, "queued")
            await self.redis_manager.enqueue_task(user_id, task)
            await self.init_task_tracking(task)

            _log.debug(f"Task {task_id} enqueued successfully")
            return task

    async def queue_size(self) -> int:
        """Get total queue size across all users.

        Returns:
            Total number of queued tasks
        """
        users = await self.redis_manager.get_all_users_with_tasks()
        total_size = 0

        for user_id in users:
            size = await self.redis_manager.get_user_queue_size(user_id)
            total_size += size

        return total_size

    async def get_queue_position(self, task_id: str) -> Optional[int]:
        """Get position in queue for a specific task.

        Note: This is approximate due to fair scheduling - tasks from different
        users are interleaved, so position depends on other users' queues.

        Args:
            task_id: Task identifier

        Returns:
            Approximate queue position or None if not found
        """
        async with self._redis_gate.acquire(self.config.redis_gate_wait_timeout):
            metadata = await self.redis_manager.get_task_metadata(task_id)
            if not metadata:
                return None

            user_id = metadata.get("user_id")
            if not user_id:
                return None

            queue_size = await self.redis_manager.get_user_queue_size(user_id)
            return queue_size if queue_size > 0 else None

    async def task_result(
        self,
        task_id: str,
    ) -> Optional[DoclingTaskResult]:
        """Retrieve task result from Redis.

        Args:
            task_id: Task identifier

        Returns:
            Task result or None if not found
        """
        async with self._redis_gate.acquire(self.config.redis_gate_wait_timeout):
            return await self.redis_manager.get_task_result(task_id)

    async def process_queue(self):
        """Start the dispatcher and pub/sub listener.

        This starts:
        1. Fair Task Dispatcher loop (Ray Actor)
        2. Redis pub/sub listener for task updates
        """
        # Ensure Redis is connected
        await self.redis_manager.connect()

        _log.info("Starting Fair Ray Orchestrator queue processing")

        # Start dispatcher loop in Ray Actor
        _log.info("Starting Fair Task Dispatcher")
        self._dispatcher_task = asyncio.create_task(self._start_dispatcher())

        # Start pub/sub listener
        _log.info("Starting Redis pub/sub listener")
        self._pubsub_task = asyncio.create_task(self._listen_for_updates())

        # Wait for both tasks
        await asyncio.gather(
            self._dispatcher_task,
            self._pubsub_task,
        )

    async def _start_dispatcher(self):
        """Start the Fair Task Dispatcher Ray Actor."""
        try:
            # This is a blocking call that runs the dispatcher loop
            await self.dispatcher.start_dispatching.remote()
        except Exception as e:
            _log.error(f"Dispatcher failed: {e}", exc_info=True)
            raise

    async def _listen_for_updates(self):
        """Listen for task updates via Redis pub/sub.

        Updates local task tracking based on pub/sub messages.
        """
        try:
            async for update in self.redis_manager.subscribe_to_updates():
                task_id = update.task_id

                if task_id in self.tasks:
                    task = self.tasks[task_id]
                    task.set_status(update.task_status)

                    if update.error_message:
                        task.error_message = update.error_message

                    if update.progress:
                        task.processing_meta = update.progress

                    # Notify subscribers if notifier is configured
                    if self.notifier:
                        try:
                            await self.notifier.notify_task_subscribers(task_id)
                            await self.notifier.notify_queue_positions()
                        except Exception as e:
                            _log.error(f"Notifier error for task {task_id}: {e}")

                    _log.debug(f"Updated task {task_id} status to {update.task_status}")

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
        if not ray.is_initialized():
            raise OrchestratorError("Ray is not initialized")

        # Check dispatcher health
        try:
            is_active = await self.dispatcher.is_active.remote()
            _log.debug(f"Dispatcher active: {is_active}")
        except Exception as e:
            raise OrchestratorError(f"Dispatcher health check failed: {e}")

        _log.info("All connections healthy")

    async def shutdown(self):
        """Shutdown the orchestrator gracefully.

        Stops the dispatcher, closes Redis connections, and cleans up Ray resources.
        """
        _log.info("Shutting down RayOrchestrator")

        # Stop dispatcher
        try:
            await self.dispatcher.stop_dispatching.remote()
        except Exception as e:
            _log.error(f"Error stopping dispatcher: {e}")

        # Cancel pub/sub listener
        if self._pubsub_task:
            self._pubsub_task.cancel()
            try:
                await self._pubsub_task
            except asyncio.CancelledError:
                pass

        # Cancel dispatcher task
        if self._dispatcher_task:
            self._dispatcher_task.cancel()
            try:
                await self._dispatcher_task
            except asyncio.CancelledError:
                pass

        # Disconnect from Redis
        await self.redis_manager.disconnect()

        # Shutdown Ray Serve deployment
        try:
            serve.delete("docling_processor")
            _log.info("Ray Serve deployment deleted")
        except Exception as e:
            _log.warning(f"Error deleting Ray Serve deployment: {e}")

        _log.info("RayOrchestrator shutdown complete")

    async def get_stats(self) -> dict:
        """Get orchestrator statistics.

        Returns:
            Dictionary with orchestrator stats
        """
        users = await self.redis_manager.get_all_users_with_tasks()

        # Get dispatcher stats
        try:
            dispatcher_stats = await self.dispatcher.get_stats.remote()
        except Exception as e:
            _log.error(f"Failed to get dispatcher stats: {e}")
            dispatcher_stats = {}

        stats = {
            "orchestrator_type": "ray",
            "total_tasks": len(self.tasks),
            "users_with_tasks": len(users),
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
