"""Ray Serve deployment for document processing with autoscaling."""

import asyncio
import datetime
import gc
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any, Optional, Union

from ray import serve

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.convert.chunking import process_chunk_results
from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.convert.results import process_export_results
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
from docling_jobkit.datamodel.result import DoclingTaskResult
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus, TaskType
from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.logging_utils import (
    configure_ray_actor_logging,
)
from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager

_log = logging.getLogger(__name__)

# Try to import psutil for memory monitoring
try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    _log.warning("psutil not available - memory monitoring disabled")


@serve.deployment
class DocumentProcessorDeployment:
    """Ray Serve deployment for document processing with autoscaling.

    This provides:
    - Automatic autoscaling based on load
    - Built-in load balancing
    - Health checks and monitoring
    - Zero-downtime deployments
    - Persistent DoclingConverterManager for warm model reuse
    """

    def __init__(
        self,
        converter_manager_config: DoclingConverterManagerConfig,
        config: RayOrchestratorConfig,
        redis_url: str,
    ):
        """Initialize the deployment with persistent converter manager.

        Args:
            converter_manager_config: Configuration for DoclingConverterManager
            config: Orchestrator configuration
            redis_url: Redis connection URL for state management
        """
        configure_ray_actor_logging(config.log_level)

        self.config = config
        self.converter_manager_config = converter_manager_config

        # Get replica context for logging
        try:
            replica_context = serve.get_replica_context()
            self.replica_id = replica_context.replica_id
        except RuntimeError:
            self.replica_id = "unknown"

        # Create persistent converter manager (WARM)
        _log.info(f"Replica {self.replica_id}: Initializing DoclingConverterManager")
        self.cm = DoclingConverterManager(config=converter_manager_config)

        # Create Redis state manager for task status updates
        _log.info(f"Replica {self.replica_id}: Initializing Redis connection")
        self.redis_manager = RedisStateManager(
            redis_url=redis_url,
            results_ttl=config.results_ttl,
            log_level=config.log_level,
        )
        # Note: Connection will be established on first use (lazy connection)

        # Scratch directory
        self.scratch_dir = config.scratch_dir or Path(
            tempfile.mkdtemp(prefix=f"docling_serve_{self.replica_id}_")
        )
        self.scratch_dir.mkdir(exist_ok=True, parents=True)

        # Configure logging level for this deployment
        _log.setLevel(self.config.log_level.upper())

        # Statistics
        self.tasks_processed = 0
        self.documents_processed = 0
        self.last_task_time: Optional[datetime.datetime] = None
        self.memory_warnings = 0

        _log.info(f"Replica {self.replica_id}: Initialized successfully")

    async def process_task(self, task: Task) -> DoclingTaskResult:
        """Process a task using the persistent converter manager.

        Args:
            task: Task to process

        Returns:
            Task result
        """
        task_start = datetime.datetime.now(datetime.timezone.utc)
        _log.info(
            f"Replica {self.replica_id}: Processing task {task.task_id} "
            f"with {len(task.sources)} documents"
        )

        # Extract user_id from task metadata (needed for cleanup)
        user_id = task.metadata.get("user_id", "default")

        # Update task status and processing state (actor is now actively processing)
        try:
            # Update task metadata status to STARTED
            await self.redis_manager.update_task_status(
                task.task_id, TaskStatus.STARTED
            )

            # Update processing state to "processing" (used for accurate metrics)
            await self.redis_manager.mark_task_processing(task.task_id, user_id)

            _log.debug(
                f"Replica {self.replica_id}: Task {task.task_id} marked as processing"
            )
        except Exception as e:
            _log.warning(
                f"Replica {self.replica_id}: Failed to update task processing state: {e}"
            )
            # Continue processing even if status update fails

        # Check memory before processing
        if self.config.enable_oom_protection and PSUTIL_AVAILABLE:
            await self._check_memory()

        # Create task-specific work directory
        workdir = self.scratch_dir / task.task_id
        workdir.mkdir(exist_ok=True, parents=True)

        try:
            # Process based on task type
            if task.task_type == TaskType.CONVERT:
                result = await self._process_convert_with_retry(task, workdir)
            elif task.task_type == TaskType.CHUNK:
                result = await self._process_chunk_with_retry(task, workdir)
            else:
                raise ValueError(f"Unknown task type: {task.task_type}")

            # Update statistics
            self.tasks_processed += 1
            self.documents_processed += len(task.sources)
            self.last_task_time = datetime.datetime.now(datetime.timezone.utc)

            duration = (self.last_task_time - task_start).total_seconds()
            _log.info(
                f"Replica {self.replica_id}: Task {task.task_id} completed "
                f"in {duration:.2f}s"
            )

            return result

        finally:
            # Clean up work directory
            if workdir.exists():
                shutil.rmtree(workdir, ignore_errors=True)

            # Clean up processing state (actor created it, actor deletes it)
            try:
                processing_key = f"task:{task.task_id}:processing"
                await self.redis_manager._ensure_redis().delete(processing_key)
                _log.debug(
                    f"Replica {self.replica_id}: Cleaned up processing state for {task.task_id}"
                )
            except Exception as e:
                _log.warning(
                    f"Replica {self.replica_id}: Failed to clean up processing state: {e}"
                )

    async def _process_convert_with_retry(
        self, task: Task, workdir: Path
    ) -> DoclingTaskResult:
        """Process convert task with retry logic."""
        max_retries = self.config.max_task_retries
        retry_delay = self.config.retry_delay
        last_exception: Optional[Exception] = None

        for attempt in range(max_retries + 1):
            try:
                return self._process_convert(task, workdir)
            except Exception as e:
                last_exception = e
                if attempt < max_retries:
                    _log.warning(
                        f"Replica {self.replica_id}: Task {task.task_id} failed "
                        f"(attempt {attempt + 1}/{max_retries + 1}): {e}. "
                        f"Retrying in {retry_delay}s..."
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    _log.error(
                        f"Replica {self.replica_id}: Task {task.task_id} failed "
                        f"after {max_retries + 1} attempts: {e}"
                    )

        raise last_exception or Exception("Task processing failed")

    def _process_convert(self, task: Task, workdir: Path) -> DoclingTaskResult:
        """Process a convert task."""
        # Prepare sources
        convert_sources: list[Union[str, DocumentStream]] = []
        headers: Optional[dict[str, Any]] = None

        for source in task.sources:
            if isinstance(source, DocumentStream):
                convert_sources.append(source)
            elif isinstance(source, FileSource):
                convert_sources.append(source.to_document_stream())
            elif isinstance(source, HttpSource):
                convert_sources.append(str(source.url))
                if headers is None and source.headers:
                    headers = source.headers

        # Initialize callback invoker if needed
        callback_invoker = None
        if task.callbacks:
            callback_invoker = CallbackInvoker(
                max_retries=3,
                timeout=30.0,
                retry_delay=1.0,
            )

        # Convert documents using PERSISTENT converter manager
        convert_opts = task.convert_options or ConvertDocumentsOptions()
        conv_results = self.cm.convert_documents(
            sources=convert_sources,
            options=convert_opts,
            headers=headers,
        )

        # Process and export results
        processed_results = process_export_results(
            task=task,
            conv_results=conv_results,
            work_dir=workdir,
            callback_invoker=callback_invoker,
        )

        return processed_results

    async def _process_chunk_with_retry(
        self, task: Task, workdir: Path
    ) -> DoclingTaskResult:
        """Process chunk task with retry logic."""
        max_retries = self.config.max_task_retries
        retry_delay = self.config.retry_delay
        last_exception: Optional[Exception] = None

        for attempt in range(max_retries + 1):
            try:
                return self._process_chunk(task, workdir)
            except Exception as e:
                last_exception = e
                if attempt < max_retries:
                    _log.warning(
                        f"Replica {self.replica_id}: Task {task.task_id} failed "
                        f"(attempt {attempt + 1}/{max_retries + 1}): {e}. "
                        f"Retrying in {retry_delay}s..."
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    _log.error(
                        f"Replica {self.replica_id}: Task {task.task_id} failed "
                        f"after {max_retries + 1} attempts: {e}"
                    )

        raise last_exception or Exception("Task processing failed")

    def _process_chunk(self, task: Task, workdir: Path) -> DoclingTaskResult:
        """Process a chunk task."""
        # Prepare sources
        convert_sources: list[Union[str, DocumentStream]] = []
        headers: Optional[dict[str, Any]] = None

        for source in task.sources:
            if isinstance(source, DocumentStream):
                convert_sources.append(source)
            elif isinstance(source, FileSource):
                convert_sources.append(source.to_document_stream())
            elif isinstance(source, HttpSource):
                convert_sources.append(str(source.url))
                if headers is None and source.headers:
                    headers = source.headers

        # Initialize callback invoker if needed
        callback_invoker = None
        if task.callbacks:
            callback_invoker = CallbackInvoker(
                max_retries=3,
                timeout=30.0,
                retry_delay=1.0,
            )

        # Convert documents using PERSISTENT converter manager
        convert_opts = task.convert_options or ConvertDocumentsOptions()
        conv_results = self.cm.convert_documents(
            sources=convert_sources,
            options=convert_opts,
            headers=headers,
        )

        # Import chunker manager
        from docling_jobkit.convert.chunking import DocumentChunkerManager

        chunker_manager = DocumentChunkerManager()

        # Process and chunk results
        processed_results = process_chunk_results(
            task=task,
            conv_results=conv_results,
            work_dir=workdir,
            chunker_manager=chunker_manager,
            callback_invoker=callback_invoker,
        )

        return processed_results

    async def _check_memory(self):
        """Check memory usage and warn if approaching limits."""
        if not PSUTIL_AVAILABLE:
            return

        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024

            # Parse memory limit if configured
            if self.config.ray_memory_limit_per_actor:
                limit_str = self.config.ray_memory_limit_per_actor
                # Parse "4GB" -> 4096 MB
                if limit_str.endswith("GB"):
                    limit_mb = float(limit_str[:-2]) * 1024
                elif limit_str.endswith("MB"):
                    limit_mb = float(limit_str[:-2])
                else:
                    limit_mb = None

                threshold = self.config.memory_warning_threshold
                if limit_mb and memory_mb > limit_mb * threshold:
                    self.memory_warnings += 1
                    _log.warning(
                        f"Replica {self.replica_id}: High memory usage: "
                        f"{memory_mb:.0f}MB / {limit_mb:.0f}MB "
                        f"({threshold * 100:.0f}% threshold)"
                    )

                    # If consistently high, trigger garbage collection
                    if self.memory_warnings > 3:
                        _log.warning(
                            f"Replica {self.replica_id}: Triggering garbage collection"
                        )
                        gc.collect()
                        self.memory_warnings = 0

        except Exception as e:
            _log.warning(f"Replica {self.replica_id}: Memory check failed: {e}")

    async def health_check(self) -> dict:
        """Health check endpoint for Ray Serve.

        Returns:
            Health status dictionary
        """
        try:
            # Check memory
            memory_mb = 0.0
            if PSUTIL_AVAILABLE:
                process = psutil.Process()
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / 1024 / 1024

            # Check if converter manager is responsive
            cm_healthy = self.cm is not None

            return {
                "replica_id": self.replica_id,
                "healthy": cm_healthy,
                "tasks_processed": self.tasks_processed,
                "documents_processed": self.documents_processed,
                "memory_mb": memory_mb,
                "memory_warnings": self.memory_warnings,
                "last_task_time": (
                    self.last_task_time.isoformat() if self.last_task_time else None
                ),
            }

        except Exception as e:
            _log.error(f"Replica {self.replica_id}: Health check failed: {e}")
            return {
                "replica_id": self.replica_id,
                "healthy": False,
                "error": str(e),
            }

    async def get_stats(self) -> dict:
        """Get processing statistics.

        Returns:
            Statistics dictionary
        """
        return {
            "replica_id": self.replica_id,
            "tasks_processed": self.tasks_processed,
            "documents_processed": self.documents_processed,
            "memory_warnings": self.memory_warnings,
            "last_task_time": (
                self.last_task_time.isoformat() if self.last_task_time else None
            ),
        }

    async def clear_cache(self):
        """Clear converter cache to free memory."""
        _log.info(f"Replica {self.replica_id}: Clearing converter cache")
        self.cm.clear_cache()
        gc.collect()
        self.memory_warnings = 0


def create_deployment(
    converter_manager_config: DoclingConverterManagerConfig,
    config: RayOrchestratorConfig,
    redis_url: str,
    deployment_name: str = "document_processor",
) -> Any:
    """Create and configure Ray Serve deployment with autoscaling.

    Args:
        converter_manager_config: Configuration for DoclingConverterManager
        config: Orchestrator configuration
        redis_url: Redis connection URL for state management
        deployment_name: Name for the deployment

    Returns:
        Configured Ray Serve deployment
    """
    # Configure autoscaling
    autoscaling_config = {
        "min_replicas": config.min_actors,
        "max_replicas": config.max_actors,
        "target_num_ongoing_requests_per_replica": config.target_requests_per_replica,
        "upscale_delay_s": config.upscale_delay_s,
        "downscale_delay_s": config.downscale_delay_s,
    }

    # Configure resource requirements
    ray_actor_options = {
        "num_cpus": config.ray_num_cpus_per_actor,
    }

    # Parse memory limit if configured
    if config.ray_memory_limit_per_actor:
        limit_str = config.ray_memory_limit_per_actor
        # Convert "4GB" to bytes for Ray
        if limit_str.endswith("GB"):
            memory_bytes = int(float(limit_str[:-2]) * 1024 * 1024 * 1024)
        elif limit_str.endswith("MB"):
            memory_bytes = int(float(limit_str[:-2]) * 1024 * 1024)
        else:
            memory_bytes = int(limit_str)  # Assume bytes
        ray_actor_options["memory"] = memory_bytes

    _log.info(
        f"Creating Ray Serve deployment '{deployment_name}' with autoscaling: "
        f"min={config.min_actors}, max={config.max_actors}, "
        f"target_requests={config.target_requests_per_replica}"
    )

    # Create deployment with configuration
    deployment = DocumentProcessorDeployment.options(  # type: ignore[attr-defined]
        name=deployment_name,
        autoscaling_config=autoscaling_config,
        ray_actor_options=ray_actor_options,
    ).bind(
        converter_manager_config=converter_manager_config,
        config=config,
        redis_url=redis_url,
    )

    return deployment


def deploy_processor(
    converter_manager_config: DoclingConverterManagerConfig,
    config: RayOrchestratorConfig,
    redis_url: str,
    deployment_name: str = "document_processor",
) -> Any:
    """Deploy the document processor to Ray Serve.

    Args:
        converter_manager_config: Configuration for DoclingConverterManager
        config: Orchestrator configuration
        redis_url: Redis connection URL for state management
        deployment_name: Name for the deployment

    Returns:
        Deployment handle for making requests
    """
    deployment = create_deployment(
        converter_manager_config=converter_manager_config,
        config=config,
        redis_url=redis_url,
        deployment_name=deployment_name,
    )

    # Deploy to Ray Serve
    handle = serve.run(
        deployment, name=deployment_name, route_prefix=f"/{deployment_name}"
    )

    _log.info(f"Ray Serve deployment '{deployment_name}' is running")

    return handle
