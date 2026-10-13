import asyncio
import gc
import logging
import tempfile
import uuid
import warnings
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel

from docling.datamodel.base_models import InputFormat
from docling.datamodel.service.callbacks import CallbackSpec
from docling.datamodel.service.chunking import BaseChunkerOptions
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.tasks import TaskType

from docling_jobkit.config.target_config import PresignedConfig
from docling_jobkit.convert.chunking import DocumentChunkerManager
from docling_jobkit.convert.manager import DoclingConverterManager
from docling_jobkit.datamodel.chunking import ChunkingExportOptions
from docling_jobkit.datamodel.result import DoclingTaskResult
from docling_jobkit.datamodel.task import Task, TaskSource, TaskTarget, validate_task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.base_orchestrator import (
    BaseOrchestrator,
    TaskCancellationConflictError,
    TaskNotFoundError,
)
from docling_jobkit.orchestrators.local.worker import AsyncLocalWorker

_log = logging.getLogger(__name__)


class LocalOrchestratorConfig(BaseModel):
    num_workers: int = 2
    shared_models: bool = False
    scratch_dir: Optional[Path] = None
    result_removal_delay: int = 300  # seconds until result is deleted after fetch
    presigned_config: PresignedConfig | None = None
    allowed_target_kinds: Optional[set[str]] = None


class LocalOrchestrator(BaseOrchestrator):
    def __init__(
        self,
        config: LocalOrchestratorConfig,
        converter_manager: DoclingConverterManager,
    ):
        super().__init__()
        self.config = config
        self.allowed_target_kinds = config.allowed_target_kinds
        self.task_queue: asyncio.Queue[str] = asyncio.Queue()
        self.queue_list: list[str] = []
        self.cm = converter_manager
        self.chunker_manager = DocumentChunkerManager()
        self.worker_cms: list[DoclingConverterManager] = []
        self._task_results: dict[str, DoclingTaskResult] = {}
        self._background_tasks: set[asyncio.Task] = set()
        self.scratch_dir = self.config.scratch_dir or Path(
            tempfile.mkdtemp(prefix="docling_")
        )

    async def enqueue(
        self,
        sources: list[TaskSource],
        target: TaskTarget | None = None,
        task_type: TaskType = TaskType.CONVERT,
        options: ConvertDocumentsOptions | None = None,
        convert_options: ConvertDocumentsOptions | None = None,
        chunking_options: BaseChunkerOptions | None = None,
        chunking_export_options: ChunkingExportOptions | None = None,
        callbacks: list[CallbackSpec] | None = None,
        metadata: dict[str, Any] | None = None,
        targets: list[TaskTarget] | None = None,
    ) -> Task:
        if options is not None and convert_options is None:
            convert_options = options
            warnings.warn(
                "'options' is deprecated and will be removed in a future version. "
                "Use 'conversion_options' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        resolved_targets = self._resolve_enqueue_targets(target, targets)
        self._validate_targets(resolved_targets)
        task_id = str(uuid.uuid4())
        chunking_export_options = chunking_export_options or ChunkingExportOptions()
        task = validate_task(
            {
                "task_id": task_id,
                "task_type": task_type,
                "sources": sources,
                "convert_options": convert_options,
                "chunking_options": chunking_options,
                "chunking_export_options": chunking_export_options,
                "targets": resolved_targets,
                "callbacks": callbacks or [],
            },
            allow_external_plugins=self.cm.config.allow_external_plugins,
        )
        await self.init_task_tracking(task)

        self.queue_list.append(task_id)
        await self.task_queue.put(task_id)
        return task

    async def queue_size(self) -> int:
        # queue_list, not the raw asyncio queue: a cancelled task keeps its
        # (undrainable) queue entry until a worker picks it up, and it must not
        # keep occupying queue capacity in the meantime.
        return len(self.queue_list)

    def claim_task_started(self, task_id: str) -> bool:
        """Claim a dequeued task for conversion, or report it as already claimed.

        This is the no-await counterpart of a PENDING -> STARTED claim: every
        ``await`` on the conversion path is preceded by this call, so the check
        and the status write form one indivisible section of the event loop and
        no concurrent ``cancel_queued_task`` can interleave between them.
        Returns False when the task is unknown or no longer PENDING, in which
        case the caller must skip the conversion.
        """
        task = self.tasks.get(task_id)
        if task is None or task.task_status != TaskStatus.PENDING:
            return False
        task.set_status(TaskStatus.STARTED)
        return True

    async def cancel_queued_task(self, task_id: str) -> Task:
        task = self._cancel_pending_task(task_id)
        if self.notifier:
            await self.notifier.notify_task_subscribers(task_id)
            await self.notifier.notify_queue_positions()
        return task

    def _cancel_pending_task(self, task_id: str) -> Task:
        """Terminalize a queued task without ever awaiting, so it races cleanly.

        Mutating the status and dropping the queue entry in one await-free
        section makes cancellation mutually exclusive with
        :meth:`claim_task_started`: exactly one of the two observes PENDING.
        """
        task = self.tasks.get(task_id)
        if task is None:
            raise TaskNotFoundError(task_id)

        if task.task_status == TaskStatus.CANCELLED:
            return task

        reason = self.cancellation_conflict_reason(task.task_status)
        if reason is not None:
            raise TaskCancellationConflictError(reason, task.task_status)

        task.set_status(TaskStatus.CANCELLED)
        if task_id in self.queue_list:
            self.queue_list.remove(task_id)
        return task

    async def get_queue_position(self, task_id: str) -> Optional[int]:
        return (
            self.queue_list.index(task_id) + 1 if task_id in self.queue_list else None
        )

    async def task_result(
        self,
        task_id: str,
    ) -> Optional[DoclingTaskResult]:
        if task_id not in self._task_results:
            return None
        return self._task_results[task_id]

    async def process_queue(self):
        # Create a pool of workers
        workers = []
        for i in range(self.config.num_workers):
            _log.debug(f"Starting worker {i}")
            w = AsyncLocalWorker(
                i,
                self,
                use_shared_manager=self.config.shared_models,
                scratch_dir=self.scratch_dir,
            )
            worker_task = asyncio.create_task(w.loop())
            workers.append(worker_task)

        # Wait for all workers to complete (they won't, as they run indefinitely)
        await asyncio.gather(*workers)
        _log.debug("All workers completed.")

    async def warm_up_caches(self):
        # Converter with default options
        pdf_format_option = self.cm.get_pdf_pipeline_opts(ConvertDocumentsOptions())
        converter = self.cm.get_converter(pdf_format_option)
        converter.initialize_pipeline(InputFormat.PDF)

    async def delete_task(self, task_id: str):
        _log.info(f"Deleting result of task {task_id=}")
        if task_id in self._task_results:
            del self._task_results[task_id]
        await super().delete_task(task_id)

    async def on_result_fetched(self, task_id: str) -> None:
        """Schedule deletion of result after result_removal_delay seconds."""

        async def _delayed_delete():
            await asyncio.sleep(self.config.result_removal_delay)
            await self.delete_task(task_id=task_id)

        bg_task = asyncio.create_task(_delayed_delete())
        self._background_tasks.add(bg_task)
        bg_task.add_done_callback(self._background_tasks.discard)

    async def clear_converters(self):
        self.cm.clear_cache()
        self.chunker_manager.clear_cache()
        for cm in self.worker_cms:
            cm.clear_cache()
        gc.collect()

    async def check_connection(self):
        pass
