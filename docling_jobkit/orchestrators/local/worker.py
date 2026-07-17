import asyncio
import logging
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from docling.datamodel.service.tasks import TaskType

from docling_jobkit.convert.chunking import process_chunkable_results
from docling_jobkit.convert.manager import DoclingConverterManager
from docling_jobkit.convert.results import process_exportable_results
from docling_jobkit.convert.source_expansion import expand_task_sources
from docling_jobkit.datamodel.exportable_document import (
    ExportableDocument,
    source_to_public_uri,
)
from docling_jobkit.datamodel.result import DoclingTaskResult
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker
from docling_jobkit.public_errors import build_public_task_error

if TYPE_CHECKING:
    from docling_jobkit.orchestrators.local.orchestrator import LocalOrchestrator

_log = logging.getLogger(__name__)


class AsyncLocalWorker:
    def __init__(
        self,
        worker_id: int,
        orchestrator: "LocalOrchestrator",
        use_shared_manager: bool,
        scratch_dir: Path,
    ):
        self.worker_id = worker_id
        self.orchestrator = orchestrator
        self.use_shared_manager = use_shared_manager
        self.scratch_dir = scratch_dir

    async def loop(self):
        _log.debug(f"Starting loop for worker {self.worker_id}")
        if self.use_shared_manager:
            cm = self.orchestrator.cm
        else:
            cm = DoclingConverterManager(self.orchestrator.cm.config)
            self.orchestrator.worker_cms.append(cm)
        while True:
            task_id: str = await self.orchestrator.task_queue.get()
            self.orchestrator.queue_list.remove(task_id)

            if task_id not in self.orchestrator.tasks:
                raise RuntimeError(f"Task {task_id} not found.")
            task = self.orchestrator.tasks[task_id]
            workdir = self.scratch_dir / task_id

            try:
                task.set_status(TaskStatus.STARTED)
                _log.info(f"Worker {self.worker_id} processing task {task_id}")

                if self.orchestrator.notifier:
                    # Notify clients about task updates
                    await self.orchestrator.notifier.notify_task_subscribers(task_id)

                    # Notify clients about queue updates
                    await self.orchestrator.notifier.notify_queue_positions()

                callback_invoker = CallbackInvoker() if task.callbacks else None

                # Define a callback function to send progress updates to the client.
                def run_task() -> DoclingTaskResult:
                    convert_sources, headers = expand_task_sources(
                        task,
                        max_file_size=cm.config.max_file_size,
                    )
                    # Note: results are only an iterator->lazy evaluation
                    conv_results = cm.convert_documents(
                        sources=convert_sources,
                        options=task.convert_options,
                        headers=headers,
                    )
                    exportable_documents = (
                        ExportableDocument.from_conversion_result(
                            conv_res,
                            source_index=idx,
                            source_uri=(
                                source_to_public_uri(task.sources[idx])
                                if idx < len(task.sources)
                                else str(conv_res.input.file)
                            ),
                        )
                        for idx, conv_res in enumerate(conv_results)
                    )

                    # The real processing will happen here
                    processed_results: DoclingTaskResult
                    if task.task_type == TaskType.CONVERT:
                        processed_results = process_exportable_results(
                            task=task,
                            exportable_documents=exportable_documents,
                            work_dir=workdir,
                            presigned_config=self.orchestrator.config.presigned_config,
                            callback_invoker=callback_invoker,
                        )
                    elif task.task_type == TaskType.CHUNK:
                        processed_results = process_chunkable_results(
                            task=task,
                            exportable_documents=exportable_documents,
                            work_dir=workdir,
                            chunker_manager=self.orchestrator.chunker_manager,
                            callback_invoker=callback_invoker,
                        )
                    else:
                        raise RuntimeError(f"Unsupported task type: {task.task_type}")

                    return processed_results

                # Run in a thread to avoid blocking the event loop.
                task_result = await asyncio.to_thread(
                    run_task,
                )
                self.orchestrator._task_results[task_id] = task_result
                task.sources = []

                task.set_status(TaskStatus.SUCCESS)
                _log.info(
                    f"Worker {self.worker_id} completed job {task_id} "
                    f"in {task_result.processing_time:.2f} seconds"
                )

            except Exception as e:
                _log.error(
                    f"Worker {self.worker_id} failed to process job {task_id}: {e}"
                )
                task.set_status(TaskStatus.FAILURE)
                task.error_message = build_public_task_error(e)

            finally:
                if workdir.exists():
                    _log.debug(f"Cleaning {self.worker_id} workdir for {task_id}")
                    shutil.rmtree(workdir)

                if self.orchestrator.notifier:
                    await self.orchestrator.notifier.notify_task_subscribers(task_id)
                self.orchestrator.task_queue.task_done()

                _log.debug(f"Worker {self.worker_id} completely done with {task_id}")
