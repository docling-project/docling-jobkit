import asyncio
import logging
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.convert.chunking import process_chunk_results
from docling_jobkit.convert.manager import DoclingConverterManager
from docling_jobkit.convert.results import process_export_results
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
from docling_jobkit.datamodel.result import DoclingTaskResult
from docling_jobkit.datamodel.task_meta import TaskStatus, TaskType

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

                # Define a callback function to send progress updates to the client.
                # TODO: send partial updates, e.g. when a document in the batch is done
                def run_task() -> DoclingTaskResult:
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

                    # Note: results are only an iterator->lazy evaluation
                    conv_results = cm.convert_documents(
                        sources=convert_sources,
                        options=task.convert_options,
                        headers=headers,
                    )

                    # The real processing will happen here
                    processed_results: DoclingTaskResult
                    if task.task_type == TaskType.CONVERT:
                        processed_results = process_export_results(
                            task=task,
                            conv_results=conv_results,
                            work_dir=workdir,
                        )
                    elif task.task_type == TaskType.CHUNK:
                        processed_results = process_chunk_results(
                            task=task,
                            conv_results=conv_results,
                            work_dir=workdir,
                            chunker_manager=self.orchestrator.chunker_manager,
                        )

                    return processed_results

                # Run the prediction in a thread to avoid blocking the event loop.
                # Get the current event loop
                # loop = asyncio.get_event_loop()
                # future = asyncio.run_coroutine_threadsafe(
                #     run_conversion(),
                #     loop=loop
                # )
                # response = future.result()

                # Run in a thread
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

            finally:
                if workdir.exists():
                    _log.debug(f"Cleaning {self.worker_id} workdir for {task_id}")
                    shutil.rmtree(workdir)

                if self.orchestrator.notifier:
                    await self.orchestrator.notifier.notify_task_subscribers(task_id)
                self.orchestrator.task_queue.task_done()

                _log.debug(f"Worker {self.worker_id} completely done with {task_id}")
