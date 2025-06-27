import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any, Optional, Union

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
from docling_jobkit.datamodel.task_meta import TaskStatus

if TYPE_CHECKING:
    from docling_jobkit.orchestrators.local.orchestrator import LocalOrchestrator

_log = logging.getLogger(__name__)


class AsyncLocalWorker:
    def __init__(self, worker_id: int, orchestrator: "LocalOrchestrator"):
        self.worker_id = worker_id
        self.orchestrator = orchestrator

    async def loop(self):
        _log.debug(f"Starting loop for worker {self.worker_id}")
        while True:
            task_id: str = await self.orchestrator.task_queue.get()
            self.orchestrator.queue_list.remove(task_id)

            if task_id not in self.orchestrator.tasks:
                raise RuntimeError(f"Task {task_id} not found.")
            task = self.orchestrator.tasks[task_id]

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
                def run_conversion():
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
                    results = self.orchestrator.cm.convert_documents(
                        sources=convert_sources,
                        options=task.options,
                        headers=headers,
                    )

                    # The real processing will happen here
                    return list(results)

                start_time = time.monotonic()

                # Run the prediction in a thread to avoid blocking the event loop.
                # Get the current event loop
                # loop = asyncio.get_event_loop()
                # future = asyncio.run_coroutine_threadsafe(
                #     run_conversion(),
                #     loop=loop
                # )
                # response = future.result()

                # Run in a thread
                response = await asyncio.to_thread(
                    run_conversion,
                )
                processing_time = time.monotonic() - start_time

                task.results = response
                task.sources = []

                task.set_status(TaskStatus.SUCCESS)
                _log.info(
                    f"Worker {self.worker_id} completed job {task_id} "
                    f"in {processing_time:.2f} seconds"
                )

            except Exception as e:
                _log.error(
                    f"Worker {self.worker_id} failed to process job {task_id}: {e}"
                )
                task.set_status(TaskStatus.FAILURE)

            finally:
                if self.orchestrator.notifier:
                    await self.orchestrator.notifier.notify_task_subscribers(task_id)
                self.orchestrator.task_queue.task_done()
                _log.debug(f"Worker {self.worker_id} completely done with {task_id}")
