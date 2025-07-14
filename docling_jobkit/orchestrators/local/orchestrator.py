import asyncio
import logging
import uuid
from typing import Optional

from pydantic import BaseModel

from docling.datamodel.base_models import InputFormat

from docling_jobkit.convert.manager import DoclingConverterManager
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.task import Task, TaskSource
from docling_jobkit.datamodel.task_targets import TaskTarget
from docling_jobkit.orchestrators.base_orchestrator import BaseOrchestrator
from docling_jobkit.orchestrators.local.worker import AsyncLocalWorker

_log = logging.getLogger(__name__)


class LocalOrchestratorConfig(BaseModel):
    num_workers: int = 2


class LocalOrchestrator(BaseOrchestrator):
    def __init__(
        self,
        config: LocalOrchestratorConfig,
        converter_manager: DoclingConverterManager,
    ):
        super().__init__()
        self.config = config
        self.task_queue: asyncio.Queue[str] = asyncio.Queue()
        self.queue_list: list[str] = []
        self.cm = converter_manager

    async def enqueue(
        self,
        sources: list[TaskSource],
        options: ConvertDocumentsOptions,
        target: TaskTarget,
    ) -> Task:
        task_id = str(uuid.uuid4())
        task = Task(task_id=task_id, sources=sources, options=options, target=target)
        await self.init_task_tracking(task)

        self.queue_list.append(task_id)
        await self.task_queue.put(task_id)
        return task

    async def queue_size(self) -> int:
        return self.task_queue.qsize()

    async def get_queue_position(self, task_id: str) -> Optional[int]:
        return (
            self.queue_list.index(task_id) + 1 if task_id in self.queue_list else None
        )

    async def process_queue(self):
        # Create a pool of workers
        workers = []
        for i in range(self.config.num_workers):
            _log.debug(f"Starting worker {i}")
            w = AsyncLocalWorker(i, self)
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

    async def clear_converters(self):
        self.cm.clear_cache()
