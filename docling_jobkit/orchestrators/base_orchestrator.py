import datetime
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Union

from docling.datamodel.document import ConversionResult

from docling_jobkit.datamodel.callback import ProgressCallbackRequest
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.task import Task, TaskSource
from docling_jobkit.datamodel.task_targets import TaskTarget

if TYPE_CHECKING:
    from docling_jobkit.orchestrators.base_notifier import BaseNotifier

_log = logging.getLogger(__name__)


class OrchestratorError(Exception):
    pass


class TaskNotFoundError(OrchestratorError):
    pass


class ProgressInvalid(OrchestratorError):
    pass


class BaseOrchestrator(ABC):
    def __init__(self):
        self.tasks: dict[str, Task] = {}
        self.notifier: Optional["BaseNotifier"] = None

    def bind_notifier(self, notifier: "BaseNotifier"):
        self.notifier = notifier

    @abstractmethod
    async def enqueue(
        self,
        sources: list[TaskSource],
        options: ConvertDocumentsOptions,
        target: TaskTarget,
    ) -> Task:
        pass

    @abstractmethod
    async def queue_size(self) -> int:
        pass

    @abstractmethod
    async def get_queue_position(self, task_id: str) -> Optional[int]:
        pass

    @abstractmethod
    async def process_queue(self):
        pass

    @abstractmethod
    async def warm_up_caches(self):
        pass

    @abstractmethod
    async def clear_converters(self):
        pass

    async def init_task_tracking(self, task: Task):
        task_id = task.task_id
        self.tasks[task.task_id] = task
        if self.notifier:
            await self.notifier.add_task(task_id)

    async def get_raw_task(self, task_id: str) -> Task:
        if task_id not in self.tasks:
            raise TaskNotFoundError()
        return self.tasks[task_id]

    async def task_status(self, task_id: str, wait: float = 0.0) -> Task:
        return await self.get_raw_task(task_id=task_id)

    async def task_results(
        # self, task_id: str, background_tasks: BackgroundTasks
        self,
        task_id: str,
    ) -> Union[list[ConversionResult], None]:
        try:
            task = await self.get_raw_task(task_id=task_id)
            return task.results
        except TaskNotFoundError:
            return None

    async def delete_task(self, task_id: str):
        _log.info(f"Deleting {task_id=}")
        if self.notifier:
            await self.notifier.remove_task(task_id=task_id)
        if task_id in self.tasks:
            del self.tasks[task_id]

    async def clear_results(self, older_than: float = 0.0):
        cutoff_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            seconds=older_than
        )

        tasks_to_delete = [
            task_id
            for task_id, task in self.tasks.items()
            if task.finished_at is not None and task.finished_at < cutoff_time
        ]
        for task_id in tasks_to_delete:
            await self.delete_task(task_id=task_id)

    async def receive_task_progress(self, request: ProgressCallbackRequest):
        raise NotImplementedError()
