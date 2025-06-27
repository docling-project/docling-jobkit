from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from docling_jobkit.orchestrators.base_orchestrator import BaseOrchestrator


class BaseNotifier(ABC):
    def __init__(self, orchestrator: "BaseOrchestrator"):
        self.orchestrator = orchestrator

    @abstractmethod
    async def add_task(self, task_id: str):
        pass

    @abstractmethod
    async def remove_task(self, task_id: str):
        pass

    @abstractmethod
    async def notify_task_subscribers(self, task_id: str):
        pass

    @abstractmethod
    async def notify_queue_positions(self):
        pass
