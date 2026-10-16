import enum

from docling.datamodel.service.tasks import TaskProcessingMeta, TaskType


class TaskStatus(str, enum.Enum):
    SUCCESS = "success"
    PENDING = "pending"
    STARTED = "started"
    FAILURE = "failure"
    CANCELLED = "cancelled"


__all__ = ["TaskProcessingMeta", "TaskStatus", "TaskType"]
