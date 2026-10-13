import enum

from docling.datamodel.service.tasks import TaskProcessingMeta, TaskType


class TaskStatus(str, enum.Enum):
    SUCCESS = "success"
    PENDING = "pending"
    STARTED = "started"
    FAILURE = "failure"
    # Terminal state of a task that was cancelled while still queued. Its
    # conversion never ran, so it carries no result and no failure info.
    CANCELLED = "cancelled"


__all__ = ["TaskProcessingMeta", "TaskStatus", "TaskType"]
