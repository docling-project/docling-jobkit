import enum

from pydantic import BaseModel


class TaskStatus(str, enum.Enum):
    SUCCESS = "success"
    PENDING = "pending"
    STARTED = "started"
    FAILURE = "failure"


class TaskProcessingMeta(BaseModel):
    num_docs: int
    num_processed: int = 0
    num_succeeded: int = 0
    num_failed: int = 0
