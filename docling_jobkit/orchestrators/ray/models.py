"""Data models for Fair Ray orchestrator."""

from typing import Optional

from pydantic import BaseModel, Field

from docling_jobkit.datamodel.task_meta import TaskProcessingMeta, TaskStatus


class UserLimits(BaseModel):
    """Per-user resource limits and current usage.

    Attributes:
        max_concurrent_tasks: Maximum tasks being scheduled/processed simultaneously
        max_queued_tasks: Maximum tasks in queue (None = unlimited)
        max_documents: Maximum documents being processed (None = unlimited, off by default)
        active_tasks: Currently being processed
        queued_tasks: Waiting in queue
        active_documents: Currently being processed
    """

    max_concurrent_tasks: int = Field(
        default=5, description="Max tasks being scheduled/processed simultaneously"
    )
    max_queued_tasks: Optional[int] = Field(
        default=None, description="Max tasks in queue (None = unlimited)"
    )
    max_documents: Optional[int] = Field(
        default=None, description="Max documents being processed (None = unlimited)"
    )
    active_tasks: int = Field(default=0, description="Currently being processed")
    queued_tasks: int = Field(default=0, description="Waiting in queue")
    active_documents: int = Field(default=0, description="Currently being processed")


class UserStats(BaseModel):
    """Per-user statistics for tracking usage and performance.

    Attributes:
        total_tasks: Total number of tasks submitted
        total_documents: Total number of documents processed
        successful_documents: Number of successfully processed documents
        failed_documents: Number of failed documents
    """

    total_tasks: int = Field(default=0, description="Total tasks submitted")
    total_documents: int = Field(default=0, description="Total documents processed")
    successful_documents: int = Field(
        default=0, description="Successfully processed documents"
    )
    failed_documents: int = Field(default=0, description="Failed documents")


class TaskUpdate(BaseModel):
    """Internal task status update message for pub/sub communication.

    Used to communicate task status changes between Ray actors and the orchestrator
    via Redis pub/sub.

    Attributes:
        task_id: Unique task identifier
        task_status: Current status of the task
        result_key: Redis key where result is stored (if completed)
        error_message: Error message if task failed
        progress: Task processing metadata (progress, counts, etc.)
    """

    task_id: str = Field(description="Unique task identifier")
    task_status: TaskStatus = Field(description="Current status of the task")
    result_key: Optional[str] = Field(
        default=None, description="Redis key where result is stored"
    )
    error_message: Optional[str] = Field(
        default=None, description="Error message if task failed"
    )
    progress: Optional[TaskProcessingMeta] = Field(
        default=None, description="Task processing metadata"
    )
