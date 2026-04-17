"""Data models for Ray orchestrator."""

import datetime
from dataclasses import dataclass
from typing import Optional

from pydantic import BaseModel, Field

from docling.datamodel.service.tasks import TaskProcessingMeta, TaskType

from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus


class TenantLimits(BaseModel):
    """Per-tenant resource limits and current usage.

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


class TenantStats(BaseModel):
    """Per-tenant statistics for tracking usage and performance.

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


class RedisTaskMetadata(BaseModel):
    """Durable task metadata stored in Redis for Ray task recovery.

    This model intentionally captures orchestration-only state that is not part
    of the public service API models. It exists so a restarted API/orchestrator
    process can reconstruct task state from Redis without depending on
    in-memory bookkeeping.
    """

    task_id: str = Field(description="Unique task identifier")
    tenant_id: str = Field(description="Tenant that owns the task")
    status: TaskStatus = Field(description="Current task status")
    task_type: TaskType = Field(description="Task type")
    task_size: int = Field(description="Number of documents associated with the task")
    created_at: datetime.datetime = Field(
        description="UTC timestamp when the task metadata was created"
    )
    last_update_at: datetime.datetime = Field(
        description="UTC timestamp for the last task metadata update"
    )
    error_message: Optional[str] = Field(
        default=None, description="Failure message if the task failed"
    )
    started_at: Optional[datetime.datetime] = Field(
        default=None, description="UTC timestamp when processing started"
    )
    finished_at: Optional[datetime.datetime] = Field(
        default=None, description="UTC timestamp when processing finished"
    )
    retry_count: int = Field(
        default=0, description="Recorded retry counter for recovery bookkeeping"
    )

    @classmethod
    def _parse_optional_datetime(
        cls, raw_value: Optional[str]
    ) -> Optional[datetime.datetime]:
        if raw_value is None or raw_value == "null":
            return None
        return datetime.datetime.fromisoformat(raw_value)

    @classmethod
    def from_redis_mapping(
        cls, redis_mapping: dict[str, str]
    ) -> Optional["RedisTaskMetadata"]:
        created_at_raw = redis_mapping.get("created_at")
        last_update_at_raw = redis_mapping.get("last_update_at")
        if created_at_raw is None or last_update_at_raw is None:
            return None

        created_at = cls._parse_optional_datetime(created_at_raw)
        last_update_at = cls._parse_optional_datetime(last_update_at_raw)
        if created_at is None or last_update_at is None:
            return None

        task_id = redis_mapping.get("task_id")
        tenant_id = redis_mapping.get("tenant_id")
        status = redis_mapping.get("status")
        if task_id is None or tenant_id is None or status is None:
            return None

        return cls(
            task_id=task_id,
            tenant_id=tenant_id,
            status=TaskStatus(status),
            task_type=TaskType(redis_mapping.get("task_type", TaskType.CONVERT.value)),
            task_size=int(redis_mapping.get("task_size", "0")),
            created_at=created_at,
            last_update_at=last_update_at,
            error_message=redis_mapping.get("error_message"),
            started_at=cls._parse_optional_datetime(redis_mapping.get("started_at")),
            finished_at=cls._parse_optional_datetime(redis_mapping.get("finished_at")),
            retry_count=int(redis_mapping.get("retry_count", "0")),
        )

    def to_task(self) -> Task:
        return Task(
            task_id=self.task_id,
            task_type=self.task_type,
            task_status=self.status,
            sources=[],
            metadata={"tenant_id": self.tenant_id},
            error_message=self.error_message,
            created_at=self.created_at,
            started_at=self.started_at,
            finished_at=self.finished_at,
            last_update_at=self.last_update_at,
        )


@dataclass(frozen=True)
class TaskTerminalizationResult:
    """Outcome of an idempotent terminalization attempt."""

    final_status: TaskStatus
    status_changed: bool
    capacity_released: bool
    result_key: Optional[str] = None
