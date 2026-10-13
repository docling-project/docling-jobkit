import datetime
import enum
import logging
import warnings
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional

from docling.datamodel.service.callbacks import CallbackSpec, ProgressCallbackRequest
from docling.datamodel.service.chunking import BaseChunkerOptions
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.tasks import TaskType

from docling_jobkit.datamodel.chunking import ChunkingExportOptions
from docling_jobkit.datamodel.result import DoclingTaskResult, TaskOutcome
from docling_jobkit.datamodel.task import Task, TaskSource, TaskTarget
from docling_jobkit.datamodel.task_meta import TaskStatus

if TYPE_CHECKING:
    from docling_jobkit.orchestrators.base_notifier import BaseNotifier

_log = logging.getLogger(__name__)


class OrchestratorError(Exception):
    pass


class RedisBackpressureError(OrchestratorError):
    pass


class TaskNotFoundError(OrchestratorError):
    pass


class ProgressInvalid(OrchestratorError):
    pass


class CancellationConflictReason(str, enum.Enum):
    """Machine-readable reasons why a task can no longer be cancelled."""

    # The task already claimed its STARTED transition, so conversion may run.
    ALREADY_STARTED = "already_started"
    # The task already reached SUCCESS or FAILURE.
    ALREADY_FINISHED = "already_finished"


class TaskCancellationConflictError(OrchestratorError):
    """Raised when a queued-only cancellation lost the race against execution.

    ``reason`` is a stable machine-readable value (see
    :class:`CancellationConflictReason`) and ``task_status`` is the status that
    made the cancellation impossible, so callers can map the conflict without
    parsing a message.
    """

    def __init__(
        self,
        reason: CancellationConflictReason,
        task_status: Optional[TaskStatus] = None,
    ):
        self.reason = reason
        self.task_status = task_status
        super().__init__(
            f"Task cannot be cancelled ({reason.value}"
            f"{f', current status {task_status.value}' if task_status else ''})"
        )


class TargetNotAllowedError(OrchestratorError):
    """Raised at enqueue when a target kind is not permitted by the orchestrator."""


class BaseOrchestrator(ABC):
    def __init__(self):
        self.tasks: dict[str, Task] = {}
        self.notifier: Optional[BaseNotifier] = None
        # Optional allowlist of target ``kind`` values this orchestrator accepts.
        # ``None`` means "allow all". Concrete orchestrators set this from config.
        self.allowed_target_kinds: Optional[set[str]] = None

    def bind_notifier(self, notifier: "BaseNotifier"):
        self.notifier = notifier

    def _validate_targets(self, targets: list[TaskTarget]) -> None:
        """Reject any target whose ``kind`` is not permitted by this orchestrator.

        Called at the top of each concrete ``enqueue`` so a disallowed target
        fails fast with a clear, non-retryable error before the task is queued.
        """
        if self.allowed_target_kinds is None:
            return
        for target in targets:
            kind = getattr(target, "kind", None)
            if kind not in self.allowed_target_kinds:
                raise TargetNotAllowedError(
                    f"Target kind {kind!r} is not permitted by this orchestrator. "
                    f"Allowed kinds: {sorted(self.allowed_target_kinds)}"
                )

    def _validate_target(self, target: TaskTarget) -> None:
        """Backwards-compatible single-target variant of ``_validate_targets``."""
        self._validate_targets([target])

    @staticmethod
    def _resolve_enqueue_targets(
        target: TaskTarget | None,
        targets: list[TaskTarget] | None,
    ) -> list[TaskTarget]:
        """Normalise the ``target`` / ``targets`` enqueue arguments into a list.

        Callers may pass either the legacy singular ``target`` keyword or the
        preferred plural ``targets`` list.  Passing both is an error.  Passing
        neither is also an error (Task validation would catch it anyway, but
        failing early produces a clearer message).
        """
        if target is not None and targets is not None:
            raise ValueError(
                "'target' and 'targets' are mutually exclusive in enqueue()"
            )
        if target is not None:
            warnings.warn(
                "Passing 'target' to enqueue() is deprecated; use 'targets=[...]' instead.",
                DeprecationWarning,
                stacklevel=3,
            )
            return [target]
        if targets is not None:
            return targets
        raise ValueError(
            "At least one target is required. Provide either 'target' or 'targets'."
        )

    @abstractmethod
    async def enqueue(
        self,
        sources: list[TaskSource],
        target: TaskTarget | None = None,
        task_type: TaskType = TaskType.CONVERT,
        options: ConvertDocumentsOptions | None = None,
        convert_options: ConvertDocumentsOptions | None = None,
        chunking_options: BaseChunkerOptions | None = None,
        chunking_export_options: ChunkingExportOptions | None = None,
        callbacks: list[CallbackSpec] | None = None,
        metadata: dict[str, Any] | None = None,
        targets: list[TaskTarget] | None = None,
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

    @abstractmethod
    async def check_connection(self):
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

    @abstractmethod
    async def task_result(
        self,
        task_id: str,
    ) -> Optional[DoclingTaskResult]:
        pass

    async def task_outcome(self, task_id: str) -> Optional[TaskOutcome]:
        return await self.task_result(task_id)

    @staticmethod
    def cancellation_conflict_reason(
        status: TaskStatus,
    ) -> Optional[CancellationConflictReason]:
        """Map a non-cancellable task status to its machine-readable reason.

        Returns None for ``PENDING`` and ``CANCELLED``, which are the only
        states a queued-only cancellation may still resolve.
        """
        if status == TaskStatus.STARTED:
            return CancellationConflictReason.ALREADY_STARTED
        if status in (TaskStatus.SUCCESS, TaskStatus.FAILURE):
            return CancellationConflictReason.ALREADY_FINISHED
        return None

    async def cancel_queued_task(self, task_id: str) -> Task:
        """Cancel a task that has not started converting yet.

        Returns the cancelled task. Raises ``TaskNotFoundError`` for an unknown
        task and ``TaskCancellationConflictError`` once the task already claimed
        STARTED or reached a terminal outcome, in which case the task is left
        untouched. Cancelling an already cancelled task is idempotent and
        returns that same task.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support queued-only cancellation"
        )

    async def delete_task(self, task_id: str):
        _log.info(f"Deleting {task_id=}")
        if self.notifier:
            await self.notifier.remove_task(task_id=task_id)
        if task_id in self.tasks:
            del self.tasks[task_id]

    async def on_result_fetched(self, task_id: str) -> None:
        """Called after a result has been successfully returned to the caller.
        Subclasses override to implement single-use cleanup strategies."""

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
