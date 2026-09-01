from __future__ import annotations

from typing import TYPE_CHECKING

import ray.exceptions as ray_exceptions

from docling.datamodel.service.responses import (
    FailureCategory,
    FailurePhase,
    PublicFailureInfo,
)

from docling_jobkit.public_errors import (
    PipelineInitializationError,
    classify_public_task_failure,
)

if TYPE_CHECKING:
    from docling_jobkit.datamodel.task import Task


def task_target_kind(task: "Task") -> str:
    """Return the ``kind`` of the first target, or its type name as a fallback.

    ``task.target`` is always ``None`` at runtime because the model validator
    normalises it into ``task.targets`` immediately on construction.  Reading
    ``task.targets[0]`` is therefore the correct way to reach the first target.
    """
    targets = task.targets
    first = targets[0] if targets else None
    return getattr(first, "kind", type(first).__name__)


def _unwrap_ray_failure_exception(exc: BaseException) -> BaseException:
    current = exc
    seen: set[int] = set()

    while True:
        obj_id = id(current)
        if obj_id in seen:
            return current
        seen.add(obj_id)

        if isinstance(current, ray_exceptions.RayTaskError):
            current = current.cause
            continue

        return current


def is_request_wide_capability_failure(exc: BaseException) -> bool:
    """Return whether conversion failed during converter setup, before any document.

    Such failures come from the eager setup span of ``convert_documents()``
    (option resolution + pipeline/model init), which depends only on the request's
    options and ``artifacts_path`` -- not on any document. They are therefore
    request-wide (e.g. a configured model absent from ``artifacts_path``) and must
    abort the task, not be recorded as one document's failure. Classified by
    exception type so it survives changes to error text.
    """
    root_exc = _unwrap_ray_failure_exception(exc)
    return isinstance(root_exc, PipelineInitializationError)


def classify_ray_public_task_failure(
    exc: BaseException,
    *,
    task_id: str,
    phase: FailurePhase = FailurePhase.ORCHESTRATION,
    details: dict[str, str] | None = None,
) -> PublicFailureInfo:
    """Classify Ray task failures after unwrapping Ray's exception envelope."""
    root_exc = _unwrap_ray_failure_exception(exc)
    failure = classify_public_task_failure(
        root_exc,
        task_id=task_id,
        phase=phase,
        details=details,
    )
    if failure.category != FailureCategory.INTERNAL or not isinstance(
        exc,
        (
            ray_exceptions.RayTaskError,
            ray_exceptions.ActorDiedError,
            ray_exceptions.OutOfMemoryError,
        ),
    ):
        return failure

    lowered = str(root_exc).lower()
    if "outofmemory" in lowered or "oom" in lowered:
        return failure.model_copy(
            update={
                "category": FailureCategory.CAPACITY,
                "retryable": True,
                "message": "Service capacity was exhausted while processing the task.",
            }
        )

    return failure.model_copy(update={"retryable": True})
