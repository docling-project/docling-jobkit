from __future__ import annotations

import ray.exceptions as ray_exceptions

from docling.datamodel.service.responses import (
    FailureCategory,
    FailurePhase,
    PublicFailureInfo,
)

from docling_jobkit.public_errors import classify_public_task_failure


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
