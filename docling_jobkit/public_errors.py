from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import httpx

from docling.datamodel.base_models import DoclingComponentType, ErrorItem
from docling.datamodel.service.responses import (
    FailureCategory,
    FailurePhase,
    PublicFailureInfo,
)

BotoCoreErrorType: type[BaseException] | None
try:
    from botocore.exceptions import (
        BotoCoreError as _BotoCoreError,  # type: ignore[import-untyped]
    )
except ImportError:  # pragma: no cover - optional dependency
    BotoCoreErrorType = None
else:
    BotoCoreErrorType = _BotoCoreError

RequestsHTTPErrorType: type[BaseException] | None
RequestsRequestExceptionType: type[BaseException] | None
try:
    from requests.exceptions import (
        HTTPError as _RequestsHTTPError,
        RequestException as _RequestsRequestException,
    )
except ImportError:  # pragma: no cover - optional dependency
    RequestsHTTPErrorType = None
    RequestsRequestExceptionType = None
else:
    RequestsHTTPErrorType = _RequestsHTTPError
    RequestsRequestExceptionType = _RequestsRequestException

ray_exceptions: Any | None
try:
    import ray.exceptions as ray_exceptions
except ImportError:  # pragma: no cover - optional dependency
    ray_exceptions = None

MaterializationLimitExceededErrorType: type[BaseException] | None
try:
    from docling_jobkit.convert.materialization import (
        MaterializationLimitExceededError as _MaterializationLimitExceededError,
    )
except ImportError:  # pragma: no cover - defensive only
    MaterializationLimitExceededErrorType = None
else:
    MaterializationLimitExceededErrorType = _MaterializationLimitExceededError


class TargetWriteError(RuntimeError):
    """Raised when writing results to a user-provided target (PutTarget) fails."""


INTERNAL_TASK_ERROR_MESSAGE = "Internal processing error."
_ALLOWED_DETAIL_KEYS = {
    "source_kind",
    "target_kind",
    "timeout_class",
    "task_size",
}


def _exception_text(exc: BaseException) -> str:
    detail = str(exc)
    return detail or exc.__class__.__name__


def _safe_details(**details: str | int | None) -> dict[str, str]:
    return {
        key: str(value)
        for key, value in details.items()
        if key in _ALLOWED_DETAIL_KEYS and value is not None
    }


def _unwrap_failure_exception(exc: BaseException) -> BaseException:
    current = exc
    seen: set[int] = set()

    while True:
        obj_id = id(current)
        if obj_id in seen:
            return current
        seen.add(obj_id)

        if ray_exceptions is not None and isinstance(
            current, getattr(ray_exceptions, "RayTaskError", ())
        ):
            cause = getattr(current, "cause", None)
            if isinstance(cause, BaseException):
                current = cause
                continue

        cause = getattr(current, "__cause__", None)
        if isinstance(cause, BaseException):
            current = cause
            continue

        return current


def _classify_http_status(
    status_code: int | None,
    exception_text: str,
) -> tuple[FailureCategory, str, bool, str]:
    """Map an HTTP status code to (category, code, retryable, message)."""
    code = "http_transport_error" if status_code is None else f"http_{status_code}"
    if status_code in {401, 403, 404, 413, 415, 422}:
        return FailureCategory.POLICY, code, False, exception_text
    if status_code in {429, 502, 503, 504}:
        return FailureCategory.SOURCE_UNAVAILABLE, code, True, exception_text
    retryable = status_code is None or status_code >= 500
    return FailureCategory.SOURCE_UNAVAILABLE, code, retryable, exception_text


def classify_public_task_failure(
    exc: BaseException,
    *,
    task_id: str,
    phase: FailurePhase = FailurePhase.ORCHESTRATION,
    details: dict[str, str] | None = None,
) -> PublicFailureInfo:
    root_exc = _unwrap_failure_exception(exc)
    exception_text = _exception_text(root_exc)
    merged_details: dict[str, str] = _safe_details(**(details or {}))

    category = FailureCategory.INTERNAL
    code = "internal_error"
    retryable = False
    message = INTERNAL_TASK_ERROR_MESSAGE

    if isinstance(root_exc, TargetWriteError):
        category = FailureCategory.TARGET_UNAVAILABLE
        code = "target_write_error"
        retryable = False
        message = "Result could not be written to the requested target."
    elif isinstance(
        root_exc, (asyncio.TimeoutError, TimeoutError, httpx.TimeoutException)
    ):
        category = FailureCategory.TIMEOUT
        code = "task_timeout"
        retryable = True
        merged_details = {
            **merged_details,
            **_safe_details(timeout_class=root_exc.__class__.__name__),
        }
        message = "Task exceeded the allowed execution time."
    elif MaterializationLimitExceededErrorType is not None and isinstance(
        root_exc, MaterializationLimitExceededErrorType
    ):
        category = FailureCategory.POLICY
        phase = FailurePhase.ADMISSION
        code = "document_limits_exceeded"
        retryable = False
        message = "Document exceeds service limits."
    elif isinstance(root_exc, httpx.HTTPStatusError):
        merged_details = {**merged_details, **_safe_details(source_kind="http")}
        phase = FailurePhase.SOURCE_ENUMERATION
        category, code, retryable, message = _classify_http_status(
            root_exc.response.status_code, exception_text
        )
    elif RequestsHTTPErrorType is not None and isinstance(
        root_exc, RequestsHTTPErrorType
    ):
        response = getattr(root_exc, "response", None)
        status_code_value = response.status_code if response is not None else None
        status_code: int | None = (
            int(status_code_value) if isinstance(status_code_value, int) else None
        )
        merged_details = {**merged_details, **_safe_details(source_kind="http")}
        phase = FailurePhase.SOURCE_ENUMERATION
        category, code, retryable, message = _classify_http_status(
            status_code, exception_text
        )
    elif isinstance(root_exc, httpx.HTTPError) or (
        RequestsRequestExceptionType is not None
        and isinstance(root_exc, RequestsRequestExceptionType)
    ):
        category = FailureCategory.SOURCE_UNAVAILABLE
        phase = FailurePhase.SOURCE_ENUMERATION
        code = "http_transport_error"
        retryable = True
        message = "Source document could not be reached."
    elif BotoCoreErrorType is not None and isinstance(root_exc, BotoCoreErrorType):
        category = FailureCategory.SOURCE_UNAVAILABLE
        phase = FailurePhase.SOURCE_ENUMERATION
        code = "s3_dependency_error"
        retryable = True
        merged_details = {**merged_details, **_safe_details(source_kind="s3")}
        message = "Source object storage could not be reached."
    elif isinstance(root_exc, MemoryError) or "oom" in exception_text.lower():
        category = FailureCategory.CAPACITY
        code = "capacity_exhausted"
        retryable = True
        message = "Service capacity was exhausted while processing the task."
    elif ray_exceptions is not None and isinstance(
        exc,
        tuple(
            cls
            for cls in (
                getattr(ray_exceptions, "RayTaskError", None),
                getattr(ray_exceptions, "ActorDiedError", None),
                getattr(ray_exceptions, "OutOfMemoryError", None),
            )
            if cls is not None
        ),
    ):
        lowered = exception_text.lower()
        if "outofmemory" in lowered or "oom" in lowered:
            category = FailureCategory.CAPACITY
            code = "ray_oom"
            retryable = True
            message = "Service capacity was exhausted while processing the task."
        else:
            category = FailureCategory.INTERNAL
            code = "ray_runtime_error"
            retryable = True

    return PublicFailureInfo(
        code=code,
        category=category,
        message=message,
        retryable=retryable,
        phase=phase,
        correlation_id=task_id,
        details=merged_details,
    )


def build_public_task_error(exc: BaseException) -> str:
    root_exc = _unwrap_failure_exception(exc)
    if isinstance(root_exc, httpx.HTTPStatusError) or (
        RequestsHTTPErrorType is not None
        and isinstance(root_exc, RequestsHTTPErrorType)
    ):
        return _exception_text(root_exc)
    return INTERNAL_TASK_ERROR_MESSAGE


def is_expected_public_failure(failure: PublicFailureInfo) -> bool:
    return failure.phase == FailurePhase.SOURCE_ENUMERATION and failure.category in {
        FailureCategory.POLICY,
        FailureCategory.SOURCE_UNAVAILABLE,
    }


def build_public_error_item(exc: BaseException) -> ErrorItem:
    return ErrorItem(
        component_type=DoclingComponentType.PIPELINE,
        module_name=exc.__class__.__name__,
        error_message=_exception_text(exc),
    )


def render_public_error_list(
    errors: Sequence[ErrorItem],
    debug_enabled: bool,
) -> str | None:
    if not errors:
        return None

    rendered_errors: list[str] = []
    for error in errors:
        if debug_enabled and error.module_name:
            rendered_errors.append(f"{error.module_name}: {error.error_message}")
        else:
            rendered_errors.append(error.error_message)

    return "; ".join(rendered_errors) if rendered_errors else None
