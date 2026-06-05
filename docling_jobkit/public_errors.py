from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
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
INTERNAL_ERROR_MODULE_NAME = "internal_error"
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


@dataclass(frozen=True)
class ClassifiedTaskFailure:
    failure: PublicFailureInfo
    exception_text: str

    @property
    def error_message(self) -> str:
        return self.failure.message


def _public_message_for_code(code: str) -> str:
    if code == "document_limits_exceeded":
        return "Document exceeds service limits."
    if code in {"task_timeout"}:
        return "Task exceeded the allowed execution time."
    if code in {"target_write_error"}:
        return "Result could not be written to the requested target."
    if code in {"http_transport_error"}:
        return "Source document could not be reached."
    if code in {"s3_dependency_error"}:
        return "Source object storage could not be reached."
    if code.startswith("http_"):
        return "Source document could not be retrieved."
    if code in {"capacity_exhausted", "ray_oom"}:
        return "Service capacity was exhausted while processing the task."
    return INTERNAL_TASK_ERROR_MESSAGE


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


def classify_public_task_failure(
    exc: BaseException,
    *,
    task_id: str,
    debug_enabled: bool,
    phase: FailurePhase = FailurePhase.ORCHESTRATION,
    details: dict[str, str] | None = None,
) -> ClassifiedTaskFailure:
    del debug_enabled
    root_exc = _unwrap_failure_exception(exc)
    exception_text = _exception_text(root_exc)
    merged_details = dict(details or {})

    category = FailureCategory.INTERNAL
    code = "internal_error"
    retryable = False
    message = INTERNAL_TASK_ERROR_MESSAGE

    if isinstance(root_exc, TargetWriteError):
        category = FailureCategory.TARGET_UNAVAILABLE
        code = "target_write_error"
        retryable = False
        message = _public_message_for_code(code)
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
        message = _public_message_for_code(code)
    elif MaterializationLimitExceededErrorType is not None and isinstance(
        root_exc, MaterializationLimitExceededErrorType
    ):
        category = FailureCategory.POLICY
        phase = FailurePhase.ADMISSION
        code = "document_limits_exceeded"
        retryable = False
        message = _public_message_for_code(code)
    elif isinstance(root_exc, httpx.HTTPStatusError):
        status_code = root_exc.response.status_code
        merged_details = {
            **merged_details,
            **_safe_details(source_kind="http"),
        }
        phase = FailurePhase.SOURCE_ENUMERATION
        if status_code in {401, 403, 404, 413, 415, 422}:
            category = FailureCategory.POLICY
            code = f"http_{status_code}"
            retryable = False
        elif status_code in {429, 502, 503, 504}:
            category = FailureCategory.SOURCE_UNAVAILABLE
            code = f"http_{status_code}"
            retryable = True
        else:
            category = FailureCategory.SOURCE_UNAVAILABLE
            code = f"http_{status_code}"
            retryable = status_code >= 500
        message = exception_text
    elif RequestsHTTPErrorType is not None and isinstance(
        root_exc, RequestsHTTPErrorType
    ):
        response = getattr(root_exc, "response", None)
        status_code_value = response.status_code if response is not None else None
        request_status_code: int | None = (
            int(status_code_value) if isinstance(status_code_value, int) else None
        )
        merged_details = {
            **merged_details,
            **_safe_details(source_kind="http"),
        }
        phase = FailurePhase.SOURCE_ENUMERATION
        if request_status_code in {401, 403, 404, 413, 415, 422}:
            category = FailureCategory.POLICY
            code = f"http_{request_status_code}"
            retryable = False
        elif request_status_code in {429, 502, 503, 504}:
            category = FailureCategory.SOURCE_UNAVAILABLE
            code = f"http_{request_status_code}"
            retryable = True
        else:
            category = FailureCategory.SOURCE_UNAVAILABLE
            code = (
                f"http_{request_status_code}"
                if request_status_code is not None
                else "http_transport_error"
            )
            retryable = request_status_code is None or request_status_code >= 500
        message = exception_text
    elif isinstance(root_exc, httpx.HTTPError) or (
        RequestsRequestExceptionType is not None
        and isinstance(root_exc, RequestsRequestExceptionType)
    ):
        category = FailureCategory.SOURCE_UNAVAILABLE
        phase = FailurePhase.SOURCE_ENUMERATION
        code = "http_transport_error"
        retryable = True
        message = _public_message_for_code(code)
    elif BotoCoreErrorType is not None and isinstance(root_exc, BotoCoreErrorType):
        category = FailureCategory.SOURCE_UNAVAILABLE
        phase = FailurePhase.SOURCE_ENUMERATION
        code = "s3_dependency_error"
        retryable = True
        merged_details = {
            **merged_details,
            **_safe_details(source_kind="s3"),
        }
        message = _public_message_for_code(code)
    elif isinstance(root_exc, MemoryError) or "oom" in exception_text.lower():
        category = FailureCategory.CAPACITY
        code = "capacity_exhausted"
        retryable = True
        message = _public_message_for_code(code)
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
            message = _public_message_for_code(code)
        else:
            category = FailureCategory.INTERNAL
            code = "ray_runtime_error"
            retryable = True
            message = _public_message_for_code(code)

    failure = PublicFailureInfo(
        code=code,
        category=category,
        message=message,
        retryable=retryable,
        phase=phase,
        correlation_id=task_id,
        details=_safe_details(**merged_details),
    )
    return ClassifiedTaskFailure(failure=failure, exception_text=exception_text)


def build_public_task_error(exc: BaseException, debug_enabled: bool) -> str:
    del debug_enabled
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
