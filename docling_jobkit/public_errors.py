from __future__ import annotations

import asyncio
from collections.abc import Sequence

import httpx
from botocore.exceptions import BotoCoreError
from requests.exceptions import HTTPError as RequestsHTTPError, RequestException

from docling.datamodel.base_models import DoclingComponentType, ErrorItem
from docling.datamodel.service.responses import (
    FailureCategory,
    FailurePhase,
    PublicFailureInfo,
)

from docling_jobkit.convert.materialization import (
    MaterializationLimitExceededError,
    SourceLimitExceededError,
)


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

        cause = current.__cause__
        if isinstance(cause, BaseException):
            current = cause
            continue

        return current


def _classify_http_status(
    status_code: int | None,
    exception_text: str,
) -> tuple[FailureCategory, bool, str]:
    """Map an HTTP status code to (category, retryable, message)."""
    if status_code in {401, 403, 404, 413, 415, 422}:
        return FailureCategory.POLICY, False, exception_text
    if status_code in {429, 502, 503, 504}:
        return FailureCategory.SOURCE_UNAVAILABLE, True, exception_text
    retryable = status_code is None or status_code >= 500
    return FailureCategory.SOURCE_UNAVAILABLE, retryable, exception_text


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
    retryable = False
    message = INTERNAL_TASK_ERROR_MESSAGE

    if isinstance(root_exc, TargetWriteError):
        # The user-provided PutTarget accepted the request but could not persist output.
        category = FailureCategory.TARGET_UNAVAILABLE
        retryable = False
        message = "Result could not be written to the requested target."
    elif isinstance(
        root_exc, (asyncio.TimeoutError, TimeoutError, httpx.TimeoutException)
    ):
        # The task exceeded an execution or transport timeout and can be retried.
        category = FailureCategory.TIMEOUT
        retryable = True
        merged_details = {
            **merged_details,
            **_safe_details(timeout_class=root_exc.__class__.__name__),
        }
        message = "Task exceeded the allowed execution time."
    elif isinstance(root_exc, MaterializationLimitExceededError):
        # Local preflight rejected a document that exceeds configured service limits.
        category = FailureCategory.POLICY
        phase = FailurePhase.ADMISSION
        retryable = False
        message = "Document exceeds service limits."
    elif isinstance(root_exc, SourceLimitExceededError):
        category = FailureCategory.POLICY
        phase = FailurePhase.SOURCE_ENUMERATION
        retryable = False
        message = exception_text
    elif isinstance(root_exc, httpx.HTTPStatusError):
        # httpx fetched the source but the upstream HTTP status is not usable.
        merged_details = {**merged_details, **_safe_details(source_kind="http")}
        phase = FailurePhase.SOURCE_ENUMERATION
        category, retryable, message = _classify_http_status(
            root_exc.response.status_code, exception_text
        )
    elif isinstance(root_exc, RequestsHTTPError):
        # requests fetched the source but the upstream HTTP status is not usable.
        response = root_exc.response
        status_code_value = response.status_code if response is not None else None
        status_code: int | None = (
            int(status_code_value) if isinstance(status_code_value, int) else None
        )
        merged_details = {**merged_details, **_safe_details(source_kind="http")}
        phase = FailurePhase.SOURCE_ENUMERATION
        category, retryable, message = _classify_http_status(
            status_code, exception_text
        )
    elif isinstance(root_exc, httpx.HTTPError) or isinstance(
        root_exc, RequestException
    ):
        # HTTP transport failed before a usable response body/status was available.
        category = FailureCategory.SOURCE_UNAVAILABLE
        phase = FailurePhase.SOURCE_ENUMERATION
        retryable = True
        message = "Source document could not be reached."
    elif isinstance(root_exc, BotoCoreError):
        # S3 source enumeration or object retrieval failed through botocore.
        category = FailureCategory.SOURCE_UNAVAILABLE
        phase = FailurePhase.SOURCE_ENUMERATION
        retryable = True
        merged_details = {**merged_details, **_safe_details(source_kind="s3")}
        message = "Source object storage could not be reached."
    elif isinstance(root_exc, MemoryError) or "oom" in exception_text.lower():
        # The local process or a nested exception reports memory exhaustion.
        category = FailureCategory.CAPACITY
        retryable = True
        message = "Service capacity was exhausted while processing the task."
    return PublicFailureInfo(
        category=category,
        message=message,
        retryable=retryable,
        phase=phase,
        details=merged_details,
    )


def build_public_task_error(exc: BaseException) -> str:
    root_exc = _unwrap_failure_exception(exc)
    if (
        isinstance(root_exc, httpx.HTTPStatusError)
        or isinstance(root_exc, RequestsHTTPError)
        or isinstance(root_exc, SourceLimitExceededError)
    ):
        return _exception_text(root_exc)
    return INTERNAL_TASK_ERROR_MESSAGE


def is_client_actionable_failure(failure: PublicFailureInfo) -> bool:
    """Return true when retry/log policy should treat a failure as client-actionable.

    These failures are not internal service defects: the source could not be
    fetched, or request-adjacent source policy failed after admission-time
    validation.
    """
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


def build_public_error_item_from_failure(failure: PublicFailureInfo) -> ErrorItem:
    """Build a document-level ErrorItem from a structured converter failure.

    Used when a per-source converter request returns a ``ConverterFailureResult``
    (an already-sanitized, client-actionable failure such as a missing or
    too-large S3 object) and we want to record it as a document-level FAILURE
    rather than aborting the whole task.
    """
    return ErrorItem(
        component_type=DoclingComponentType.PIPELINE,
        module_name=failure.category.value,
        error_message=failure.message,
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
