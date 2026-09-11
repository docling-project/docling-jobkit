import logging
import time
from io import BytesIO
from typing import Any, BinaryIO, Callable, Iterator, Optional, Union

import requests

from docling_jobkit.connectors.errors import (
    SourceConnectorPolicyError,
    SourceConnectorUnavailableError,
)
from docling_jobkit.convert.materialization import (
    SourceLimitExceededError,
    normalize_max_file_size,
)
from docling_jobkit.public_errors import TargetWriteError

_log = logging.getLogger(__name__)

_SOURCE_KIND = "databricks_volumes"
_MAX_RETRIES = 3
_BACKOFF_BASE_S = 0.5
_RETRYABLE_4XX_STATUS = {429}


def _retry_call(
    fn: Callable[[], Any],
    operation: str,
    *,
    passthrough: Callable[[requests.HTTPError], bool],
    terminal_error: Callable[[requests.HTTPError, int], BaseException],
    exhausted_http_error: Callable[[requests.HTTPError], BaseException],
    exhausted_network_error: Callable[[BaseException], BaseException],
) -> Any:
    """Shared exponential-backoff loop for both the source (list/download)
    and target (mkdir/upload) call sites.
    """
    for attempt in range(_MAX_RETRIES + 1):
        try:
            result = fn()
            if isinstance(result, requests.Response):
                result.raise_for_status()
            return result
        except requests.HTTPError as exc:
            if passthrough(exc):
                raise
            status = exc.response.status_code if exc.response is not None else None
            if (
                status is not None
                and status < 500
                and status not in _RETRYABLE_4XX_STATUS
            ):
                raise terminal_error(exc, status) from exc
            if attempt == _MAX_RETRIES:
                raise exhausted_http_error(exc) from exc
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt == _MAX_RETRIES:
                raise exhausted_network_error(exc) from exc

        wait = _BACKOFF_BASE_S * (2**attempt)
        _log.warning(
            "Databricks Volumes: %s transient error, retry %d/%d in %.1fs",
            operation,
            attempt + 1,
            _MAX_RETRIES,
            wait,
        )
        time.sleep(wait)

    raise AssertionError("unreachable")


def _source_terminal_error(exc: requests.HTTPError, status: int) -> BaseException:
    error_type = (
        SourceConnectorPolicyError
        if status in {401, 403, 404, 413, 415, 422}
        else SourceConnectorUnavailableError
    )
    return error_type(
        str(exc),
        source_kind=_SOURCE_KIND,
        **(
            {"retryable": False}
            if error_type is SourceConnectorUnavailableError
            else {}
        ),
    )


def _with_source_retry(fn: Callable[[], Any], operation: str) -> Any:
    """Helper for exponential retries on transient errors."""
    return _retry_call(
        fn,
        operation,
        passthrough=lambda exc: False,
        terminal_error=_source_terminal_error,
        exhausted_http_error=lambda exc: SourceConnectorUnavailableError(
            str(exc), source_kind=_SOURCE_KIND
        ),
        exhausted_network_error=lambda exc: SourceConnectorUnavailableError(
            "Databricks Volumes could not be reached.", source_kind=_SOURCE_KIND
        ),
    )


def list_directory_page(
    host: str,
    token: str,
    path: str,
    *,
    page_token: Optional[str] = None,
    page_size: int = 1000,
) -> dict:
    """One page of ``GET /api/2.0/fs/directories{path}``.

    Returns the raw JSON body: {"contents": [...], "next_page_token": ...}.
    """
    params: dict[str, Any] = {"page_size": page_size}
    if page_token:
        params["page_token"] = page_token

    def _do() -> requests.Response:
        return requests.get(
            f"https://{host}/api/2.0/fs/directories{path}",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30,
        )

    response = _with_source_retry(_do, "list directory")
    return response.json()


def iter_directory(host: str, token: str, path: str) -> Iterator[dict]:
    """Yield every entry in one directory level of a Volume,
    paging via next_page_token.
    """
    page_token: Optional[str] = None
    while True:
        data = list_directory_page(host, token, path, page_token=page_token)
        yield from data.get("contents", [])
        page_token = data.get("next_page_token")
        if not page_token:
            return


def download_document(
    host: str,
    token: str,
    path: str,
    *,
    max_file_size: Optional[int] = None,
) -> BytesIO:
    """Download a document via ``GET /api/2.0/fs/files{path}`` with Bearer
    token auth. Disallows redirects and streams the response into a
    bounded buffer respecting max_file_size.
    """
    url = f"https://{host}/api/2.0/fs/files{path}"
    limit = normalize_max_file_size(max_file_size)
    response = _with_source_retry(
        lambda: requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
            stream=True,
            allow_redirects=False,
        ),
        "download document",
    )

    if 300 <= response.status_code < 400:
        raise SourceConnectorPolicyError(
            f"Document URL {url!r} returned a redirect ({response.status_code}); "
            "redirects are not followed to avoid leaking the workspace token "
            "to an unverified host.",
            source_kind=_SOURCE_KIND,
        )

    buffer = BytesIO()
    bytes_seen = 0
    for chunk in response.iter_content(chunk_size=1 << 16):
        if not chunk:
            continue
        bytes_seen += len(chunk)
        if limit is not None and bytes_seen > limit:
            raise SourceLimitExceededError(
                f"Source {url!r} exceeds max_file_size={limit} bytes"
            )
        buffer.write(chunk)

    buffer.seek(0)
    return buffer


def is_databricks_target_authentication_error(exc: BaseException) -> bool:
    """Auth predicate for target uploads (401/403 on the raw ``requests``
    exception, *before* :func:`_with_target_retry` has a chance to translate
    it into a :class:`TargetWriteError`)."""
    return (
        isinstance(exc, requests.HTTPError)
        and exc.response is not None
        and exc.response.status_code in {401, 403}
    )


def _with_target_retry(
    fn: Callable[[], requests.Response], operation: str
) -> requests.Response:
    """Retry helper for target (upload/mkdir) calls."""
    return _retry_call(
        fn,
        operation,
        passthrough=is_databricks_target_authentication_error,
        terminal_error=lambda exc, status: TargetWriteError(str(exc)),
        exhausted_http_error=lambda exc: TargetWriteError(str(exc)),
        exhausted_network_error=lambda exc: TargetWriteError(
            "Databricks Volumes could not be reached."
        ),
    )


def ensure_directory(host: str, token: str, path: str) -> None:
    """Idempotent ``mkdir -p`` via ``PUT /api/2.0/fs/directories{path}``.

    Called once from the target processor's _initialize() since the
    Files API upload endpoint does not document auto-creating parent
    directories, unlike object stores which have no real directory concept.
    """

    def _do() -> requests.Response:
        return requests.put(
            f"https://{host}/api/2.0/fs/directories{path}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )

    _with_target_retry(_do, "create directory")


def upload_document(
    host: str,
    token: str,
    path: str,
    data: Union[bytes, BinaryIO],
    *,
    content_type: str,
) -> None:
    """Upload raw bytes to ``PUT /api/2.0/fs/files{path}?overwrite=true``."""

    def _do() -> requests.Response:
        if hasattr(data, "seek"):
            data.seek(0)
        return requests.put(
            f"https://{host}/api/2.0/fs/files{path}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": content_type,
            },
            params={"overwrite": "true"},
            data=data,
            timeout=30,
        )

    _with_target_retry(_do, "upload document")
