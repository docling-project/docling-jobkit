import logging
import time
from io import BytesIO
from typing import Any, BinaryIO, Callable, Iterator, Optional, Union

import requests

from docling_jobkit.connectors.errors import (
    SourceConnectorAuthenticationError,
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
_AUTH_STATUS = {401, 403}
_POLICY_STATUS = {404, 413, 415, 422}

# Transport failures worth another attempt. Kept explicit rather than catching
# requests.RequestException wholesale so malformed-request bugs (InvalidURL,
# MissingSchema, ...) still surface immediately instead of being retried.
_TRANSPORT_ERRORS = (
    requests.Timeout,
    requests.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ContentDecodingError,
)

# Client-safe explanations keyed by status. Deliberately free of the request URL:
# these strings are surfaced verbatim to API clients by
# ``build_public_task_error``, and the workspace host and volume path should not
# ride along in a failure message.
_SOURCE_STATUS_HINTS = {
    404: (
        "Databricks Volumes path not found (HTTP 404); verify volume_path. Note "
        "that the Files API also returns 404 when listing an existing but empty "
        "Unity Catalog volume."
    ),
    413: "Databricks Volumes rejected the request as too large (HTTP 413).",
    415: "Databricks Volumes rejected the content type (HTTP 415).",
    422: "Databricks Volumes could not process the request (HTTP 422).",
}


def _safe_source_message(status: Optional[int]) -> str:
    if status is None:
        return "Databricks Volumes request failed."
    hint = _SOURCE_STATUS_HINTS.get(status)
    if hint is not None:
        return hint
    return f"Databricks Volumes request failed (HTTP {status})."


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
        except _TRANSPORT_ERRORS as exc:
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
    del exc  # Never echoed back: str(HTTPError) embeds the full request URL.
    if status in _AUTH_STATUS:
        return SourceConnectorAuthenticationError(
            "Databricks Volumes authentication failed; verify permissions and "
            "supply valid credentials.",
            source_kind=_SOURCE_KIND,
        )
    if status in _POLICY_STATUS:
        return SourceConnectorPolicyError(
            _safe_source_message(status), source_kind=_SOURCE_KIND
        )
    return SourceConnectorUnavailableError(
        _safe_source_message(status), source_kind=_SOURCE_KIND, retryable=False
    )


def _with_source_retry(fn: Callable[[], Any], operation: str) -> Any:
    """Helper for exponential retries on transient errors."""
    return _retry_call(
        fn,
        operation,
        passthrough=lambda exc: False,
        terminal_error=_source_terminal_error,
        exhausted_http_error=lambda exc: SourceConnectorUnavailableError(
            _safe_source_message(
                exc.response.status_code if exc.response is not None else None
            ),
            source_kind=_SOURCE_KIND,
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
    ``page_size`` is capped at 1000 by the Files API.
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
    try:
        return response.json()
    except ValueError as exc:
        # A proxy or captive portal answering 200 with non-JSON, or a truncated
        # body. Translated here so it lands as a typed source failure instead of
        # escaping as a bare ValueError and being reported as an internal error.
        raise SourceConnectorUnavailableError(
            "Databricks Volumes returned a malformed directory listing.",
            source_kind=_SOURCE_KIND,
        ) from exc


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

    The body stream is consumed *inside* the retried callable so that a
    connection dropped mid-download is retried and translated like any other
    transport failure, rather than escaping untyped from a loop that ran after
    the retry helper had already returned. The response is always closed, so
    aborting early on the size limit cannot strand the socket.
    """
    url = f"https://{host}/api/2.0/fs/files{path}"
    limit = normalize_max_file_size(max_file_size)

    def _do() -> BytesIO:
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
            stream=True,
            allow_redirects=False,
        )
        try:
            if 300 <= response.status_code < 400:
                raise SourceConnectorPolicyError(
                    f"Document path {path!r} returned a redirect "
                    f"({response.status_code}); redirects are not followed to "
                    "avoid leaking the workspace token to an unverified host.",
                    source_kind=_SOURCE_KIND,
                )
            response.raise_for_status()

            buffer = BytesIO()
            bytes_seen = 0
            for chunk in response.iter_content(chunk_size=1 << 16):
                if not chunk:
                    continue
                bytes_seen += len(chunk)
                if limit is not None and bytes_seen > limit:
                    raise SourceLimitExceededError(
                        f"Source '{path}' exceeds max_file_size={limit} bytes"
                    )
                buffer.write(chunk)
        finally:
            response.close()

        buffer.seek(0)
        return buffer

    return _with_source_retry(_do, "download document")


def is_databricks_target_authentication_error(exc: BaseException) -> bool:
    """Auth predicate for target uploads (401/403 on the raw ``requests``
    exception, *before* :func:`_with_target_retry` has a chance to translate
    it into a :class:`TargetWriteError`)."""
    return (
        isinstance(exc, requests.HTTPError)
        and exc.response is not None
        and exc.response.status_code in _AUTH_STATUS
    )


def _with_target_retry(
    fn: Callable[[], requests.Response], operation: str
) -> requests.Response:
    """Retry helper for target (upload/mkdir) calls."""
    return _retry_call(
        fn,
        operation,
        passthrough=is_databricks_target_authentication_error,
        terminal_error=lambda exc, status: TargetWriteError(
            f"Databricks Volumes rejected the write (HTTP {status})."
        ),
        exhausted_http_error=lambda exc: TargetWriteError(
            "Databricks Volumes rejected the write after retries."
        ),
        exhausted_network_error=lambda exc: TargetWriteError(
            "Databricks Volumes could not be reached."
        ),
    )


def ensure_directory(host: str, token: str, path: str) -> None:
    """Idempotent ``mkdir -p`` via ``PUT /api/2.0/fs/directories{path}``.

    The Files API documents this endpoint as creating any missing parent
    directories and as succeeding when the directory already exists, so one
    call per distinct parent prefix is enough. The upload endpoint
    (``PUT /api/2.0/fs/files``) makes no such guarantee, which is why the
    target processor calls this for every directory it is about to write into.
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
