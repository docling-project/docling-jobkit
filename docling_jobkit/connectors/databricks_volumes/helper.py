import logging
import time
from typing import Any, Callable, Iterator, Optional

import requests

from docling_jobkit.connectors.errors import (
    SourceConnectorPolicyError,
    SourceConnectorUnavailableError,
)

_log = logging.getLogger(__name__)

_SOURCE_KIND = "databricks_volumes"
_MAX_RETRIES = 3
_BACKOFF_BASE_S = 0.5
_RETRYABLE_4XX_STATUS = {429}


def _with_exponential_retry(fn: Callable[[], Any], operation: str) -> Any:
    """Helper for exponential retries on transient errors."""
    for attempt in range(_MAX_RETRIES + 1):
        try:
            result = fn()
            if isinstance(result, requests.Response):
                result.raise_for_status()
            return result
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt == _MAX_RETRIES:
                raise SourceConnectorUnavailableError(
                    "Databricks Volumes could not be reached.",
                    source_kind=_SOURCE_KIND,
                ) from exc
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if (
                status is not None
                and status < 500
                and status not in _RETRYABLE_4XX_STATUS
            ):
                error_type = (
                    SourceConnectorPolicyError
                    if status in {401, 403, 404, 413, 415, 422}
                    else SourceConnectorUnavailableError
                )
                raise error_type(
                    str(exc),
                    source_kind=_SOURCE_KIND,
                    **(
                        {"retryable": False}
                        if error_type is SourceConnectorUnavailableError
                        else {}
                    ),
                ) from exc
            if attempt == _MAX_RETRIES:
                raise SourceConnectorUnavailableError(
                    str(exc),
                    source_kind=_SOURCE_KIND,
                ) from exc

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


def list_directory_page(
    host: str,
    token: str,
    path: str,
    *,
    page_token: Optional[str] = None,
    page_size: int = 1000,
) -> dict:
    """One page of ``GET /api/2.0/fs/directories{path}``.

    Returns the raw JSON body: ``{"contents": [...], "next_page_token": ...}``.
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

    response = _with_exponential_retry(_do, "list directory")
    return response.json()


def iter_directory(host: str, token: str, path: str) -> Iterator[dict]:
    """Yield every entry in one directory level of a Volume, transparently
    paging via ``next_page_token``.

    Each entry is a dict with ``name``, ``path``, ``is_directory``, and (for
    files) ``file_size``/``last_modified``, per the Files API response shape.
    The List Directory API is single-level only — callers wanting a recursive
    walk must recurse into entries where ``is_directory`` is ``True``.
    """
    page_token: Optional[str] = None
    while True:
        data = list_directory_page(host, token, path, page_token=page_token)
        yield from data.get("contents", [])
        page_token = data.get("next_page_token")
        if not page_token:
            return
