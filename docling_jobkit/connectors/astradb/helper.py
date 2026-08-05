import logging
import time
from typing import Any, Callable, TypeVar

import httpx

_log = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRYABLE_4XX_STATUS = frozenset({408, 429})  # Timeout, rate limit
_BACKOFF_BASE = 1.0

T = TypeVar("T")


def _with_exponential_retry(
    fn: Callable[[], T],
    operation: str,
    max_retries: int = _MAX_RETRIES,
) -> T:
    """helper for exponential retries on transient errors"""
    from astrapy.exceptions import DataAPIHttpException, DataAPITimeoutException

    last_exc: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            return fn()
        except DataAPITimeoutException as exc:
            last_exc = exc
        except DataAPIHttpException as exc:
            status = exc.httpx_error.response.status_code
            if status < 500 and status not in _RETRYABLE_4XX_STATUS:
                raise  # Permanent 4xx error
            last_exc = exc
        except httpx.TransportError as exc:
            last_exc = exc

        if attempt < max_retries:
            wait = _BACKOFF_BASE * (2**attempt)
            _log.warning(
                "AstraDB: %s transient error, retry %d/%d in %.1fs",
                operation,
                attempt + 1,
                max_retries,
                wait,
            )
            time.sleep(wait)

    _log.error("AstraDB: %s failed after %d attempts", operation, max_retries + 1)
    raise last_exc  # type: ignore[misc]


def upsert_record_with_retry(
    collection: Any,
    record: dict[str, Any],
    max_retries: int = _MAX_RETRIES,
) -> None:
    """Upsert a single record with retry logic.

    Uses update_one with upsert=True for idempotent writes. The record must
    contain an '_id' field which is used as the document identifier.
    """
    if "_id" not in record:
        raise ValueError("Record must contain '_id' field for upsert")

    record_id = record["_id"]
    update_doc = {k: v for k, v in record.items() if k != "_id"}

    def _upsert():
        collection.update_one(
            {"_id": record_id},
            {"$set": update_doc},
            upsert=True,
        )

    _with_exponential_retry(_upsert, "upsert_record", max_retries)


__all__ = [
    "upsert_record_with_retry",
]
