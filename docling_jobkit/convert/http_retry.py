import asyncio
import logging
import time
from io import BytesIO
from pathlib import Path
from typing import Callable

from docling.datamodel.base_models import ConversionStatus, DocumentStream
from docling.datamodel.service.responses import FailurePhase
from docling.datamodel.service.sources import HttpSource

from docling_jobkit.convert.materialization import (
    MaterializationError,
    _filename_for_http_source,
    fetch_http_source_bytes_async,
)
from docling_jobkit.datamodel.exportable_document import (
    ExportableDocument,
    source_to_public_uri,
)
from docling_jobkit.public_errors import (
    build_public_error_item,
    classify_public_task_failure,
)

_log = logging.getLogger(__name__)


def fetch_http_source_with_retry(
    source: HttpSource,
    *,
    max_file_size: int | None,
    max_retries: int,
    retry_delay: float,
    task_id: str = "",
    sleep: Callable[[float], None] = time.sleep,
) -> DocumentStream:
    """Fetch an HTTP source into a DocumentStream, retrying transient failures.

    Retryable failures (per the shared classifier: 429/502/503/504 and
    connection/timeout errors) are retried up to ``max_retries`` times with a
    fixed ``retry_delay``; permanent failures (4xx, oversize) raise immediately.
    """
    attempt = 0
    while True:
        try:
            data = asyncio.run(
                fetch_http_source_bytes_async(source, max_file_size=max_file_size)
            )
            return DocumentStream(
                name=_filename_for_http_source(source), stream=BytesIO(data)
            )
        except MaterializationError as exc:
            failure = classify_public_task_failure(
                exc, task_id=task_id, phase=FailurePhase.SOURCE_ENUMERATION
            )
            if not failure.retryable or attempt >= max_retries:
                raise
            attempt += 1
            _log.warning(
                "HTTP source %s failed (%s); retry %d/%d after %.1fs",
                source.url,
                exc,
                attempt,
                max_retries,
                retry_delay,
            )
            sleep(retry_delay)


def build_http_failure_document(
    source: HttpSource, exc: BaseException, *, source_index: int
) -> ExportableDocument:
    """Record an unfetchable HTTP source as a document-level FAILURE with its cause."""
    return ExportableDocument(
        file=Path(_filename_for_http_source(source)),
        status=ConversionStatus.FAILURE,
        errors=[build_public_error_item(exc)],
        source_index=source_index,
        source_uri=source_to_public_uri(source),
    )
