from __future__ import annotations

import asyncio
import sys
from io import BytesIO
from pathlib import PurePath
from urllib.parse import urlparse

import requests
from pydantic import BaseModel, ConfigDict, Field

from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel.base_models import InputFormat
from docling.datamodel.document import InputDocument
from docling.datamodel.service.sources import FileSource, HttpSource
from docling.datamodel.settings import DocumentLimits
from docling_core.utils.file import (
    FileSizeLimitExceededError,
    resolve_source_to_stream,
)


# Admission-control parameters applied before any conversion work begins.
class MaterializationLimits(BaseModel):
    max_file_size: int = Field(description="Maximum allowed source size in bytes")
    max_num_pages: int = Field(description="Maximum allowed PDF page count")


# Eagerly-loaded, preflighted source payload shared across coordinator and workers.
# The byte alias ("bytes") keeps this serialization-compatible with existing
# Ray object-store and msgpack consumers that expect the original field name.
class MaterializedSource(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    content_bytes: bytes = Field(
        alias="bytes",
        serialization_alias="bytes",
        description="Materialized PDF bytes",
    )
    page_count: int = Field(description="Total page count for the full source PDF")
    filename: str = Field(description="Filename used for exports and hashing")


class MaterializationError(RuntimeError):
    """Base error for source materialization and PDF preflight failures."""


class MaterializationLimitExceededError(MaterializationError):
    """Raised when the source exceeds configured document admission limits."""


class SourceLimitExceededError(MaterializationError):
    """Raised when source retrieval exceeds a configured fetch-time size limit."""


class SourceFetchError(MaterializationError):
    """Raised when a remote source cannot be downloaded.

    Covers unreachable hosts, connection/timeout failures, and error status
    codes (404/403/5xx) for an otherwise well-formed URL. Typed so callers can
    surface it as a document-level FAILURE instead of a generic internal error.
    """


def normalize_max_file_size(max_file_size: int | None) -> int | None:
    if max_file_size is None or max_file_size >= sys.maxsize:
        return None
    return max_file_size


def _filename_for_http_source(source: HttpSource) -> str:
    parsed = urlparse(str(source.url))
    filename = PurePath(parsed.path).name
    return filename or "document.pdf"


async def fetch_http_source_bytes_async(
    source: HttpSource,
    *,
    max_file_size: int | None,
    error_cls: type[MaterializationError] = SourceLimitExceededError,
) -> bytes:
    """Download an HTTP source through docling-core's guarded fetch.

    ``resolve_source_to_stream`` is the call the converter itself uses for URL
    sources. Going through it keeps this path under the same private-address
    check (initial URL, every redirect, and the connect-time address, subject to
    ``DOCLINGCORE_ALLOWED_PRIVATE_IPS``) and the same download size cap. Do not
    fetch the URL with another HTTP client here.
    """
    filename = _filename_for_http_source(source)
    limit = normalize_max_file_size(max_file_size)
    try:
        # The fetch is synchronous, so keep it off the event loop.
        document_stream = await asyncio.to_thread(
            resolve_source_to_stream,
            str(source.url),
            headers=source.headers,
            max_file_size=limit,
        )
    except FileSizeLimitExceededError as exc:
        raise error_cls(
            f"Source '{filename}' exceeds max_file_size={limit} bytes"
        ) from exc
    except (ValueError, requests.RequestException) as exc:
        # A refused address (ValueError), an unreachable host, a
        # connection/timeout failure, or an error status (404/403/5xx).
        # Translate to a typed source error so the caller records a
        # document-level FAILURE instead of a generic internal task error.
        raise SourceFetchError(
            f"Source '{filename}' could not be downloaded: {exc}"
        ) from exc
    return document_stream.stream.getvalue()


async def materialize_and_preflight(
    source: FileSource | HttpSource,
    limits: MaterializationLimits,
) -> MaterializedSource:
    """Resolve a source to bytes and run PDF preflight before any conversion.

    Downloading/reading happens here so the coordinator can put the bytes in the
    Ray object store once and let all worker slices read from it without
    re-fetching.  InputDocument is opened purely for preflight: PyPdfium validates
    the file structure and reports the page count without triggering the full
    pipeline.  Limit checks are performed explicitly so callers receive typed
    MaterializationLimitExceededError (and can surface it as HTTP 422) instead of
    the generic validity failure from docling.
    """
    if isinstance(source, FileSource):
        source_bytes = source.to_document_stream().stream.getvalue()
        filename = source.filename
    else:
        source_bytes = await fetch_http_source_bytes_async(
            source,
            max_file_size=limits.max_file_size,
            error_cls=MaterializationLimitExceededError,
        )
        filename = _filename_for_http_source(source)

    input_doc = InputDocument(
        path_or_stream=BytesIO(source_bytes),
        format=InputFormat.PDF,
        backend=PyPdfiumDocumentBackend,
        filename=filename,
        limits=DocumentLimits(
            max_file_size=limits.max_file_size,
            max_num_pages=limits.max_num_pages,
        ),
    )

    input_filesize = input_doc.filesize
    if input_filesize is None:
        raise MaterializationError(f"Source '{filename}' did not report a filesize")

    if input_filesize > limits.max_file_size:
        raise MaterializationLimitExceededError(
            f"Source '{filename}' exceeds max_file_size={limits.max_file_size} bytes"
        )
    if input_doc.page_count > limits.max_num_pages:
        raise MaterializationLimitExceededError(
            f"Source '{filename}' exceeds max_num_pages={limits.max_num_pages}"
        )
    if not input_doc.valid:
        raise MaterializationError(f"Source '{filename}' failed PDF preflight")

    return MaterializedSource(
        content_bytes=source_bytes,
        page_count=input_doc.page_count,
        filename=filename,
    )
