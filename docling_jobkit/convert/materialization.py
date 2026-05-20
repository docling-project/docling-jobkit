from __future__ import annotations

from io import BytesIO
from pathlib import PurePath
from urllib.parse import urlparse

import httpx
from pydantic import BaseModel, ConfigDict, Field

from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel.base_models import InputFormat
from docling.datamodel.document import InputDocument
from docling.datamodel.service.sources import FileSource, HttpSource
from docling.datamodel.settings import DocumentLimits


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


def _filename_for_http_source(source: HttpSource) -> str:
    parsed = urlparse(str(source.url))
    filename = PurePath(parsed.path).name
    return filename or "document.pdf"


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
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(
                str(source.url),
                headers=source.headers,
            )
            response.raise_for_status()
            source_bytes = response.content
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
