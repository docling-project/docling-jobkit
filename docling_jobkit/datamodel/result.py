import warnings
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field

from docling.datamodel.service.responses import (
    ChunkedDocumentResultItem,
    DoclingTaskResult,
    ExportDocumentResponse,
    ExportResult,
)


class ZipArchiveResult(BaseModel):
    """Container for a zip archive of the conversion."""

    kind: Literal["ZipArchiveResult"] = "ZipArchiveResult"
    content: bytes


class RemoteTargetResult(BaseModel):
    """No content, the result has been pushed to a remote target."""

    kind: Literal["RemoteTargetResult"] = "RemoteTargetResult"


class ChunkedDocumentResult(BaseModel):
    kind: Literal["ChunkedDocumentResponse"] = "ChunkedDocumentResponse"
    chunks: list[ChunkedDocumentResultItem]
    documents: list[ExportResult]
    chunking_info: Optional[dict] = None


ResultType = Annotated[
    ExportResult | ZipArchiveResult | RemoteTargetResult | ChunkedDocumentResult,
    Field(discriminator="kind"),
]


class ConvertDocumentResult(DoclingTaskResult):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "ConvertDocumentResult is deprecated and will be removed in a future version. "
            "Use DoclingTaskResult instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


__all__ = [
    "ChunkedDocumentResult",
    "ChunkedDocumentResultItem",
    "ConvertDocumentResult",
    "DoclingTaskResult",
    "ExportDocumentResponse",
    "ExportResult",
    "RemoteTargetResult",
    "ResultType",
    "ZipArchiveResult",
]
