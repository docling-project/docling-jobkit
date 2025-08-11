from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field

from docling.datamodel.document import ConversionStatus, ErrorItem
from docling.utils.profiling import ProfilingItem
from docling_core.types.doc import DoclingDocument


class ExportDocumentResponse(BaseModel):
    filename: str
    md_content: Optional[str] = None
    json_content: Optional[DoclingDocument] = None
    html_content: Optional[str] = None
    text_content: Optional[str] = None
    doctags_content: Optional[str] = None


class ExportResult(BaseModel):
    """Container of all exported content."""

    kind: Literal["ExportResult"] = "ExportResult"
    content: ExportDocumentResponse
    status: ConversionStatus
    errors: list[ErrorItem] = []
    timings: dict[str, ProfilingItem] = {}


class ZipArchiveResult(BaseModel):
    """Container for a zip archive of the conversion."""

    kind: Literal["ZipArchiveResult"] = "ZipArchiveResult"
    content: bytes


class RemoteTargetResult(BaseModel):
    """No content, the result has been pushed to a remote target."""

    kind: Literal["RemoteTargetResult"] = "RemoteTargetResult"


ResultType = Annotated[
    ExportResult | ZipArchiveResult | RemoteTargetResult, Field(discriminator="kind")
]


class ConvertDocumentResult(BaseModel):
    result: ResultType
    processing_time: float
    num_converted: int
    num_succeeded: int
    num_failed: int
