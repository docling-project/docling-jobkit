import warnings
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field

from docling.datamodel.base_models import ConversionStatus, ErrorItem
from docling.utils.profiling import ProfilingItem
from docling_core.types.doc.document import DoclingDocument

from docling_jobkit.datamodel.chunking import ChunkedDocumentResponse


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
    ExportResult | ZipArchiveResult | RemoteTargetResult | ChunkedDocumentResponse,
    Field(discriminator="kind"),
]


class DoclingTaskResult(BaseModel):
    result: ResultType
    processing_time: float
    num_converted: int
    num_succeeded: int
    num_failed: int


class ConvertDocumentResult(DoclingTaskResult):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "ConvertDocumentResult is deprecated and will be removed in a future version. "
            "Use DoclingTaskResult instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
