from pathlib import PurePath
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from docling.datamodel.base_models import ConversionStatus, ErrorItem
from docling.datamodel.document import ConversionResult
from docling.utils.profiling import ProfilingItem
from docling_core.types.doc.document import DoclingDocument


class ExportableDocument(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    file: PurePath = Field(description="Input filename or path metadata")
    document_hash: Optional[str] = Field(
        default=None, description="Stable hash of the input document"
    )
    status: ConversionStatus = Field(description="Conversion status")
    errors: list[ErrorItem] = Field(
        default_factory=list, description="Conversion errors"
    )
    timings: dict[str, ProfilingItem] = Field(
        default_factory=dict, description="Profiling timings"
    )
    document: Optional[DoclingDocument] = Field(
        default=None, description="Converted document content when exportable"
    )
    page_range: Optional[tuple[int, int]] = Field(
        default=None, description="Absolute page range for a sliced child document"
    )
    slice_index: Optional[int] = Field(
        default=None, description="Ascending slice index for a sliced child document"
    )

    @classmethod
    def from_conversion_result(
        cls,
        conversion_result: ConversionResult,
        *,
        page_range: Optional[tuple[int, int]] = None,
        slice_index: Optional[int] = None,
    ) -> "ExportableDocument":
        document: Optional[DoclingDocument] = conversion_result.document
        if conversion_result.status not in (
            ConversionStatus.SUCCESS,
            ConversionStatus.PARTIAL_SUCCESS,
        ):
            document = None

        return cls(
            file=conversion_result.input.file,
            document_hash=conversion_result.input.document_hash,
            status=conversion_result.status,
            errors=conversion_result.errors,
            timings=conversion_result.timings,
            document=document,
            page_range=page_range,
            slice_index=slice_index,
        )
