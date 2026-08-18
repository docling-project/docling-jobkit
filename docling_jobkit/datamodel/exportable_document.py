from pathlib import PurePath
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from docling.datamodel.base_models import (
    ConversionStatus,
    DocumentStream,
    ErrorItem,
    InputFormat,
)
from docling.datamodel.document import ConversionResult
from docling.datamodel.service.responses import ConfidenceScores
from docling.datamodel.service.sources import (
    AzureBlobCoordinates,
    FileSource,
    GoogleCloudStorageCoordinates,
    GoogleDriveCoordinates,
    HttpSource,
    S3Coordinates,
)
from docling.utils.profiling import ProfilingItem
from docling_core.types.doc.document import DoclingDocument


def project_document(
    document: Optional[DoclingDocument], target_version: Optional[str]
) -> Optional[DoclingDocument]:
    """Down-project a produced doc to the newest version the client can read.

    Targets at or above ``document.version`` return the original document.
    Lower targets use ``project_to``, including its hard-fail below the floor.
    """
    if document is None or not target_version:
        return document

    target_parts = target_version.split(".")
    document_parts = document.version.split(".")
    if (
        len(target_parts) == len(document_parts) == 3
        and all(part.isdigit() for part in target_parts + document_parts)
        and tuple(map(int, target_parts)) >= tuple(map(int, document_parts))
    ):
        return document

    from docling_core.compat import project_to

    return project_to(document, target_version)


def source_to_public_uri(source: object) -> str | None:
    if isinstance(source, HttpSource):
        return str(source.url)
    if isinstance(source, FileSource):
        return source.filename
    if isinstance(source, S3Coordinates):
        key_prefix = source.key_prefix.lstrip("/")
        if key_prefix:
            return f"s3://{source.bucket}/{key_prefix}"
        return f"s3://{source.bucket}"
    if isinstance(source, AzureBlobCoordinates):
        blob_prefix = source.blob_prefix.lstrip("/")
        if blob_prefix:
            return f"azure://{source.account_name}/{source.container}/{blob_prefix}"
        return f"azure://{source.account_name}/{source.container}"
    if isinstance(source, GoogleCloudStorageCoordinates):
        key_prefix = source.key_prefix.lstrip("/")
        if key_prefix:
            return f"gs://{source.bucket}/{key_prefix}"
        return f"gs://{source.bucket}"
    if isinstance(source, GoogleDriveCoordinates):
        return f"gdrive://{source.path_id}"
    if isinstance(source, DocumentStream):
        return source.name
    return None


class ExportableDocument(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    file: PurePath = Field(description="Input filename or path metadata")
    document_hash: Optional[str] = Field(
        default=None, description="Stable hash of the input document"
    )
    document_type: Optional[InputFormat] = Field(
        default=None, description="Detected input document format"
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
    confidence: Optional[ConfidenceScores] = Field(
        default=None, description="Document-level confidence scores, if computed"
    )
    source_index: Optional[int] = Field(
        default=None, description="Ordinal position in the expanded document list"
    )
    source_uri: Optional[str] = Field(
        default=None, description="Stable public identity of the expanded source"
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
        source_index: Optional[int] = None,
        source_uri: Optional[str] = None,
        page_range: Optional[tuple[int, int]] = None,
        slice_index: Optional[int] = None,
        target_version: Optional[str] = None,
    ) -> "ExportableDocument":
        document: Optional[DoclingDocument] = conversion_result.document
        confidence: Optional[ConfidenceScores] = None
        if conversion_result.status in (
            ConversionStatus.SUCCESS,
            ConversionStatus.PARTIAL_SUCCESS,
        ):
            confidence = ConfidenceScores.from_scores(conversion_result.confidence)
        else:
            document = None

        document = project_document(document, target_version)

        return cls(
            file=conversion_result.input.file,
            document_hash=conversion_result.input.document_hash,
            document_type=conversion_result.input.format,
            status=conversion_result.status,
            errors=conversion_result.errors,
            timings=conversion_result.timings,
            document=document,
            confidence=confidence,
            source_index=source_index,
            source_uri=source_uri,
            page_range=page_range,
            slice_index=slice_index,
        )
