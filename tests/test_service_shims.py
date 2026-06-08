from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.service.chunking import (
    BaseChunkerOptions as SharedBaseChunkerOptions,
)
from docling.datamodel.service.options import (
    ConvertDocumentsOptions as SharedConvertDocumentsOptions,
)
from docling.datamodel.service.responses import (
    ArtifactRef as SharedArtifactRef,
    ChunkedDocumentResult as SharedChunkedDocumentResult,
    ConvertDocumentResponse as SharedConvertDocumentResponse,
    DoclingTaskResult as SharedDoclingTaskResult,
    DocumentArtifactItem as SharedDocumentArtifactItem,
    DocumentResultItem as SharedDocumentResultItem,
    ExportDocumentResponse as SharedExportDocumentResponse,
    ExportResult as SharedExportResult,
    PresignedArtifactResult as SharedPresignedArtifactResult,
    PresignedUrlConvertDocumentResponse as SharedPresignedUrlConvertDocumentResponse,
    RemoteTargetResult as SharedRemoteTargetResult,
    ResultType as SharedResultType,
    ZipArchiveResult as SharedZipArchiveResult,
)
from docling.datamodel.service.sources import HttpSource as SharedHttpSource
from docling.datamodel.service.targets import (
    InBodyTarget as SharedInBodyTarget,
    PresignedUrlTarget as SharedPresignedUrlTarget,
)

from docling_jobkit.datamodel.chunking import BaseChunkerOptions
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.http_inputs import HttpSource
from docling_jobkit.datamodel.result import (
    ArtifactRef,
    ChunkedDocumentResult,
    DoclingTaskResult,
    DocumentArtifactItem,
    DocumentResultItem,
    ExportDocumentResponse,
    ExportResult,
    PresignedArtifactResult,
    PresignedUrlConvertDocumentResponse,
    RemoteTargetResult,
    ResultType,
    ZipArchiveResult,
)
from docling_jobkit.datamodel.task_targets import InBodyTarget, PresignedUrlTarget


def test_jobkit_convert_options_is_shared_type():
    assert ConvertDocumentsOptions is SharedConvertDocumentsOptions


def test_jobkit_chunker_options_is_shared_type():
    assert BaseChunkerOptions is SharedBaseChunkerOptions


def test_jobkit_http_source_is_shared_type():
    assert HttpSource is SharedHttpSource


def test_jobkit_inbody_target_is_shared_type():
    assert InBodyTarget is SharedInBodyTarget


def test_jobkit_presigned_target_is_shared_type():
    assert PresignedUrlTarget is SharedPresignedUrlTarget


def test_jobkit_result_models_are_shared_types():
    assert DoclingTaskResult is SharedDoclingTaskResult
    assert ChunkedDocumentResult is SharedChunkedDocumentResult
    assert ZipArchiveResult is SharedZipArchiveResult
    assert RemoteTargetResult is SharedRemoteTargetResult
    assert ResultType is SharedResultType
    assert ArtifactRef is SharedArtifactRef
    assert DocumentArtifactItem is SharedDocumentArtifactItem
    assert DocumentResultItem is SharedDocumentResultItem
    assert ExportResult is SharedExportResult
    assert PresignedArtifactResult is SharedPresignedArtifactResult
    assert (
        PresignedUrlConvertDocumentResponse is SharedPresignedUrlConvertDocumentResponse
    )


def test_shared_service_response_still_constructs_from_jobkit_result():
    assert (
        DocumentResultItem.model_fields["document"].annotation
        is SharedConvertDocumentResponse.model_fields["document"].annotation
    )
    assert (
        DocumentResultItem.model_fields["document"].annotation
        is SharedExportDocumentResponse
    )
    assert DocumentResultItem.model_fields["document"].serialization_alias == "content"
    assert ExportResult is DocumentResultItem


def test_document_result_item_serializes_document_to_legacy_content_field():
    item = DocumentResultItem(
        document=ExportDocumentResponse(filename="file.pdf"),
        status=ConversionStatus.SUCCESS,
    )

    payload = item.model_dump(mode="json")

    assert "document" not in payload
    assert payload["content"]["filename"] == "file.pdf"
