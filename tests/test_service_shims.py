from docling.datamodel.service.chunking import (
    BaseChunkerOptions as SharedBaseChunkerOptions,
)
from docling.datamodel.service.options import (
    ConvertDocumentsOptions as SharedConvertDocumentsOptions,
)
from docling.datamodel.service.responses import (
    ChunkedDocumentResult as SharedChunkedDocumentResult,
    ConvertDocumentResponse as SharedConvertDocumentResponse,
    DoclingTaskResult as SharedDoclingTaskResult,
    RemoteTargetResult as SharedRemoteTargetResult,
    ResultType as SharedResultType,
    ZipArchiveResult as SharedZipArchiveResult,
)
from docling.datamodel.service.sources import HttpSource as SharedHttpSource
from docling.datamodel.service.targets import InBodyTarget as SharedInBodyTarget

from docling_jobkit.datamodel.chunking import BaseChunkerOptions
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.http_inputs import HttpSource
from docling_jobkit.datamodel.result import (
    ChunkedDocumentResult,
    DoclingTaskResult,
    ExportResult,
    RemoteTargetResult,
    ResultType,
    ZipArchiveResult,
)
from docling_jobkit.datamodel.task_targets import InBodyTarget


def test_jobkit_convert_options_is_shared_type():
    assert ConvertDocumentsOptions is SharedConvertDocumentsOptions


def test_jobkit_chunker_options_is_shared_type():
    assert BaseChunkerOptions is SharedBaseChunkerOptions


def test_jobkit_http_source_is_shared_type():
    assert HttpSource is SharedHttpSource


def test_jobkit_inbody_target_is_shared_type():
    assert InBodyTarget is SharedInBodyTarget


def test_jobkit_result_models_are_shared_types():
    assert DoclingTaskResult is SharedDoclingTaskResult
    assert ChunkedDocumentResult is SharedChunkedDocumentResult
    assert ZipArchiveResult is SharedZipArchiveResult
    assert RemoteTargetResult is SharedRemoteTargetResult
    assert ResultType is SharedResultType


def test_shared_service_response_still_constructs_from_jobkit_result():
    assert (
        ExportResult.model_fields["content"].annotation
        is SharedConvertDocumentResponse.model_fields["document"].annotation
    )
