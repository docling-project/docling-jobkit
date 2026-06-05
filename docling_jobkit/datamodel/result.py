from typing import TYPE_CHECKING, TypeAlias

from docling.datamodel.service.responses import (
    ArtifactRef,
    ChunkedDocumentResult,
    ChunkedDocumentResultItem,
    DoclingTaskResult,
    DocumentArtifactItem,
    DocumentResultItem,
    ExportDocumentResponse,
    ExportResult,
    FailureCategory,
    FailurePhase,
    PresignedArtifactResult,
    PresignedUrlConvertDocumentResponse,
    PresignedUrlConvertResponse,
    PublicFailureInfo,
    RemoteTargetResult,
    ResultType,
    TaskFailureResult,
    ZipArchiveResult,
)

if TYPE_CHECKING:
    from docling_jobkit.datamodel.stored_outcome import StoredTaskOutcome

TaskOutcome: TypeAlias = "StoredTaskOutcome | DoclingTaskResult"

__all__ = [
    "ArtifactRef",
    "ChunkedDocumentResult",
    "ChunkedDocumentResultItem",
    "DoclingTaskResult",
    "DocumentArtifactItem",
    "DocumentResultItem",
    "ExportDocumentResponse",
    "ExportResult",
    "FailureCategory",
    "FailurePhase",
    "PresignedArtifactResult",
    "PresignedUrlConvertDocumentResponse",
    "PresignedUrlConvertResponse",
    "PublicFailureInfo",
    "RemoteTargetResult",
    "ResultType",
    "TaskFailureResult",
    "TaskOutcome",
    "ZipArchiveResult",
]
