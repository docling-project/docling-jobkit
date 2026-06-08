"""Compatibility re-exports for callback wire types now owned by docling."""

from docling.datamodel.service.callbacks import (
    BaseProgress,
    CallbackSpec,
    DocumentCompletedItem,
    ProcessedDocsItem,
    ProgressCallbackRequest,
    ProgressCallbackResponse,
    ProgressDocumentCompleted,
    ProgressKind,
    ProgressSetNumDocs,
    ProgressUpdateProcessed,
)

__all__ = [
    "BaseProgress",
    "CallbackSpec",
    "DocumentCompletedItem",
    "ProcessedDocsItem",
    "ProgressCallbackRequest",
    "ProgressCallbackResponse",
    "ProgressDocumentCompleted",
    "ProgressKind",
    "ProgressSetNumDocs",
    "ProgressUpdateProcessed",
]
