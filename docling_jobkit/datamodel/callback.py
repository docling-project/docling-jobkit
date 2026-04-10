"""Compatibility re-exports for callback wire types now owned by docling."""

from docling.datamodel.service.callbacks import (
    BaseProgress,
    CallbackSpec,
    DocumentCompletedItem,
    FailedDocsItem,
    ProgressCallbackRequest,
    ProgressCallbackResponse,
    ProgressDocumentCompleted,
    ProgressKind,
    ProgressSetNumDocs,
    ProgressUpdateProcessed,
    SucceededDocsItem,
)

__all__ = [
    "BaseProgress",
    "CallbackSpec",
    "DocumentCompletedItem",
    "FailedDocsItem",
    "ProgressCallbackRequest",
    "ProgressCallbackResponse",
    "ProgressDocumentCompleted",
    "ProgressKind",
    "ProgressSetNumDocs",
    "ProgressUpdateProcessed",
    "SucceededDocsItem",
]
