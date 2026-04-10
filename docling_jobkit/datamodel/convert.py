"""Compatibility re-exports for service option types now owned by docling."""

from docling.datamodel.service.options import (
    ConvertDocumentsOptions,
    PictureDescriptionApi,
    PictureDescriptionLocal,
    VlmModelApi,
    VlmModelLocal,
)

__all__ = [
    "ConvertDocumentsOptions",
    "PictureDescriptionApi",
    "PictureDescriptionLocal",
    "VlmModelApi",
    "VlmModelLocal",
]
