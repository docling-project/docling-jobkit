"""Compatibility re-exports for service option types now owned by docling."""

from docling.datamodel.chart_extraction_options import ChartExtractionVlmEngineOptions
from docling.datamodel.service.options import (
    ConvertDocumentsOptions,
    PictureDescriptionApi,
    PictureDescriptionLocal,
    VlmModelApi,
    VlmModelLocal,
)

__all__ = [
    "ChartExtractionVlmEngineOptions",
    "ConvertDocumentsOptions",
    "PictureDescriptionApi",
    "PictureDescriptionLocal",
    "VlmModelApi",
    "VlmModelLocal",
]
