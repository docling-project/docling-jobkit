"""Compatibility re-exports for service option types now owned by docling."""

from docling.datamodel.chart_extraction_options import ChartExtractionVlmEngineOptions
from docling.datamodel.service.options import (
    ConvertDocumentsOptions,
    PictureDescriptionApi,
    PictureDescriptionLocal,
    VlmModelApi,
    VlmModelLocal,
)

MAX_SAFE_PAGE: int = 2**31 - 1
"""Maximum safe page number (2^31 - 1) fitting within standard 32-bit signed integer limits."""


def normalize_page_range(page_range: tuple[int, int]) -> tuple[int, int]:
    """Clamps a 1-based page range to safe integer bounds.

    Large upper bounds like sys.maxsize (2^63 - 1) sent by clients can cause
    integer overflow or internal errors in downstream PDF backends and service
    components. This helper clamps both bounds to [1, MAX_SAFE_PAGE] while
    preserving start <= end.

    Args:
        page_range: A 1-based (start_page, end_page) tuple.

    Returns:
        A clamped (start_page, end_page) tuple where start >= 1 and
        start <= end <= MAX_SAFE_PAGE.
    """
    start, end = page_range
    start_clamped = min(max(1, start), MAX_SAFE_PAGE)
    end_clamped = min(max(start_clamped, end), MAX_SAFE_PAGE)
    return (start_clamped, end_clamped)


__all__ = [
    "MAX_SAFE_PAGE",
    "ChartExtractionVlmEngineOptions",
    "ConvertDocumentsOptions",
    "PictureDescriptionApi",
    "PictureDescriptionLocal",
    "VlmModelApi",
    "VlmModelLocal",
    "normalize_page_range",
]
