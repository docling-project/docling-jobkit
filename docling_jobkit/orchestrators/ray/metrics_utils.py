"""Ray Serve metrics generation utilities."""

from dataclasses import dataclass, field
from typing import Optional

from docling.datamodel.base_models import ConversionStatus, InputFormat

from docling_jobkit.datamodel.exportable_document import ExportableDocument

# Pipeline stages we know how to map onto a dcls_* histogram in emit_metrics().
_TIMING_KEYS = (
    "pipeline_total",
    "page_parse",
    "ocr",
    "layout",
    "table_structure",
    "page_assemble",
    "doc_assemble",
    "reading_order",
    "doc_enrich",
)


@dataclass
class DocumentStats:
    """Per-document counts collected for a single conversion.

    Fields other than ``input_format`` are ``None`` when the conversion
    produced no ``DoclingDocument`` (e.g. a failed conversion).
    """

    input_format: Optional[InputFormat] = None
    num_pages: Optional[int] = None
    pictures: Optional[int] = None
    tables: Optional[int] = None
    key_value_items: Optional[int] = None
    form_items: Optional[int] = None
    texts: Optional[int] = None
    groups: Optional[int] = None


@dataclass
class ConversionMetrics:
    """One conversion's metrics, ready to be emitted by RayMetricsRecorder."""

    document_hash: Optional[str]
    timings_stats: dict[str, list[float]] = field(default_factory=dict)
    document_stats: DocumentStats = field(default_factory=DocumentStats)
    status: ConversionStatus = ConversionStatus.FAILURE


def reduce_timings(timings: dict) -> dict[str, list[float]]:
    """Return the raw per-sample timing values for each known pipeline stage.

    Raw samples are kept rather than pre-aggregated into a min/max/median -
    aggregating client-side would hide a single slow page inside a
    document-level summary statistic, defeating the point of a histogram
    (accurate percentiles / outlier detection) on the Prometheus side.
    """
    conversion_timings = timings
    timing_stats: dict[str, list[float]] = {}
    if conversion_timings:
        for key in _TIMING_KEYS:
            if key in conversion_timings:
                timing_stats[key] = list(conversion_timings[key].times)
    return timing_stats


def collect_doc_stats(exp_doc: ExportableDocument) -> DocumentStats:
    doc_stats = DocumentStats(input_format=exp_doc.document_type)
    doc = exp_doc.document
    if doc is not None:
        doc_stats.num_pages = len(doc.pages)
        doc_stats.pictures = len(doc.pictures)
        doc_stats.tables = len(doc.tables)
        doc_stats.key_value_items = len(doc.key_value_items)
        doc_stats.form_items = len(doc.form_items)
        doc_stats.texts = len(doc.texts)
        doc_stats.groups = len(doc.groups)
    return doc_stats


def get_metrics_from_exportable_doc(exp_doc: ExportableDocument) -> ConversionMetrics:
    return ConversionMetrics(
        document_hash=exp_doc.document_hash,
        timings_stats=reduce_timings(timings=exp_doc.timings),
        document_stats=collect_doc_stats(exp_doc=exp_doc),
        status=exp_doc.status,
    )
