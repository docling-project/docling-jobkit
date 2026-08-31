"""Ray Serve metrics generation utilities."""

from typing import Any

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


def reduce_timings(timings: dict) -> dict:
    """Return the raw per-sample timing values for each known pipeline stage.

    Raw samples are kept rather than pre-aggregated into a min/max/median -
    aggregating client-side would hide a single slow page inside a
    document-level summary statistic, defeating the point of a histogram
    (accurate percentiles / outlier detection) on the Prometheus side.
    """
    conversion_timings = timings
    timing_stats: dict = {}
    if conversion_timings:
        for key in _TIMING_KEYS:
            if key in conversion_timings:
                timing_stats[key] = list(conversion_timings[key].times)
    return timing_stats


def collect_doc_stats(exp_doc: ExportableDocument):
    doc_stats: dict[str, Any] = {}

    doc_stats["input_format"] = exp_doc.document_type
    doc = exp_doc.document
    if doc is not None:
        doc_stats["num_pages"] = len(doc.pages)
        doc_stats["pictures"] = len(doc.pictures)
        doc_stats["tables"] = len(doc.tables)
        doc_stats["key_value_items"] = len(doc.key_value_items)
        doc_stats["form_items"] = len(doc.form_items)
        doc_stats["texts"] = len(doc.texts)
        doc_stats["groups"] = len(doc.groups)
    return doc_stats


def get_metrics_from_exportable_doc(exp_doc: ExportableDocument):
    metrics: dict[str, Any] = {}
    metrics["document_hash"] = exp_doc.document_hash
    metrics["timings_stats"] = reduce_timings(timings=exp_doc.timings)
    metrics["document_stats"] = collect_doc_stats(exp_doc=exp_doc)
    metrics["status"] = exp_doc.status

    return metrics
