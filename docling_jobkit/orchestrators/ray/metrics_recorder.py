"""Standalone Prometheus/Ray metrics recorder for docling conversions.

Both the coordinator and converter Ray Serve deployments construct their own
instance of this class at __init__ time (when generate_metrics is enabled),
so metrics can be emitted directly from whichever actor produced them,
without depending on a remote call to a converter replica.
"""

import logging

from ray import serve
from ray.util.metrics import Counter, Histogram

from docling.datamodel.base_models import InputFormat

from docling_jobkit.orchestrators.ray.metrics_utils import (
    ConversionMetrics,
    DocumentStats,
)

_log = logging.getLogger(__name__)

_TAG_KEYS = ("tenant_id", "replica_tag")
_DOC_TYPE_TAG_KEYS = (*_TAG_KEYS, "format")

_TIMINGS_HIST_BUCKETS = [
    0.000001,
    0.00001,
    0.0001,
    0.001,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    1.25,
    1.5,
    1.75,
    2.0,
    2.5,
    3.0,
    3.5,
    4.0,
    5.0,
    6.0,
    7.0,
    8.0,
    9.0,
    10.0,
    12.5,
    15.0,
    17.5,
    20.0,
    25.0,
    30.0,
    # Coarser tail buckets so a genuine aberration (stuck OCR/layout call,
    # etc.) gets resolution instead of collapsing into +Inf.
    45.0,
    60.0,
    90.0,
    120.0,
]


def _doc_type_label(doc_type: InputFormat | None) -> str:
    if doc_type == InputFormat.PDF:
        return "pdf"
    elif doc_type == InputFormat.DOCX:
        return "docx"
    elif doc_type == InputFormat.PPTX:
        return "pptx"
    elif doc_type == InputFormat.HTML:
        return "html"
    elif doc_type == InputFormat.IMAGE:
        return "image"
    elif doc_type == InputFormat.MD:
        return "md"
    elif doc_type == InputFormat.XLSX:
        return "xlsx"
    elif doc_type in (
        InputFormat.XML_USPTO,
        InputFormat.XML_JATS,
        InputFormat.XML_XBRL,
    ):
        return "xml"
    elif doc_type == InputFormat.XML_DOCLANG:
        return "doclang"
    elif doc_type == InputFormat.JSON_DOCLING:
        return "docling"
    else:
        return "other"


class RayMetricsRecorder:
    """Owns the dcls_* Counter/Histogram objects and emits observations for
    them. Construct one instance per actor that has generate_metrics
    enabled; Ray's metrics backend aggregates same-named metrics reported by
    multiple actors, tagged by replica_tag, exactly like multiple converter
    replicas already do today."""

    def __init__(self) -> None:
        self.metric_emission_counter = Counter(
            "dcls_metrics_emitted",
            description="Number of attempts to emit metrics",
            tag_keys=_TAG_KEYS,
        )
        self.success_counter = Counter(
            "dcls_conversion_success",
            description="Number of successful conversions",
            tag_keys=_TAG_KEYS,
        )
        self.partial_counter = Counter(
            "dcls_conversion_partial",
            description="Number of partial conversions",
            tag_keys=_TAG_KEYS,
        )
        self.failed_counter = Counter(
            "dcls_conversion_failed",
            description="Number of failed conversions",
            tag_keys=_TAG_KEYS,
        )
        self.pipeline_total_hist = Histogram(
            "dcls_pipeline_total",
            description="Total pipeline execution time in seconds",
            boundaries=_TIMINGS_HIST_BUCKETS,
            tag_keys=_TAG_KEYS,
        )
        self.page_parse_hist = Histogram(
            "dcls_page_parse",
            description="Per-page parse time in seconds",
            boundaries=_TIMINGS_HIST_BUCKETS,
            tag_keys=_TAG_KEYS,
        )
        self.ocr_hist = Histogram(
            "dcls_ocr",
            description="Per-page OCR time in seconds",
            boundaries=_TIMINGS_HIST_BUCKETS,
            tag_keys=_TAG_KEYS,
        )
        self.layout_hist = Histogram(
            "dcls_layout",
            description="Per-page layout time in seconds",
            boundaries=_TIMINGS_HIST_BUCKETS,
            tag_keys=_TAG_KEYS,
        )
        self.table_structure_hist = Histogram(
            "dcls_table_structure",
            description="Per-page table structure time in seconds",
            boundaries=_TIMINGS_HIST_BUCKETS,
            tag_keys=_TAG_KEYS,
        )
        self.page_assemble_hist = Histogram(
            "dcls_page_assemble",
            description="Per-page assemble time in seconds",
            boundaries=_TIMINGS_HIST_BUCKETS,
            tag_keys=_TAG_KEYS,
        )
        self.doc_assemble_hist = Histogram(
            "dcls_doc_assemble",
            description="Document assemble time in seconds",
            boundaries=_TIMINGS_HIST_BUCKETS,
            tag_keys=_TAG_KEYS,
        )
        self.reading_order_hist = Histogram(
            "dcls_reading_order",
            description="Reading order time in seconds",
            boundaries=_TIMINGS_HIST_BUCKETS,
            tag_keys=_TAG_KEYS,
        )
        self.doc_enrich_hist = Histogram(
            "dcls_doc_enrich",
            description="Document enrichment time in seconds",
            boundaries=_TIMINGS_HIST_BUCKETS,
            tag_keys=_TAG_KEYS,
        )
        self.doc_type_counter = Counter(
            "dcls_doc_type",
            description="Number of documents converted, by format",
            tag_keys=_DOC_TYPE_TAG_KEYS,
        )
        self.num_pages_counter = Counter(
            "dcls_num_pages",
            description="Number of pages in converted document",
            tag_keys=_TAG_KEYS,
        )
        self.pictures_counter = Counter(
            "dcls_pictures",
            description="Number of pictures in converted document",
            tag_keys=_TAG_KEYS,
        )
        self.tables_counter = Counter(
            "dcls_tables",
            description="Number of tables in converted document",
            tag_keys=_TAG_KEYS,
        )
        self.key_value_items_counter = Counter(
            "dcls_key_value_items",
            description="Number of key value items in converted document",
            tag_keys=_TAG_KEYS,
        )
        self.form_items_counter = Counter(
            "dcls_form_items",
            description="Number of form items in converted document",
            tag_keys=_TAG_KEYS,
        )
        self.texts_counter = Counter(
            "dcls_texts",
            description="Number of text items in converted document",
            tag_keys=_TAG_KEYS,
        )
        self.groups_counter = Counter(
            "dcls_groups",
            description="Number of group items in converted document",
            tag_keys=_TAG_KEYS,
        )

    def _observe_all(self, hist: Histogram, values: list, tags: dict) -> None:
        # Observe each raw sample (not a pre-aggregated min/max/median) so
        # Prometheus can compute accurate percentiles and surface a single
        # slow page instead of it being smoothed into a document average.
        for value in values:
            hist.observe(value, tags=tags)

    def _emit_timing_stats(self, timings_stats: dict[str, list], tags: dict) -> None:
        for key, hist in (
            ("pipeline_total", self.pipeline_total_hist),
            ("page_parse", self.page_parse_hist),
            ("ocr", self.ocr_hist),
            ("layout", self.layout_hist),
            ("table_structure", self.table_structure_hist),
            ("page_assemble", self.page_assemble_hist),
            ("doc_assemble", self.doc_assemble_hist),
            ("reading_order", self.reading_order_hist),
            ("doc_enrich", self.doc_enrich_hist),
        ):
            values = timings_stats.get(key)
            if values:
                self._observe_all(hist, values, tags)

    def _emit_document_stats(self, document_stats: DocumentStats, tags: dict) -> None:
        if document_stats.input_format is not None:
            self.doc_type_counter.inc(
                tags={**tags, "format": _doc_type_label(document_stats.input_format)}
            )
        # Ray's Counter.inc() rejects 0 (requires value > 0), and a document
        # can legitimately have 0 tables/pictures/etc., so these are truthy
        # checks rather than `is not None` checks.
        if document_stats.num_pages:
            self.num_pages_counter.inc(document_stats.num_pages, tags=tags)
        if document_stats.pictures:
            self.pictures_counter.inc(document_stats.pictures, tags=tags)
        if document_stats.tables:
            self.tables_counter.inc(document_stats.tables, tags=tags)
        if document_stats.key_value_items:
            self.key_value_items_counter.inc(document_stats.key_value_items, tags=tags)
        if document_stats.form_items:
            self.form_items_counter.inc(document_stats.form_items, tags=tags)
        if document_stats.texts:
            self.texts_counter.inc(document_stats.texts, tags=tags)
        if document_stats.groups:
            self.groups_counter.inc(document_stats.groups, tags=tags)

    def _emit_status_counter(self, conv_status: str, tags: dict) -> None:
        if conv_status == "success":
            self.success_counter.inc(tags=tags)
        elif conv_status == "partial_success":
            self.partial_counter.inc(tags=tags)
        else:
            self.failed_counter.inc(tags=tags)

    def _tags(self, tenant_id: str) -> dict:
        replica_tag = serve.get_replica_context().replica_tag or "unknown"
        return {"tenant_id": tenant_id, "replica_tag": replica_tag}

    def emit_metrics(self, metrics: list[ConversionMetrics], tenant_id: str) -> None:
        tags = self._tags(tenant_id)
        _log.info(
            "Emitting metrics, total number of records %s, replica tag: %s",
            len(metrics),
            tags["replica_tag"],
        )
        self.metric_emission_counter.inc(tags=tags)

        for record in metrics:
            self._emit_timing_stats(record.timings_stats, tags)
            self._emit_document_stats(record.document_stats, tags)
            self._emit_status_counter(record.status, tags)

    def emit_failure_metrics(self, tenant_id: str) -> None:
        """Record a failed conversion attempt when no ExportableDocument was
        produced (e.g. ConverterFailureResult), so failures aren't silently
        dropped from dcls_metrics_emitted / dcls_conversion_failed."""
        tags = self._tags(tenant_id)
        self.metric_emission_counter.inc(tags=tags)
        self.failed_counter.inc(tags=tags)
