"""Standalone Prometheus/Ray metrics recorder for docling conversions.

Both the coordinator and converter Ray Serve deployments construct their own
instance of this class at __init__ time (when generate_metrics is enabled),
so metrics can be emitted directly from whichever actor produced them,
without depending on a remote call to a converter replica.
"""

import logging
import random
import string

from ray import serve
from ray.util.metrics import Counter, Histogram

from docling.datamodel.base_models import InputFormat

_log = logging.getLogger(__name__)

_TAG_KEYS = ("tenant_id", "replica_tag")

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


def _random_digit_string(length: int) -> str:
    return "".join(random.choices(string.digits, k=length))


class RayMetricsRecorder:
    """Owns the dcls_* Counter/Histogram objects and emits observations for
    them. Construct one instance per actor that has generate_metrics
    enabled; Ray's metrics backend aggregates same-named metrics reported by
    multiple actors, tagged by replica_tag, exactly like multiple converter
    replicas already do today."""

    def __init__(self) -> None:
        self.metric_emission_counter = Counter(
            "dcls_metrics_emitted",
            description="Number of attemps to emmit metrics",
            tag_keys=_TAG_KEYS,
        )
        self.success_counter = Counter(
            "dcls_conversion_success",
            description="Number of successeful conversions",
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
        self.doc_type_pdf_counter = Counter(
            "dcls_doc_type_pdf",
            description="Number of pdf documents",
            tag_keys=_TAG_KEYS,
        )
        self.doc_type_docx_counter = Counter(
            "dcls_doc_type_docx",
            description="Number of docx documents",
            tag_keys=_TAG_KEYS,
        )
        self.doc_type_pptx_counter = Counter(
            "dcls_doc_type_pptx",
            description="Number of pptx documents",
            tag_keys=_TAG_KEYS,
        )
        self.doc_type_html_counter = Counter(
            "dcls_doc_type_html",
            description="Number of html documents",
            tag_keys=_TAG_KEYS,
        )
        self.doc_type_image_counter = Counter(
            "dcls_doc_type_image",
            description="Number of image documents",
            tag_keys=_TAG_KEYS,
        )
        self.doc_type_md_counter = Counter(
            "dcls_doc_type_md",
            description="Number of md documents",
            tag_keys=_TAG_KEYS,
        )
        self.doc_type_xlsx_counter = Counter(
            "dcls_doc_type_xlsx",
            description="Number of xlsx documents",
            tag_keys=_TAG_KEYS,
        )
        self.doc_type_xml_counter = Counter(
            "dcls_doc_type_xml",
            description="Number of xml documents",
            tag_keys=_TAG_KEYS,
        )
        self.doc_type_doclang_counter = Counter(
            "dcls_doc_type_doclang",
            description="Number of doclang documents",
            tag_keys=_TAG_KEYS,
        )
        self.doc_type_docling_counter = Counter(
            "dcls_doc_type_docling",
            description="Number of docling type documents",
            tag_keys=_TAG_KEYS,
        )
        self.doc_type_other_counter = Counter(
            "dcls_doc_type_other",
            description="Number of other type documents",
            tag_keys=_TAG_KEYS,
        )
        self.num_pages_hist = Counter(
            "dcls_num_pages",
            description="Number of pages in converted document",
            tag_keys=_TAG_KEYS,
        )
        self.pictures_hist = Counter(
            "dcls_pictures",
            description="Number of pictures in converted document",
            tag_keys=_TAG_KEYS,
        )
        self.tables_hist = Counter(
            "dcls_tables",
            description="Number of tables in converted document",
            tag_keys=_TAG_KEYS,
        )
        self.key_value_items_hist = Counter(
            "dcls_key_value_items",
            description="Number of key value items in converted document",
            tag_keys=_TAG_KEYS,
        )
        self.form_items_hist = Counter(
            "dcls_form_items",
            description="Number of form items in converted document",
            tag_keys=_TAG_KEYS,
        )
        self.texts_hist = Counter(
            "dcls_texts",
            description="Number of text items in converted document",
            tag_keys=_TAG_KEYS,
        )
        self.groups_hist = Counter(
            "dcls_groups",
            description="Number of group items in converted document",
            tag_keys=_TAG_KEYS,
        )

    def emit_metrics(self, metrics: list, tenant_id: str) -> None:
        replica_tag = serve.get_replica_context().replica_tag
        if not replica_tag:
            replica_tag = _random_digit_string(12)
        _log.info(
            "Emitting metrics, total number of records %s, replica tag: %s",
            len(metrics),
            replica_tag,
        )
        tags = {"tenant_id": tenant_id, "replica_tag": replica_tag}
        self.metric_emission_counter.inc(tags=tags)

        def _observe_all(hist: Histogram, values: list) -> None:
            # Observe each raw sample (not a pre-aggregated min/max/median) so
            # Prometheus can compute accurate percentiles and surface a single
            # slow page instead of it being smoothed into a document average.
            for value in values:
                hist.observe(value, tags=tags)

        for item in metrics:
            if "reference" in item:
                metrics_list = item["metrics"]
            else:
                metrics_list = [item]

            for record in metrics_list:
                pipeline_stats = record["timings_stats"]
                if "pipeline_total" in pipeline_stats:
                    _observe_all(
                        self.pipeline_total_hist, pipeline_stats["pipeline_total"]
                    )
                if "page_parse" in pipeline_stats:
                    _observe_all(self.page_parse_hist, pipeline_stats["page_parse"])
                if "ocr" in pipeline_stats:
                    _observe_all(self.ocr_hist, pipeline_stats["ocr"])
                if "layout" in pipeline_stats:
                    _observe_all(self.layout_hist, pipeline_stats["layout"])
                if "table_structure" in pipeline_stats:
                    _observe_all(
                        self.table_structure_hist, pipeline_stats["table_structure"]
                    )
                if "page_assemble" in pipeline_stats:
                    _observe_all(
                        self.page_assemble_hist, pipeline_stats["page_assemble"]
                    )
                if "doc_assemble" in pipeline_stats:
                    _observe_all(self.doc_assemble_hist, pipeline_stats["doc_assemble"])
                if "reading_order" in pipeline_stats:
                    _observe_all(
                        self.reading_order_hist, pipeline_stats["reading_order"]
                    )
                if "doc_enrich" in pipeline_stats:
                    _observe_all(self.doc_enrich_hist, pipeline_stats["doc_enrich"])

                document_stats = record["document_stats"]
                if "input_format" in document_stats:
                    doc_type = document_stats["input_format"]
                    if doc_type == InputFormat.PDF:
                        self.doc_type_pdf_counter.inc(tags=tags)
                    elif doc_type == InputFormat.DOCX:
                        self.doc_type_docx_counter.inc(tags=tags)
                    elif doc_type == InputFormat.PPTX:
                        self.doc_type_pptx_counter.inc(tags=tags)
                    elif doc_type == InputFormat.HTML:
                        self.doc_type_html_counter.inc(tags=tags)
                    elif doc_type == InputFormat.IMAGE:
                        self.doc_type_image_counter.inc(tags=tags)
                    elif doc_type == InputFormat.MD:
                        self.doc_type_md_counter.inc(tags=tags)
                    elif doc_type == InputFormat.XLSX:
                        self.doc_type_xlsx_counter.inc(tags=tags)
                    elif doc_type in (
                        InputFormat.XML_USPTO,
                        InputFormat.XML_JATS,
                        InputFormat.XML_XBRL,
                    ):
                        self.doc_type_xml_counter.inc(tags=tags)
                    elif doc_type == InputFormat.XML_DOCLANG:
                        self.doc_type_doclang_counter.inc(tags=tags)
                    elif doc_type == InputFormat.JSON_DOCLING:
                        self.doc_type_docling_counter.inc(tags=tags)
                    else:
                        self.doc_type_other_counter.inc(tags=tags)
                if "num_pages" in document_stats:
                    self.num_pages_hist.inc(document_stats["num_pages"], tags=tags)
                if "pictures" in document_stats:
                    self.pictures_hist.inc(document_stats["pictures"], tags=tags)
                if "tables" in document_stats:
                    self.tables_hist.inc(document_stats["tables"], tags=tags)
                if "key_value_items" in document_stats:
                    self.key_value_items_hist.inc(
                        document_stats["key_value_items"], tags=tags
                    )
                if "form_items" in document_stats:
                    self.form_items_hist.inc(document_stats["form_items"], tags=tags)
                if "texts" in document_stats:
                    self.texts_hist.inc(document_stats["texts"], tags=tags)
                if "groups" in document_stats:
                    self.groups_hist.inc(document_stats["groups"], tags=tags)

                conv_status = record["status"]
                if conv_status == "success":
                    self.success_counter.inc(tags=tags)
                elif conv_status == "partial_success":
                    self.partial_counter.inc(tags=tags)
                else:
                    self.failed_counter.inc(tags=tags)

    def emit_failure_metrics(self, tenant_id: str) -> None:
        """Record a failed conversion attempt when no ExportableDocument was
        produced (e.g. ConverterFailureResult), so failures aren't silently
        dropped from dcls_metrics_emitted / dcls_conversion_failed."""
        replica_tag = serve.get_replica_context().replica_tag
        if not replica_tag:
            replica_tag = _random_digit_string(12)
        tags = {"tenant_id": tenant_id, "replica_tag": replica_tag}
        self.metric_emission_counter.inc(tags=tags)
        self.failed_counter.inc(tags=tags)
