"""Tests for the Ray metrics recorder and the metrics-record helpers it consumes."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from docling.datamodel.base_models import ConversionStatus, InputFormat

from docling_jobkit.orchestrators.ray.metrics_recorder import (
    RayMetricsRecorder,
    _doc_type_label,
)
from docling_jobkit.orchestrators.ray.metrics_utils import (
    ConversionMetrics,
    DocumentStats,
    collect_doc_stats,
    get_metrics_from_exportable_doc,
)


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> RayMetricsRecorder:
    monkeypatch.setattr(
        "docling_jobkit.orchestrators.ray.metrics_recorder.serve.get_replica_context",
        lambda: SimpleNamespace(replica_tag="replica-1"),
    )
    rec = RayMetricsRecorder()
    # Replace the ray.util.metrics objects with mocks: constructing them works
    # without a Ray runtime, but .inc()/.observe() require one.
    for attr in vars(rec):
        setattr(rec, attr, MagicMock())
    return rec


def _metrics(status: ConversionStatus) -> ConversionMetrics:
    return ConversionMetrics(
        document_hash="abc",
        timings_stats={"pipeline_total": [1.0, 2.0]},
        document_stats=DocumentStats(input_format=InputFormat.PDF, num_pages=3),
        status=status,
    )


class TestStatusMapping:
    """Locks down the success/partial_success/failure status -> counter mapping.

    A prior squash commit fixed a regression where the emitted status string
    was "partial" instead of ConversionStatus.PARTIAL_SUCCESS's "partial_success",
    which silently routed every partial conversion into the failure bucket.
    """

    def test_success_status_increments_success_counter(
        self, recorder: RayMetricsRecorder
    ) -> None:
        recorder.emit_metrics(
            metrics=[_metrics(ConversionStatus.SUCCESS)], tenant_id="t1"
        )
        recorder.success_counter.inc.assert_called_once()
        recorder.partial_counter.inc.assert_not_called()
        recorder.failed_counter.inc.assert_not_called()

    def test_partial_success_status_increments_partial_counter(
        self, recorder: RayMetricsRecorder
    ) -> None:
        recorder.emit_metrics(
            metrics=[_metrics(ConversionStatus.PARTIAL_SUCCESS)], tenant_id="t1"
        )
        recorder.partial_counter.inc.assert_called_once()
        recorder.success_counter.inc.assert_not_called()
        recorder.failed_counter.inc.assert_not_called()

    def test_failure_status_increments_failed_counter(
        self, recorder: RayMetricsRecorder
    ) -> None:
        recorder.emit_metrics(
            metrics=[_metrics(ConversionStatus.FAILURE)], tenant_id="t1"
        )
        recorder.failed_counter.inc.assert_called_once()
        recorder.success_counter.inc.assert_not_called()
        recorder.partial_counter.inc.assert_not_called()


class TestDocTypeLadder:
    @pytest.mark.parametrize(
        "input_format,expected_label",
        [
            (InputFormat.PDF, "pdf"),
            (InputFormat.DOCX, "docx"),
            (InputFormat.PPTX, "pptx"),
            (InputFormat.HTML, "html"),
            (InputFormat.IMAGE, "image"),
            (InputFormat.MD, "md"),
            (InputFormat.XLSX, "xlsx"),
            (InputFormat.XML_USPTO, "xml"),
            (InputFormat.XML_JATS, "xml"),
            (InputFormat.XML_XBRL, "xml"),
            (InputFormat.XML_DOCLANG, "doclang"),
            (InputFormat.JSON_DOCLING, "docling"),
            (InputFormat.ASCIIDOC, "other"),
            (None, "other"),
        ],
    )
    def test_doc_type_label(
        self, input_format: InputFormat | None, expected_label: str
    ) -> None:
        assert _doc_type_label(input_format) == expected_label

    def test_emit_document_stats_tags_doc_type_counter_with_format(
        self, recorder: RayMetricsRecorder
    ) -> None:
        recorder.emit_metrics(
            metrics=[_metrics(ConversionStatus.SUCCESS)], tenant_id="t1"
        )
        recorder.doc_type_counter.inc.assert_called_once()
        _, kwargs = recorder.doc_type_counter.inc.call_args
        assert kwargs["tags"]["format"] == "pdf"

    def test_missing_input_format_does_not_emit_doc_type_counter(
        self, recorder: RayMetricsRecorder
    ) -> None:
        metrics = ConversionMetrics(
            document_hash=None,
            timings_stats={},
            document_stats=DocumentStats(input_format=None),
            status=ConversionStatus.FAILURE,
        )
        recorder.emit_metrics(metrics=[metrics], tenant_id="t1")
        recorder.doc_type_counter.inc.assert_not_called()


class TestZeroValuedDocumentStats:
    """Ray's Counter.inc() raises ValueError for a value of 0 (it requires
    value > 0), but a real document can legitimately have 0 tables/pictures/
    etc. emit_metrics must not attempt to increment those counters by 0.
    """

    def test_zero_counts_do_not_raise_or_increment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "docling_jobkit.orchestrators.ray.metrics_recorder.serve.get_replica_context",
            lambda: SimpleNamespace(replica_tag="replica-1"),
        )
        # Use the real ray.util.metrics.Counter objects (not mocked): Counter.inc
        # raises ValueError on a value <= 0 before touching any Ray runtime state,
        # so this reproduces the production crash without needing a live cluster.
        recorder = RayMetricsRecorder()
        metrics = ConversionMetrics(
            document_hash="abc",
            timings_stats={},
            document_stats=DocumentStats(
                input_format=InputFormat.PDF,
                num_pages=1,
                pictures=0,
                tables=0,
                key_value_items=0,
                form_items=0,
                texts=0,
                groups=0,
            ),
            status=ConversionStatus.SUCCESS,
        )
        recorder.emit_metrics(metrics=[metrics], tenant_id="t1")


class TestTimingReduction:
    def test_emit_timing_stats_observes_only_present_keys(
        self, recorder: RayMetricsRecorder
    ) -> None:
        metrics = ConversionMetrics(
            document_hash="abc",
            timings_stats={"pipeline_total": [0.5], "ocr": [0.1, 0.2]},
            document_stats=DocumentStats(),
            status=ConversionStatus.SUCCESS,
        )
        recorder.emit_metrics(metrics=[metrics], tenant_id="t1")
        assert recorder.pipeline_total_hist.observe.call_count == 1
        assert recorder.ocr_hist.observe.call_count == 2
        recorder.layout_hist.observe.assert_not_called()
        recorder.page_parse_hist.observe.assert_not_called()


class TestGetMetricsFromExportableDoc:
    def test_no_document_leaves_document_stats_mostly_none(self) -> None:
        exp_doc = SimpleNamespace(
            document_type=InputFormat.PDF, document=None, timings={}, document_hash=None
        )
        stats = collect_doc_stats(exp_doc)
        assert stats.input_format == InputFormat.PDF
        assert stats.num_pages is None
        assert stats.pictures is None

    def test_returns_conversion_metrics_with_status(self) -> None:
        exp_doc = SimpleNamespace(
            document_type=InputFormat.PDF,
            document=None,
            timings={},
            document_hash="hash123",
            status=ConversionStatus.PARTIAL_SUCCESS,
        )
        metrics = get_metrics_from_exportable_doc(exp_doc)
        assert isinstance(metrics, ConversionMetrics)
        assert metrics.status == ConversionStatus.PARTIAL_SUCCESS
        assert metrics.document_hash == "hash123"
