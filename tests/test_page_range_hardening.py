import sys
from io import BytesIO
from unittest.mock import patch

from docling.datamodel.base_models import OutputFormat
from docling.datamodel.document import DocumentStream
from docling.datamodel.service.options import ConvertDocumentsOptions

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.datamodel.convert import MAX_SAFE_PAGE, normalize_page_range
from docling_jobkit.datamodel.task import Task
from docling_jobkit.orchestrators.ray.serve_deployment import _build_slice_plan


class TestNormalizePageRange:
    """Tests for normalize_page_range utility function."""

    def test_clamps_sys_maxsize(self):
        """sys.maxsize (2^63 - 1) is clamped to MAX_SAFE_PAGE (2^31 - 1)."""
        clamped = normalize_page_range((1, sys.maxsize))
        assert clamped == (1, MAX_SAFE_PAGE)

    def test_clamps_large_arbitrary_int(self):
        """Arbitrary large integers > 2^31 - 1 are clamped to MAX_SAFE_PAGE."""
        clamped = normalize_page_range((1, 2**63 - 1))
        assert clamped == (1, MAX_SAFE_PAGE)

    def test_preserves_valid_safe_ranges(self):
        """Legitimate page ranges within 32-bit limits are untouched."""
        assert normalize_page_range((1, 10)) == (1, 10)
        assert normalize_page_range((5, 100)) == (5, 100)
        assert normalize_page_range((1, MAX_SAFE_PAGE)) == (1, MAX_SAFE_PAGE)

    def test_clamps_negative_or_zero_start(self):
        """Start page < 1 is normalized to 1."""
        assert normalize_page_range((0, 10)) == (1, 10)
        assert normalize_page_range((-5, 10)) == (1, 10)

    def test_ensures_start_not_greater_than_end(self):
        """If end < start, end is raised to start."""
        assert normalize_page_range((10, 5)) == (10, 10)


class TestTaskPageRangeHardening:
    """Tests for defensive page_range normalization in Task model."""

    def test_task_normalizes_convert_options_with_sys_maxsize(self):
        """Task normalizes convert_options with sys.maxsize on creation."""
        task = Task(
            task_id="t-1",
            convert_options=ConvertDocumentsOptions(
                page_range=(1, sys.maxsize),
                to_formats=[OutputFormat.JSON],
            ),
        )
        assert task.convert_options is not None
        assert task.convert_options.page_range == (1, MAX_SAFE_PAGE)

    def test_task_model_validate_normalizes_wire_payload(self):
        """Task.model_validate normalizes a wire payload containing 64-bit page_range."""
        payload = {
            "task_id": "t-wire",
            "convert_options": {
                "page_range": [1, 9223372036854775807],
                "to_formats": ["json"],
            },
        }
        task = Task.model_validate(payload)
        assert task.convert_options is not None
        assert task.convert_options.page_range == (1, MAX_SAFE_PAGE)

    def test_task_normalizes_deprecated_options_field(self):
        """Task normalizes page_range when provided via deprecated options field."""
        task = Task(
            task_id="t-deprecated",
            options=ConvertDocumentsOptions(
                page_range=(1, sys.maxsize),
                to_formats=[OutputFormat.JSON],
            ),
        )
        assert task.convert_options is not None
        assert task.convert_options.page_range == (1, MAX_SAFE_PAGE)


class TestSlicePlanPageRangeHardening:
    """Tests for _build_slice_plan handling large page ranges."""

    def test_build_slice_plan_with_sys_maxsize(self):
        """_build_slice_plan clamps requested range with sys.maxsize."""
        plan = _build_slice_plan(
            total_pages=5,
            requested_page_range=(1, sys.maxsize),
            max_page_slice_size=2,
        )
        assert plan.effective_page_range == (1, 5)
        assert len(plan.slices) == 3
        assert plan.slices[0].page_range == (1, 2)
        assert plan.slices[1].page_range == (3, 4)
        assert plan.slices[2].page_range == (5, 5)

    def test_build_slice_plan_with_single_slice(self):
        """_build_slice_plan with total_pages <= max_page_slice_size."""
        plan = _build_slice_plan(
            total_pages=5,
            requested_page_range=(1, sys.maxsize),
            max_page_slice_size=10,
        )
        assert plan.effective_page_range == (1, 5)
        assert len(plan.slices) == 1
        assert plan.slices[0].page_range == (1, 5)


class TestConverterManagerPageRangeHardening:
    """Tests for DoclingConverterManager passing safe page ranges downstream."""

    @patch("docling.document_converter.DocumentConverter.convert_all")
    def test_convert_documents_passes_normalized_page_range(self, mock_convert_all):
        """convert_documents normalizes sys.maxsize before calling converter.convert_all."""
        mock_convert_all.return_value = iter([])
        cm = DoclingConverterManager(config=DoclingConverterManagerConfig())

        opts = ConvertDocumentsOptions(
            page_range=(1, sys.maxsize),
            to_formats=[OutputFormat.JSON],
        )

        list(
            cm.convert_documents(
                sources=[DocumentStream(name="test.pdf", stream=BytesIO(b"%PDF-1.4"))],
                options=opts,
            )
        )

        assert mock_convert_all.called
        call_kwargs = mock_convert_all.call_args[1]
        assert call_kwargs["page_range"] == (1, MAX_SAFE_PAGE)


class TestCoordinatorOptionsHardening:
    """Tests for RayServeCoordinator _with_default_document_timeout."""

    def _make_coordinator(self, document_timeout):
        from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
        from docling_jobkit.orchestrators.ray.serve_deployment import (
            DoclingProcessorCoordinatorDeployment,
        )

        coordinator_cls = getattr(
            DoclingProcessorCoordinatorDeployment, "func_or_class", None
        )
        assert coordinator_cls is not None
        coordinator = coordinator_cls.__new__(coordinator_cls)
        coordinator.config = RayOrchestratorConfig(document_timeout=document_timeout)
        return coordinator

    def test_applies_config_timeout_when_task_has_none(self):
        """config.document_timeout is applied when the task does not set one."""
        coordinator = self._make_coordinator(document_timeout=30.0)
        task = Task(
            task_id="t-timeout",
            convert_options=ConvertDocumentsOptions(to_formats=[OutputFormat.JSON]),
        )

        updated = coordinator._with_default_document_timeout(task)

        assert updated.convert_options is not None
        assert updated.convert_options.document_timeout == 30.0

    def test_does_not_override_explicit_task_timeout(self):
        """Task-level document_timeout is not overwritten by config."""
        coordinator = self._make_coordinator(document_timeout=30.0)
        task = Task(
            task_id="t-explicit",
            convert_options=ConvertDocumentsOptions(
                document_timeout=10.0,
                to_formats=[OutputFormat.JSON],
            ),
        )

        updated = coordinator._with_default_document_timeout(task)

        assert updated is task
        assert updated.convert_options.document_timeout == 10.0

    def test_returns_same_task_when_no_timeout_configured(self):
        """Task is returned unchanged when config.document_timeout is None."""
        coordinator = self._make_coordinator(document_timeout=None)
        task = Task(task_id="t-no-timeout")

        updated = coordinator._with_default_document_timeout(task)

        assert updated is task
        assert updated.convert_options is None
