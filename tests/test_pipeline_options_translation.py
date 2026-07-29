"""Test ConvertDocumentsOptions → PipelineOptions translation for Triton/KServe payload."""

import pytest

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions

PAYLOAD = {
    "to_formats": ["json"],
    "image_export_mode": "placeholder",
    "do_picture_classification": False,
    "do_ocr": True,
    "force_ocr": True,
    "do_table_structure": True,
    "do_chart_extraction": False,
    "do_picture_description": False,
    "layout_custom_config": {
        "kind": "layout_object_detection",
        "model_spec": {
            "name": "layout_heron_custom",
            "repo_id": "docling-project/docling-layout-heron-onnx",
            "revision": "refs/pr/1",
        },
        "engine_options": {
            "engine_type": "api_kserve_v2",
            "transport": "grpc",
            "url": "http://triton.internal:8001",
            "model_name": "layout_heron",
        },
    },
    "table_structure_custom_config": {
        "kind": "docling_tableformer",
        "mode": "accurate",
        "do_cell_matching": True,
    },
    "ocr_custom_config": {
        "kind": "kserve_v2_ocr",
        "model_name": "ocr",
        "transport": "grpc",
        "url": "http://triton-ocr.internal:8001",
    },
}


class TestPipelineOptionsTranslation:
    @pytest.fixture
    def manager(self):
        config = DoclingConverterManagerConfig(
            allow_external_plugins=True,
            allow_custom_layout_config=True,
            allow_custom_table_structure_config=True,
            allow_custom_ocr_config=True,
        )
        return DoclingConverterManager(config)

    def test_layout_options_translated(self, manager):
        from docling.datamodel.pipeline_options import LayoutObjectDetectionOptions

        options = ConvertDocumentsOptions.model_validate(PAYLOAD)
        layout_opts = manager._parse_layout_options(options)

        assert isinstance(layout_opts, LayoutObjectDetectionOptions)
        assert layout_opts.engine_options.engine_type.value == "api_kserve_v2"
        assert str(layout_opts.engine_options.url) == "http://triton.internal:8001"
        assert layout_opts.engine_options.model_name == "layout_heron"
        assert layout_opts.engine_options.transport == "grpc"
        assert layout_opts.model_spec.name == "layout_heron_custom"
        assert layout_opts.model_spec.revision == "refs/pr/1"

    def test_table_structure_options_translated(self, manager):
        from docling.datamodel.pipeline_options import TableStructureOptions

        options = ConvertDocumentsOptions.model_validate(PAYLOAD)
        table_opts = manager._parse_table_structure_options(options)

        assert isinstance(table_opts, TableStructureOptions)
        assert table_opts.mode.value == "accurate"
        assert table_opts.do_cell_matching is True

    def test_ocr_options_translated(self, manager):
        from docling.datamodel.pipeline_options import KserveV2OcrOptions

        options = ConvertDocumentsOptions.model_validate(PAYLOAD)
        ocr_opts = manager._parse_ocr_options(options)

        assert isinstance(ocr_opts, KserveV2OcrOptions)
        assert str(ocr_opts.url) == "http://triton-ocr.internal:8001"
        assert ocr_opts.model_name == "ocr"
        assert ocr_opts.transport == "grpc"
        assert ocr_opts.force_full_page_ocr is True  # from force_ocr=True in payload

    def test_pdf_pipeline_options_flags(self, manager):
        options = ConvertDocumentsOptions.model_validate(PAYLOAD)
        pdf_format_option = manager.get_pdf_pipeline_opts(options)
        pipeline_opts = pdf_format_option.pipeline_options

        assert pipeline_opts.do_ocr is True
        assert pipeline_opts.do_table_structure is True
        assert pipeline_opts.do_picture_classification is False
        assert pipeline_opts.do_chart_extraction is False
        assert pipeline_opts.do_picture_description is False
        # placeholder mode → page images should NOT be enabled
        assert not pipeline_opts.generate_page_images


class TestHeadingHierarchyTranslation:
    @pytest.fixture
    def manager(self):
        return DoclingConverterManager(DoclingConverterManagerConfig())

    def _pipeline_opts(self, manager, enabled=None, heading_options=None):
        payload = dict(PAYLOAD)
        payload.pop("layout_custom_config")
        payload.pop("table_structure_custom_config")
        payload.pop("ocr_custom_config")
        if enabled is not None:
            payload["do_pdf_heading_hierarchy"] = enabled
        if heading_options is not None:
            payload["pdf_heading_hierarchy_options"] = heading_options
        options = ConvertDocumentsOptions.model_validate(payload)
        return manager.get_pdf_pipeline_opts(options).pipeline_options

    def test_disabled_by_default(self, manager):
        pipeline_opts = self._pipeline_opts(manager)

        assert pipeline_opts.heading_hierarchy_options.enabled is False
        # Retaining parsed pages costs memory, so it must stay off while the
        # heading-hierarchy step is not asking for the style signal.
        assert pipeline_opts.generate_parsed_pages is False

    def test_toggle_drives_nested_enabled(self, manager):
        pipeline_opts = self._pipeline_opts(manager, enabled=True)

        # Callers flip `do_pdf_heading_hierarchy`; the pipeline reads the nested
        # flag, so the translation has to carry the toggle across.
        assert pipeline_opts.heading_hierarchy_options.enabled is True

    def test_options_forwarded(self, manager):
        pipeline_opts = self._pipeline_opts(
            manager,
            enabled=True,
            heading_options={
                "use_bookmarks": False,
                "max_level": 4,
                "bookmark_match_threshold": 0.95,
                "numbering_schemes": ["part", "arabic"],
            },
        )
        heading_opts = pipeline_opts.heading_hierarchy_options

        assert heading_opts.enabled is True
        assert heading_opts.use_bookmarks is False
        assert heading_opts.use_numbering is True
        assert heading_opts.max_level == 4
        assert heading_opts.bookmark_match_threshold == 0.95
        assert heading_opts.numbering_schemes == ["part", "arabic"]

    def test_style_signal_retains_parsed_pages(self, manager):
        pipeline_opts = self._pipeline_opts(
            manager, enabled=True, heading_options={"use_style": True}
        )

        # Style inference reads the parsed PDF cells; docling drops them after
        # assembly unless generate_parsed_pages is set, silently skipping style.
        assert pipeline_opts.generate_parsed_pages is True

    def test_no_style_signal_leaves_parsed_pages_off(self, manager):
        pipeline_opts = self._pipeline_opts(
            manager, enabled=True, heading_options={"use_style": False}
        )

        assert pipeline_opts.heading_hierarchy_options.enabled is True
        assert pipeline_opts.generate_parsed_pages is False
