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
        config = DoclingConverterManagerConfig(allow_external_plugins=True)
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
        assert ocr_opts.model_name == "rapidocr"
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
