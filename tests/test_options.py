import sys

import pytest

from docling.backend.docling_parse_backend import DoclingParseDocumentBackend
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel import vlm_model_specs
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfBackend,
    PdfPipelineOptions,
    ProcessingPipeline,
    VlmPipelineOptions,
)
from docling.pipeline.vlm_pipeline import VlmPipeline
from docling_core.types.doc import ImageRefMode

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
    _hash_pdf_format_option,
    _to_list_of_strings,
)
from docling_jobkit.datamodel.convert import (
    ConvertDocumentsOptions,
    PictureDescriptionApi,
)


def test_to_list_of_strings():
    assert _to_list_of_strings("hello world") == ["hello world"]
    assert _to_list_of_strings("hello, world") == ["hello", "world"]
    assert _to_list_of_strings("hello;world") == ["hello", "world"]
    assert _to_list_of_strings("hello; world") == ["hello", "world"]
    assert _to_list_of_strings("hello,world") == ["hello", "world"]
    assert _to_list_of_strings(["hello", "world"]) == ["hello", "world"]
    assert _to_list_of_strings(["hello;world", "test,string"]) == [
        "hello",
        "world",
        "test",
        "string",
    ]
    assert _to_list_of_strings(["hello", 123]) == ["hello", "123"]
    with pytest.raises(ValueError):
        _to_list_of_strings(123)


def test_options_validator():
    m = DoclingConverterManager(config=DoclingConverterManagerConfig())

    opts = ConvertDocumentsOptions(
        image_export_mode=ImageRefMode.EMBEDDED,
    )
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    assert pipeline_opts.pipeline_options is not None
    assert isinstance(pipeline_opts.pipeline_options, PdfPipelineOptions)
    assert pipeline_opts.backend == DoclingParseDocumentBackend
    assert pipeline_opts.pipeline_options.generate_page_images is True

    opts = ConvertDocumentsOptions(
        pdf_backend=PdfBackend.PYPDFIUM2,
        image_export_mode=ImageRefMode.REFERENCED,
    )
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    assert pipeline_opts.pipeline_options is not None
    assert pipeline_opts.backend == PyPdfiumDocumentBackend
    assert isinstance(pipeline_opts.pipeline_options, PdfPipelineOptions)
    assert pipeline_opts.pipeline_options.generate_page_images is True
    assert pipeline_opts.pipeline_options.generate_picture_images is True

    for pdf_backend in (
        PdfBackend.DLPARSE_V4,
        PdfBackend.DLPARSE_V2,
        PdfBackend.DLPARSE_V1,
        PdfBackend.DOCLING_PARSE,
    ):
        opts = ConvertDocumentsOptions(pdf_backend=pdf_backend)
        pipeline_opts = m.get_pdf_pipeline_opts(opts)
        assert pipeline_opts.pipeline_options is not None
        assert pipeline_opts.backend == DoclingParseDocumentBackend

    opts = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
    )
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    assert pipeline_opts.pipeline_options is not None
    assert pipeline_opts.pipeline_cls == VlmPipeline
    assert isinstance(pipeline_opts.pipeline_options, VlmPipelineOptions)
    if sys.platform == "darwin":
        assert (
            pipeline_opts.pipeline_options.vlm_options
            == vlm_model_specs.GRANITEDOCLING_MLX
        )
    else:
        assert (
            pipeline_opts.pipeline_options.vlm_options
            == vlm_model_specs.GRANITEDOCLING_TRANSFORMERS
        )


def test_options_cache_key():
    """Test cache key generation with deprecated picture_description_api field.

    This test uses deprecated fields and should trigger deprecation warnings.
    """
    hashes = set()

    m = DoclingConverterManager(config=DoclingConverterManagerConfig())

    opts = ConvertDocumentsOptions()
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    hash = _hash_pdf_format_option(pipeline_opts)
    assert hash not in hashes
    hashes.add(hash)

    opts.do_picture_description = True
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    hash = _hash_pdf_format_option(pipeline_opts)
    assert hash not in hashes
    hashes.add(hash)

    opts = ConvertDocumentsOptions(pipeline=ProcessingPipeline.VLM)
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    hash = _hash_pdf_format_option(pipeline_opts)
    # pprint(pipeline_opts.pipeline_options.model_dump(serialize_as_any=True))
    assert hash not in hashes
    hashes.add(hash)

    # DEPRECATED: Using picture_description_api (should trigger warning during object creation)
    with pytest.warns(DeprecationWarning):
        opts = ConvertDocumentsOptions(
            pipeline=ProcessingPipeline.VLM,
            picture_description_api=PictureDescriptionApi(
                url="http://localhost",
                params={"model": "mymodel"},
                prompt="Hello 1",
            ),
        )
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    hash = _hash_pdf_format_option(pipeline_opts)
    # pprint(pipeline_opts.pipeline_options.model_dump(serialize_as_any=True))
    assert hash not in hashes
    hashes.add(hash)

    # DEPRECATED: Modifying picture_description_api
    with pytest.warns(DeprecationWarning):
        opts = ConvertDocumentsOptions(
            pipeline=ProcessingPipeline.VLM,
        )
        opts.picture_description_api = PictureDescriptionApi(
            url="http://localhost",
            params={"model": "your-model"},
            prompt="Hello 1",
        )
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    hash = _hash_pdf_format_option(pipeline_opts)
    # pprint(pipeline_opts.pipeline_options.model_dump(serialize_as_any=True))
    assert hash not in hashes
    hashes.add(hash)

    # Modifying existing deprecated field (no new warning needed - just modify the nested object)
    opts.picture_description_api.prompt = "World"
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    hash = _hash_pdf_format_option(pipeline_opts)
    # pprint(pipeline_opts.pipeline_options.model_dump(serialize_as_any=True))
    assert hash not in hashes
    hashes.add(hash)


def test_options_cache_key_with_presets():
    """Test cache key generation with new preset system.

    This test uses the new preset and custom_config fields.
    """
    hashes = set()

    m = DoclingConverterManager(
        config=DoclingConverterManagerConfig(
            default_picture_description_preset="smolvlm",
            # Enable custom configs for testing
            allow_custom_vlm_config=True,
            allow_custom_picture_description_config=True,
            allow_custom_code_formula_config=True,
        )
    )

    # Base configuration
    opts = ConvertDocumentsOptions()
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    hash = _hash_pdf_format_option(pipeline_opts)
    assert hash not in hashes
    hashes.add(hash)

    # VLM pipeline with default preset
    opts = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
        vlm_pipeline_preset="default",
    )
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    hash = _hash_pdf_format_option(pipeline_opts)
    assert hash not in hashes
    hashes.add(hash)

    # Picture description with preset
    # Picture description with custom config (PictureDescriptionVlmEngineOptions as dict)
    opts = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
        picture_description_custom_config={
            "model_spec": {
                "name": "Custom Picture Model 1",
                "default_repo_id": "custom-model-1",
                "prompt": "Describe this image",
                "response_format": "markdown",
            },
            "engine_options": {
                "engine_type": "api",
                "url": "http://localhost:8000",
                "params": {"model": "custom-model-1"},
            },
            "scale": 2.0,
        },
    )
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    hash = _hash_pdf_format_option(pipeline_opts)
    assert hash not in hashes
    hashes.add(hash)

    # Picture description with different custom config
    opts = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
        picture_description_custom_config={
            "model_spec": {
                "name": "Custom Picture Model 2",
                "default_repo_id": "custom-model-2",  # Different model
                "prompt": "Describe this image",
                "response_format": "markdown",
            },
            "engine_options": {
                "engine_type": "api",
                "url": "http://localhost:8000",
                "params": {"model": "custom-model-2"},
            },
            "scale": 2.0,
        },
    )
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    hash = _hash_pdf_format_option(pipeline_opts)
    assert hash not in hashes
    hashes.add(hash)

    # VLM with custom config (VlmConvertOptions as dict)
    opts = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
        vlm_pipeline_custom_config={
            "model_spec": {
                "name": "Custom Test Model",
                "default_repo_id": "test-model",
                "prompt": "Convert this page to docling.",
                "response_format": "doctags",
            },
            "engine_options": {
                "engine_type": "transformers",
                "device": None,
                "load_in_8bit": True,
            },
            "scale": 2.0,
            "batch_size": 1,
        },
    )
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    hash = _hash_pdf_format_option(pipeline_opts)
    assert hash not in hashes
    hashes.add(hash)
    opts = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
        picture_description_preset="default",
    )
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    hash = _hash_pdf_format_option(pipeline_opts)
    assert hash not in hashes
    hashes.add(hash)


def test_image_pipeline_uses_vlm_pipeline_when_requested():
    m = DoclingConverterManager(config=DoclingConverterManagerConfig())
    opts = ConvertDocumentsOptions(pipeline=ProcessingPipeline.VLM)
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    converter = m.get_converter(pipeline_opts)
    img_opt = converter.format_to_options[InputFormat.IMAGE]
    assert img_opt.pipeline_cls == VlmPipeline
    assert isinstance(img_opt.pipeline_options, VlmPipelineOptions)
