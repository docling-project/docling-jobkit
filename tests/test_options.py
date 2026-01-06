import sys

import pytest

from docling.backend.docling_parse_backend import DoclingParseDocumentBackend
from docling.backend.docling_parse_v2_backend import DoclingParseV2DocumentBackend
from docling.backend.docling_parse_v4_backend import DoclingParseV4DocumentBackend
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
    assert pipeline_opts.backend == DoclingParseV4DocumentBackend
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

    opts = ConvertDocumentsOptions(pdf_backend=PdfBackend.DLPARSE_V2)
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    assert pipeline_opts.pipeline_options is not None
    assert pipeline_opts.backend == DoclingParseV2DocumentBackend

    opts = ConvertDocumentsOptions(pdf_backend=PdfBackend.DLPARSE_V1)
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

    opts.picture_description_api = PictureDescriptionApi(
        url="http://localhost",
        params={"model": "mymodel"},
        prompt="Hello 1",
    )
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    hash = _hash_pdf_format_option(pipeline_opts)
    # pprint(pipeline_opts.pipeline_options.model_dump(serialize_as_any=True))
    assert hash not in hashes
    hashes.add(hash)

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

    opts.picture_description_api.prompt = "World"
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    hash = _hash_pdf_format_option(pipeline_opts)
    # pprint(pipeline_opts.pipeline_options.model_dump(serialize_as_any=True))
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
