import base64
import sys
import zipfile
from pathlib import Path

import pytest

from docling.backend.docling_parse_backend import (
    ThreadedDoclingParseDocumentBackend,
)
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel import vlm_model_specs
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfBackend,
    PdfPipelineOptions,
    ProcessingPipeline,
    VlmPipelineOptions,
)
from docling.document_converter import (
    EpubFormatOption,
    ExcelFormatOption,
    HTMLFormatOption,
    OdpFormatOption,
    OdsFormatOption,
    OdtFormatOption,
    PowerpointFormatOption,
    WordFormatOption,
)
from docling.pipeline.vlm_pipeline import VlmPipeline
from docling_core.types.doc import ImageRefMode, PictureClassificationLabel

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
    _hash_pdf_format_option,
    _to_list_of_strings,
)
from docling_jobkit.datamodel.convert import (
    ConvertDocumentsOptions,
    PictureDescriptionApi,
    PictureDescriptionLocal,
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

    # Image generation is controlled by include_images / include_page_images,
    # independently of image_export_mode (which only controls serialization).
    # Defaults: include_images=True, include_page_images=False.
    opts = ConvertDocumentsOptions(
        image_export_mode=ImageRefMode.EMBEDDED,
    )
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    assert pipeline_opts.pipeline_options is not None
    assert isinstance(pipeline_opts.pipeline_options, PdfPipelineOptions)
    assert pipeline_opts.backend == ThreadedDoclingParseDocumentBackend
    assert pipeline_opts.pipeline_options.generate_picture_images is True
    assert pipeline_opts.pipeline_options.generate_page_images is False

    # Picture images are generated regardless of image_export_mode...
    opts = ConvertDocumentsOptions(
        pdf_backend=PdfBackend.PYPDFIUM2,
        image_export_mode=ImageRefMode.REFERENCED,
    )
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    assert pipeline_opts.pipeline_options is not None
    assert pipeline_opts.backend == PyPdfiumDocumentBackend
    assert isinstance(pipeline_opts.pipeline_options, PdfPipelineOptions)
    assert pipeline_opts.pipeline_options.generate_page_images is False
    assert pipeline_opts.pipeline_options.generate_picture_images is True

    # ...and the include_* flags drive generation explicitly.
    opts = ConvertDocumentsOptions(
        include_images=False,
        include_page_images=True,
    )
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    assert pipeline_opts.pipeline_options is not None
    assert pipeline_opts.pipeline_options.generate_picture_images is False
    assert pipeline_opts.pipeline_options.generate_page_images is True

    for pdf_backend in (
        PdfBackend.DLPARSE_V4,
        PdfBackend.DLPARSE_V2,
        PdfBackend.DLPARSE_V1,
        PdfBackend.DOCLING_PARSE,
    ):
        opts = ConvertDocumentsOptions(pdf_backend=pdf_backend)
        pipeline_opts = m.get_pdf_pipeline_opts(opts)
        assert pipeline_opts.pipeline_options is not None
        assert pipeline_opts.backend == ThreadedDoclingParseDocumentBackend

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


def test_backend_mapping_standard_and_vlm():
    """Request pdf_backend must map to the right class and be set explicitly
    for both Standard and VLM pipelines (no reliance on PdfFormatOption's default)."""
    m = DoclingConverterManager(config=DoclingConverterManagerConfig())

    cases = [
        (None, ThreadedDoclingParseDocumentBackend),  # omitted -> service-model default
        (
            PdfBackend.DOCLING_PARSE,
            ThreadedDoclingParseDocumentBackend,
        ),  # deprecated, normalizes to THREADED
        (PdfBackend.THREADED_DOCLING_PARSE, ThreadedDoclingParseDocumentBackend),
        (PdfBackend.PYPDFIUM2, PyPdfiumDocumentBackend),
    ]

    for pipeline in (ProcessingPipeline.STANDARD, ProcessingPipeline.VLM):
        for requested, expected in cases:
            kwargs = {"pipeline": pipeline}
            if requested is not None:
                kwargs["pdf_backend"] = requested
            opts = ConvertDocumentsOptions(**kwargs)
            pipeline_opts = m.get_pdf_pipeline_opts(opts)
            assert pipeline_opts.backend == expected, (pipeline, requested)


def test_threaded_request_reaches_conversion():
    """A threaded-backend request must run a real conversion end to end,
    not fail in option mapping (regression guard for the J1 compatibility work)."""
    from docling.datamodel.base_models import ConversionStatus

    m = DoclingConverterManager(config=DoclingConverterManagerConfig())
    doc = Path(__file__).parent / "2206.01062v1-pg4.pdf"
    opts = ConvertDocumentsOptions(pdf_backend=PdfBackend.THREADED_DOCLING_PARSE)

    results = list(m.convert_documents([doc], opts))

    assert len(results) == 1
    assert results[0].status == ConversionStatus.SUCCESS
    assert results[0].document is not None


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


def test_legacy_picture_description_filters_reach_pipeline_options():
    m = DoclingConverterManager(config=DoclingConverterManagerConfig())

    with pytest.warns(DeprecationWarning):
        opts = ConvertDocumentsOptions(
            do_picture_description=True,
            picture_description_local=PictureDescriptionLocal(
                repo_id="HuggingFaceTB/SmolVLM-256M-Instruct",
                classification_allow=[PictureClassificationLabel.BAR_CHART],
                classification_deny=[PictureClassificationLabel.LOGO],
                classification_min_confidence=0.55,
            ),
        )

    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    picture_opts = pipeline_opts.pipeline_options.picture_description_options

    assert picture_opts.classification_allow == [PictureClassificationLabel.BAR_CHART]
    assert picture_opts.classification_deny == [PictureClassificationLabel.LOGO]
    assert picture_opts.classification_min_confidence == 0.55


def test_legacy_picture_description_api_filters_reach_pipeline_options():
    m = DoclingConverterManager(config=DoclingConverterManagerConfig())

    with pytest.warns(DeprecationWarning):
        opts = ConvertDocumentsOptions(
            do_picture_description=True,
            picture_description_api={
                "url": "http://localhost",
                "classification_allow": ["map"],
                "classification_min_confidence": 0.8,
            },
        )

    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    picture_opts = pipeline_opts.pipeline_options.picture_description_options

    assert [label.value for label in picture_opts.classification_allow] == ["map"]
    assert picture_opts.classification_min_confidence == 0.8


def test_image_pipeline_uses_vlm_pipeline_when_requested():
    m = DoclingConverterManager(config=DoclingConverterManagerConfig())
    opts = ConvertDocumentsOptions(pipeline=ProcessingPipeline.VLM)
    pipeline_opts = m.get_pdf_pipeline_opts(opts)
    converter = m.get_converter(pipeline_opts)
    img_opt = converter.format_to_options[InputFormat.IMAGE]
    assert img_opt.pipeline_cls == VlmPipeline
    assert isinstance(img_opt.pipeline_options, VlmPipelineOptions)


def test_picture_description_options_propagate_to_office_formats():
    """do_picture_description (and the rest of the
    picture description / classification / chart extraction options) must
    reach the SimplePipeline-based office formats too, not just PDF/IMAGE.
    """
    m = DoclingConverterManager(config=DoclingConverterManagerConfig())
    opts = ConvertDocumentsOptions(
        do_picture_description=True,
        picture_description_api={
            "url": "http://localhost:8000/v1/chat/completions",
            "params": {"model": "test-model"},
        },
    )

    with pytest.warns(DeprecationWarning):
        pipeline_opts = m.get_pdf_pipeline_opts(opts)

    converter = m.get_converter(pipeline_opts)
    pdf_picture_opts = pipeline_opts.pipeline_options.picture_description_options

    office_format_to_option_cls = {
        InputFormat.DOCX: WordFormatOption,
        InputFormat.PPTX: PowerpointFormatOption,
        InputFormat.XLSX: ExcelFormatOption,
        InputFormat.HTML: HTMLFormatOption,
        InputFormat.ODT: OdtFormatOption,
        InputFormat.ODS: OdsFormatOption,
        InputFormat.ODP: OdpFormatOption,
        InputFormat.EPUB: EpubFormatOption,
    }
    for input_format, option_cls in office_format_to_option_cls.items():
        format_option = converter.format_to_options[input_format]
        assert isinstance(format_option, option_cls)
        assert format_option.pipeline_options.do_picture_description is True
        assert (
            format_option.pipeline_options.picture_description_options
            == pdf_picture_opts
        )


def _write_epub(path: Path, img_src: str) -> Path:
    png = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk"
        "+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "mimetype", "application/epub+zip", compress_type=zipfile.ZIP_STORED
        )
        archive.writestr(
            "META-INF/container.xml",
            '<?xml version="1.0"?>'
            '<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">'
            '<rootfiles><rootfile full-path="OEBPS/content.opf" '
            'media-type="application/oebps-package+xml"/></rootfiles></container>',
        )
        archive.writestr(
            "OEBPS/content.opf",
            '<?xml version="1.0"?>'
            '<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="id">'
            '<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">'
            '<dc:identifier id="id">sample</dc:identifier><dc:title>Sample</dc:title>'
            "<dc:language>en</dc:language></metadata><manifest>"
            '<item id="c1" href="text/ch1.xhtml" media-type="application/xhtml+xml"/>'
            '<item id="dot" href="images/dot.png" media-type="image/png"/>'
            '</manifest><spine><itemref idref="c1"/></spine></package>',
        )
        archive.writestr(
            "OEBPS/text/ch1.xhtml",
            '<?xml version="1.0" encoding="utf-8"?>'
            '<html xmlns="http://www.w3.org/1999/xhtml"><head><title>One</title></head>'
            "<body><h1>One</h1><p>Text.</p>"
            f"<img src={img_src} alt='dot'/>"
            "</body></html>",
        )
        archive.writestr("OEBPS/images/dot.png", png)
    return path


@pytest.mark.parametrize("img_src", ['"../images/dot.png"', "'../images/dot.png'"])
@pytest.mark.parametrize("include_images", [True, False])
def test_epub_pictures_follow_include_images(tmp_path, include_images, img_src):
    """An EPUB picture carries its image exactly when include_images is set."""
    m = DoclingConverterManager(config=DoclingConverterManagerConfig())
    opts = ConvertDocumentsOptions(include_images=include_images)
    converter = m.get_converter(m.get_pdf_pipeline_opts(opts))

    doc = converter.convert(_write_epub(tmp_path / "sample.epub", img_src)).document

    assert len(doc.pictures) == 1
    assert (doc.pictures[0].image is not None) is include_images
    epub_option = converter.format_to_options[InputFormat.EPUB]
    assert epub_option.backend_options.enable_remote_fetch is False


def test_epub_picture_outside_the_archive_keeps_the_document(tmp_path):
    """An image path escaping the extracted archive leaves an empty picture."""
    m = DoclingConverterManager(config=DoclingConverterManagerConfig())
    opts = ConvertDocumentsOptions(include_images=True)
    converter = m.get_converter(m.get_pdf_pipeline_opts(opts))
    epub = _write_epub(tmp_path / "sample.epub", '"../../../../outside.png"')

    with pytest.warns(UserWarning, match="Could not process an image"):
        doc = converter.convert(epub).document

    assert "Text." in doc.export_to_markdown()
    assert len(doc.pictures) == 1
    assert doc.pictures[0].image is None
