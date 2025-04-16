# Define the input options for the API
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from docling.datamodel.base_models import InputFormat, OutputFormat
from docling.datamodel.pipeline_options import (
    EasyOcrOptions,
    PdfBackend,
    TableFormerMode,
)
from docling.models.factories import get_ocr_factory
from docling_core.types.doc import ImageRefMode

ocr_engines_enum = get_ocr_factory().get_enum()


class ConvertDocumentsOptions(BaseModel):
    from_formats: Annotated[
        list[str],
        Field(
            description=(
                "Input format(s) to convert from. String or list of strings. "
                f"Allowed values: {', '.join([v.value for v in InputFormat])}. "
                "Optional, defaults to all formats."
            ),
            examples=[[v.value for v in InputFormat]],
        ),
    ] = [v.value for v in InputFormat]

    @field_validator("from_formats", mode="before")
    def check_from_formats(cls, v, info: ValidationInfo):
        if isinstance(v, str):
            v = [v]
        allowed_from_formats = [x.value for x in InputFormat]
        if not set(v) == set(allowed_from_formats):
            for i in v:
                if i not in allowed_from_formats:
                    raise ValueError(
                        f"{i} is not allowed. Allowed from formats: {', '.join(allowed_from_formats)}"
                    )
        return v

    to_formats: Annotated[
        list[str],
        Field(
            description=(
                "Output format(s) to convert to. String or list of strings. "
                f"Allowed values: {', '.join([v.value for v in OutputFormat])}. "
                "Optional, defaults to Markdown."
            ),
            examples=[[OutputFormat.MARKDOWN.value]],
        ),
    ] = [OutputFormat.MARKDOWN.value]

    @field_validator("to_formats", mode="before")
    def check_to_formats(cls, v, info: ValidationInfo):
        if isinstance(v, str):
            v = [v]
        allowed_to_formats = [x.value for x in OutputFormat]
        if not set(v) == set(allowed_to_formats):
            for i in v:
                if i not in allowed_to_formats:
                    raise ValueError(
                        f"{i} is not allowed. Allowed to formats: {', '.join(allowed_to_formats)}"
                    )
        return v

    image_export_mode: Annotated[
        ImageRefMode,
        Field(
            description=(
                "Image export mode for the document (in case of JSON,"
                " Markdown or HTML). "
                f"Allowed values: {', '.join([v.value for v in ImageRefMode])}. "
                "Optional, defaults to Embedded."
            ),
            examples=[ImageRefMode.EMBEDDED.value],
            # pattern="embedded|placeholder|referenced",
        ),
    ] = ImageRefMode.EMBEDDED

    do_ocr: Annotated[
        bool,
        Field(
            description=(
                "If enabled, the bitmap content will be processed using OCR. "
                "Boolean. Optional, defaults to true"
            ),
            # examples=[True],
        ),
    ] = True

    force_ocr: Annotated[
        bool,
        Field(
            description=(
                "If enabled, replace existing text with OCR-generated "
                "text over content. Boolean. Optional, defaults to false."
            ),
            # examples=[False],
        ),
    ] = False

    ocr_engine: Annotated[
        str,
        Field(
            description=(
                "The OCR engine to use. String. "
                f"Allowed values: {', '.join([v.value for v in ocr_engines_enum])}. "
                "Optional, defaults to easyocr."
            ),
            examples=[EasyOcrOptions.kind],
        ),
    ] = EasyOcrOptions.kind

    @field_validator("ocr_engine")
    def check_ocr_engine(cls, v, info: ValidationInfo):
        allowed_engines = [v.value for v in ocr_engines_enum]  # type: ignore
        if v not in allowed_engines:
            raise ValueError(
                f"{v} is not allowed. Allowed engines: {', '.join(allowed_engines)}"
            )
        return v

    ocr_lang: Annotated[
        Optional[list[str]],
        Field(
            description=(
                "List of languages used by the OCR engine. "
                "Note that each OCR engine has "
                "different values for the language names. String or list of strings. "
                "Optional, defaults to empty."
            ),
            examples=[["fr", "de", "es", "en"]],
        ),
    ] = None

    pdf_backend: Annotated[
        str,
        Field(
            description=(
                "The PDF backend to use. String. "
                f"Allowed values: {', '.join([v.value for v in PdfBackend])}. "
                f"Optional, defaults to {PdfBackend.DLPARSE_V4.value}."
            ),
            examples=[PdfBackend.DLPARSE_V4.value],
        ),
    ] = PdfBackend.DLPARSE_V4.value

    @field_validator("pdf_backend")
    def check_pdf_backend(cls, v, info: ValidationInfo):
        allowed_backend = [v.value for v in PdfBackend]
        if v not in allowed_backend:
            raise ValueError(
                f"{v} is not allowed. Allowed pdf_backend: {', '.join(allowed_backend)}"
            )
        return v

    table_mode: Annotated[  # type: ignore
        Literal[TableFormerMode.FAST.value, TableFormerMode.ACCURATE.value],
        Field(
            TableFormerMode.FAST.value,
            description=(
                "Mode to use for table structure, String. "
                f"Allowed values: {', '.join([v.value for v in TableFormerMode])}. "
                f"Optional, defaults to {TableFormerMode.FAST.value}."
            ),
            examples=[TableFormerMode.FAST.value],
            # pattern="fast|accurate",
        ),
    ] = TableFormerMode.FAST.value

    abort_on_error: Annotated[
        bool,
        Field(
            description=(
                "Abort on error if enabled. Boolean. Optional, defaults to false."
            ),
            # examples=[False],
        ),
    ] = False

    return_as_file: Annotated[
        bool,
        Field(
            description=(
                "Return the output as a zip file "
                "(will happen anyway if multiple files are generated). "
                "Boolean. Optional, defaults to false."
            ),
            examples=[False],
        ),
    ] = False

    do_table_structure: Annotated[
        bool,
        Field(
            description=(
                "If enabled, the table structure will be extracted. "
                "Boolean. Optional, defaults to true."
            ),
            examples=[True],
        ),
    ] = True

    include_images: Annotated[
        bool,
        Field(
            description=(
                "If enabled, images will be extracted from the document. "
                "Boolean. Optional, defaults to true."
            ),
            examples=[True],
        ),
    ] = True

    images_scale: Annotated[
        float,
        Field(
            description="Scale factor for images. Float. Optional, defaults to 2.0.",
            examples=[2.0],
        ),
    ] = 2.0

    do_code_enrichment: Annotated[
        bool,
        Field(
            description=(
                "If enabled, perform OCR code enrichment. "
                "Boolean. Optional, defaults to false."
            ),
            examples=[False],
        ),
    ] = False

    do_formula_enrichment: Annotated[
        bool,
        Field(
            description=(
                "If enabled, perform formula OCR, return Latex code. "
                "Boolean. Optional, defaults to false."
            ),
            examples=[False],
        ),
    ] = False

    do_picture_classification: Annotated[
        bool,
        Field(
            description=(
                "If enabled, classify pictures in documents. "
                "Boolean. Optional, defaults to false."
            ),
            examples=[False],
        ),
    ] = False

    do_picture_description: Annotated[
        bool,
        Field(
            description=(
                "If enabled, describe pictures in documents. "
                "Boolean. Optional, defaults to false."
            ),
            examples=[False],
        ),
    ] = False

    generate_picture_images: Annotated[
        bool,
        Field(
            description=(
                "If enabled, generate images from the pictures and figures in documents. "
                "Optional, defaults to false"
            ),
            examples=[False],
        ),
    ] = False
