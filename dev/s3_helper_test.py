import os
from pathlib import Path

from pydantic import Field, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict, SettingsError
from typing_extensions import Self

from docling.backend.docling_parse_backend import DoclingParseDocumentBackend
from docling.backend.docling_parse_v2_backend import DoclingParseV2DocumentBackend
from docling.backend.docling_parse_v4_backend import DoclingParseV4DocumentBackend
from docling.backend.pdf_backend import PdfDocumentBackend
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel.pipeline_options import (
    PdfBackend,
    PdfPipelineOptions,
    TableFormerMode,
)
from docling.models.factories import get_ocr_factory
from docling.utils.model_downloader import download_models

from docling_jobkit.connectors.s3_helper import (
    DoclingConvert,
    check_target_has_source_converted,
    get_s3_connection,
    get_source_files,
)
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.s3_coords import S3Coordinates


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file="./dev/.env")

    source_access_key: str = Field(validation_alias="S3_SOURCE_ACCESS_KEY")
    source_secret_key: str = Field(validation_alias="S3_SOURCE_SECRET_KEY")
    source_endpoint: str = Field(validation_alias="S3_SOURCE_ENDPOINTS")
    source_bucket: str = Field(validation_alias="S3_SOURCE_BUCKET")
    source_prefix: str = Field(validation_alias="S3_SOURCE_PREFIX")
    source_ssl: bool = Field(validation_alias="S3_SOURCE_SSL")
    target_access_key: str = Field(validation_alias="S3_TARGET_ACCESS_KEY")
    target_secret_key: str = Field(validation_alias="S3_TARGET_SECRET_KEY")
    target_endpoint: str = Field(validation_alias="S3_TARGET_ENDPOINTS")
    target_bucket: str = Field(validation_alias="S3_TARGET_BUCKET")
    target_prefix: str = Field(validation_alias="S3_TARGET_PREFIX")
    target_ssl: bool = Field(validation_alias="S3_TARGET_SSL")
    omp_num_threads: int = Field(validation_alias="OMP_NUM_THREADS")
    batch_size: int = Field(validation_alias="BATCH_SIZE")

    do_ocr: bool = Field(True, validation_alias="SETTINGS_DO_OCR")
    ocr_kind: str = Field("easyocr", validation_alias="SETTINGS_OCR_KIND")
    do_table_structure: bool = Field(
        True, validation_alias="SETTINGS_DO_TABLE_STRUCTURE"
    )
    table_structure_mode: str = Field(
        "fast", validation_alias="SETTINGS_TABLE_STRUCTURE_MODE"
    )
    generate_page_images: bool = Field(
        True, validation_alias="SETTINGS_GENERATE_PAGE_IMAGES"
    )
    from_formats: list[str] = Field(["pdf"], validation_alias="SETTINGS_FROM_FORMATS")
    to_formats: list[str] = Field(["json"], validation_alias="SETTINGS_TO_FORMATS")
    do_code_enrichment: bool = Field(
        False, validation_alias="SETTINGS_DO_CODE_ENRICHMENT"
    )
    do_formula_enrichment: bool = Field(
        False, validation_alias="SETTINGS_DO_FORMULA_ENRICHMENT"
    )
    do_picture_classification: bool = Field(
        False, validation_alias="SETTINGS_DO_PICTURE_CLASSIFICATION"
    )
    do_picture_description: bool = Field(
        False, validation_alias="SETTINGS_DO_PICTURE_DESCRIPTION"
    )
    generate_picture_images: bool = Field(
        False, validation_alias="SETTINGS_PICTURE_PAGE_IMAGES"
    )
    pdf_backend: type[PdfDocumentBackend] = Field(
        DoclingParseV4DocumentBackend, validation_alias="SETTINGS_PDF_BACKEND"
    )

    @field_validator("batch_size")
    def check_batch_size(cls, v, info: ValidationInfo):
        if v <= 0:
            raise SettingsError("batch_size have to be higher than zero")
        else:
            return v

    @field_validator("pdf_backend", mode="before")
    def check_pdf_backend(cls, v, info: ValidationInfo):
        if isinstance(v, str):
            if v == PdfBackend.DLPARSE_V1:
                return DoclingParseDocumentBackend
            elif v == PdfBackend.DLPARSE_V2:
                return DoclingParseV2DocumentBackend
            elif v == PdfBackend.DLPARSE_V4:
                return DoclingParseV4DocumentBackend
            elif v == PdfBackend.PYPDFIUM2:
                return PyPdfiumDocumentBackend
            else:
                raise SettingsError(f"Unexpected PDF backend type {v}")
        else:
            return v

    @model_validator(mode="after")
    def check_source_target_is_not_same(self) -> Self:
        if (
            (self.source_endpoint == self.target_endpoint)
            and (self.source_bucket == self.target_bucket)
            and (self.source_prefix == self.target_prefix)
        ):
            raise SettingsError("s3 source and target are the same")
        else:
            return self


settings = Settings()

# We already checked envs but in deployment the inputs arrive as json

s3_source: dict = {
    "endpoint": settings.source_endpoint,
    "verify_ssl": settings.source_ssl,
    "access_key": settings.source_access_key,
    "secret_key": settings.source_secret_key,
    "bucket": settings.source_bucket,
    "key_prefix": settings.source_prefix,
}

s3_target: dict = {
    "endpoint": settings.target_endpoint,
    "verify_ssl": settings.target_ssl,
    "access_key": settings.target_access_key,
    "secret_key": settings.target_secret_key,
    "bucket": settings.target_bucket,
    "key_prefix": settings.target_prefix,
}

# validate s3 inputs
s3_coords_source = S3Coordinates.model_validate(s3_source)
s3_target_coords = S3Coordinates.model_validate(s3_target)

# Imitate conversion options json
# Load conversion settings
input_convertion_options: dict = {
    "from_formats": settings.from_formats,
    "to_formats": settings.to_formats,
    "image_export_mode": "placeholder",
    "do_ocr": settings.do_ocr,
    "force_ocr": False,
    "ocr_engine": settings.ocr_kind,
    "ocr_lang": ["en"],
    "pdf_backend": "dlparse_v4",
    "table_mode": settings.table_structure_mode,
    "abort_on_error": False,
    "return_as_file": False,
    "do_table_structure": settings.do_table_structure,
    "include_images": settings.generate_page_images,
    "images_scale": 2,
    "do_code_enrichment": settings.do_code_enrichment,
    "do_formula_enrichment": settings.do_formula_enrichment,
    "do_picture_classification": settings.do_picture_classification,
    "do_picture_description": settings.do_picture_description,
    "generate_picture_images": settings.generate_picture_images,
}

# validate inputs
convert_options = ConvertDocumentsOptions.model_validate(input_convertion_options)


s3_source_client, s3_source_resource = get_s3_connection(s3_coords_source)
source_objects_list = get_source_files(
    s3_source_client, s3_source_resource, s3_coords_source
)
filtered_source_keys = check_target_has_source_converted(
    s3_target_coords, source_objects_list, s3_coords_source.key_prefix
)


os.environ["EASYOCR_MODULE_PATH"] = "./models_cache/EasyOcr"
models_path = download_models(output_dir=Path("./models_cache"))
pipeline_options = PdfPipelineOptions()
pipeline_options.do_ocr = convert_options.do_ocr
ocr_factory = get_ocr_factory()
pipeline_options.ocr_options = ocr_factory.create_options(
    kind=convert_options.ocr_engine
)
pipeline_options.do_table_structure = convert_options.do_table_structure
pipeline_options.table_structure_options.mode = TableFormerMode(
    convert_options.table_mode
)
pipeline_options.generate_page_images = convert_options.include_images
pipeline_options.do_code_enrichment = convert_options.do_code_enrichment
pipeline_options.do_formula_enrichment = convert_options.do_formula_enrichment
pipeline_options.do_picture_classification = convert_options.do_picture_classification
pipeline_options.do_picture_description = convert_options.do_picture_description
pipeline_options.generate_picture_images = convert_options.generate_picture_images
pipeline_options.artifacts_path = models_path

converter = DoclingConvert(
    source_s3_coords=s3_coords_source,
    target_s3_coords=s3_target_coords,
    pipeline_options=pipeline_options,
    allowed_formats=convert_options.from_formats,
    to_formats=convert_options.to_formats,
    backend=settings.pdf_backend,
)

print(filtered_source_keys)

results = []
for item in converter.convert_documents(filtered_source_keys):
    results.append(item)
    print(f"Convertion result: {item}")
