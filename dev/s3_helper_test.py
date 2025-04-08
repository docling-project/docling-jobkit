import os
from pathlib import Path

from dotenv import load_dotenv

from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
from docling.models.factories import get_ocr_factory
from docling.utils.model_downloader import download_models

from docling_jobkit.connectors.s3_helper import (
    DoclingConvert,
    # S3Coordinates,
    check_target_has_source_converted,
    generate_presigns_url,
    get_s3_connection,
    get_source_files,
)
from docling_jobkit.model.convert import ConvertDocumentsOptions
from docling_jobkit.model.s3_inputs import S3Coordinates


load_dotenv("./dev/.env")

# Load credentials
s3_source_access_key = os.environ["S3_SOURCE_ACCESS_KEY"]
s3_source_secret_key = os.environ["S3_SOURCE_SECRET_KEY"]
s3_source_endpoint = os.environ["S3_SOURCE_ENDPOINTS"]
s3_source_bucket = os.environ["S3_SOURCE_BUCKET"]
s3_source_prefix = os.environ["S3_SOURCE_PREFIX"]
s3_source_ssl = os.environ.get("S3_SOURCE_SSL", True)
s3_target_access_key = os.environ["S3_TARGET_ACCESS_KEY"]
s3_target_secret_key = os.environ["S3_TARGET_SECRET_KEY"]
s3_target_endpoint = os.environ["S3_TARGET_ENDPOINTS"]
s3_target_bucket = os.environ["S3_TARGET_BUCKET"]
s3_target_prefix = os.environ["S3_TARGET_PREFIX"]
s3_target_ssl = os.environ.get("S3_TARGET_SSL", True)
batch_size = int(os.environ["BATCH_SIZE"])
max_concurrency = int(os.environ["OMP_NUM_THREADS"])

# Load conversion settings
input_convertion_options: dict = {
    "from_formats": ["docx", "pptx", "html", "image", "pdf", "asciidoc", "md", "xlsx", "xml_uspto", "xml_jats", "json_docling"],
    "to_formats": ["md", "json", "html", "text", "doctags"],
    "image_export_mode": "placeholder",
    "do_ocr": os.environ.get("SETTINGS_DO_OCR", True),
    "force_ocr": False,
    "ocr_engine": os.environ.get("SETTINGS_OCR_KIND", "easyocr"),
    "ocr_lang": ["en"],
    "pdf_backend": os.environ.get("SETTINGS_PDF_BACKEND", "dlparse_v4"),
    "table_mode": os.environ.get("SETTINGS_TABLE_STRUCTURE_MODE", "fast"),
    "abort_on_error": False,
    "return_as_file": False,
    "do_table_structure": os.environ.get("SETTINGS_DO_TABLE_STRUCTURE", True),
    "include_images": os.environ.get("SETTINGS_GENERATE_PAGE_IMAGES", False),
    "images_scale": 2,
    "do_code_enrichment": os.environ.get("SETTINGS_DO_CODE_ENRICHMENT", False),
    "do_formula_enrichment": os.environ.get("SETTINGS_DO_FORMULA_ENRICHMENT", False),
    "do_picture_classification": os.environ.get("SETTINGS_DO_PICTURE_CLASSIFICATION", False),
    "do_picture_description": os.environ.get("SETTINGS_DO_PICTURE_DESCRIPTION", False),
    "generate_picture_images": os.environ.get("SETTINGS_PICTURE_PAGE_IMAGES", False),
}

# validate inputs
convert_options = ConvertDocumentsOptions.model_validate(input_convertion_options)


s3_source: dict = {
    "endpoint": s3_source_endpoint,
    "verify_ssl": s3_source_ssl,
    "access_key": s3_source_access_key,
    "secret_key": s3_source_secret_key,
    "bucket": s3_source_bucket,
    "key_prefix": s3_source_prefix
}

s3_target: dict = {
    "endpoint": s3_target_endpoint,
    "verify_ssl": s3_target_ssl,
    "access_key": s3_target_access_key,
    "secret_key": s3_target_secret_key,
    "bucket": s3_target_bucket,
    "key_prefix": s3_target_prefix
}

# validate inputs
s3_coords_source = S3Coordinates.model_validate(s3_source)
s3_target_coords = S3Coordinates.model_validate(s3_target)


s3_source_client, s3_source_resource = get_s3_connection(s3_coords_source)
source_objects_list = get_source_files(
    s3_source_client, s3_source_resource, s3_coords_source
)
filtered_source_keys = check_target_has_source_converted(
    s3_target_coords, source_objects_list, s3_coords_source.key_prefix
)
presigned_urls = generate_presigns_url(
    s3_source_client, filtered_source_keys, s3_coords_source.bucket, batch_size=5
)


os.environ["EASYOCR_MODULE_PATH"] = "./models_cache/EasyOcr"
models_path = download_models(output_dir=Path("./models_cache"))
pipeline_options = PdfPipelineOptions()
pipeline_options.do_ocr = convert_options.do_ocr
ocr_factory = get_ocr_factory()
pipeline_options.ocr_options = ocr_factory.create_options(kind=convert_options.ocr_engine)
pipeline_options.do_table_structure = convert_options.do_table_structure
pipeline_options.table_structure_options.mode = TableFormerMode(convert_options.table_mode)
pipeline_options.generate_page_images = convert_options.include_images
pipeline_options.do_code_enrichment = convert_options.do_code_enrichment
pipeline_options.do_formula_enrichment = convert_options.do_formula_enrichment
pipeline_options.do_picture_classification = convert_options.do_picture_classification
pipeline_options.do_picture_description = convert_options.do_picture_description
pipeline_options.generate_picture_images = convert_options.generate_picture_images

pipeline_options.artifacts_path = models_path

converter = DoclingConvert(s3_target_coords, pipeline_options)

print(presigned_urls)

results = []
for item in converter.convert_documents(presigned_urls=presigned_urls[0]):
    results.append(item)
    print(f"Convertion result: {item}")
