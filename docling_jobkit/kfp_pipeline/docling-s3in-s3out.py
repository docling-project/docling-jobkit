from kfp import dsl
from kfp import kubernetes
from pydantic import BaseModel
from typing import Dict, List, Optional



@dsl.component(
    packages_to_install=["docling==2.24.0", "git+https://github.com/docling-project/docling-jobkit@vku/s3_commons"], 
    # pip_index_urls=["https://download.pytorch.org/whl/cpu", "https://pypi.org/simple"],
    base_image="python:3.11",
    )
def convert_payload(
        options: Dict = {
            "from_formats": ["docx","pptx","html","image","pdf","asciidoc","md","xlsx","xml_uspto","xml_jats","json_docling"],
            "to_formats": ["md"],
            "image_export_mode": "placeholder",
            "do_ocr": True,
            "force_ocr": False,
            "ocr_engine": "easyocr",
            "ocr_lang": [],
            "pdf_backend": "dlparse_v2",
            "table_mode": "accurate",
            "abort_on_error": False,
            "return_as_file": False,
            "do_table_structure": True,
            "include_images": True,
            "images_scale": 2
        },
        target: Dict = {
            "s3_target_endpoint": "s3.eu-de.cloud-object-storage.appdomain.cloud",
            "s3_target_access_key": "123454321",
            "s3_target_secret_key": "secretsecret",
            "s3_target_bucket": "target-bucket",
            "s3_target_prefix": "my-docs",
            "s3_target_ssl": True
        },
        pre_signed_urls: List[str] = []
    ) -> List:

    import os
    import logging
    import json
    from typing import Dict, List
    from pathlib import Path
    from docling.backend.docling_parse_backend import DoclingParseDocumentBackend
    from docling.document_converter import DocumentConverter, PdfFormatOption
    from docling.datamodel.base_models import ConversionStatus, InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
    from docling.datamodel.document import ConversionResult
    from docling.pipeline.standard_pdf_pipeline import StandardPdfPipeline
    from docling.utils.model_downloader import download_models
    from urllib.parse import urlunsplit, urlparse
    from docling_jobkit.connectors.s3_helper import (
        S3Coordinates,
        DoclingConvert
    )


    logging.basicConfig(level=logging.INFO)

    logging.info('Type of pre_signed_urls: {}'.format(type(pre_signed_urls)))

    s3_coords = S3Coordinates(
        endpoint = target["s3_target_endpoint"],
        verify_ssl = target["s3_target_ssl"],
        access_key = target["s3_target_access_key"],
        secret_key = target["s3_target_secret_key"],
        bucket = target["s3_target_bucket"],
        key_prefix = target["s3_target_prefix"]
    )
    
    easyocr_path = Path("/models/EasyOCR")
    os.environ['MODULE_PATH'] = str(easyocr_path)
    os.environ['EASYOCR_MODULE_PATH'] = str(easyocr_path)

    models_path = download_models(output_dir=Path("/models"))

    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = options["do_ocr"]
    pipeline_options.ocr_options.kind = options["ocr_engine"]
    pipeline_options.do_table_structure = options["do_table_structure"]
    pipeline_options.table_structure_options.mode = TableFormerMode(options["table_mode"])
    pipeline_options.generate_page_images = options["include_images"]
    pipeline_options.artifacts_path = models_path
    
    converter = DoclingConvert(s3_coords, pipeline_options)

    results = []
    for item in converter.convert_documents(pre_signed_urls):
        results.append(item)
        logging.info('Convertion result: {}'.format(item))

    return results


@dsl.component(
    packages_to_install=["pydantic", "boto3~=1.35.36", "git+https://github.com/docling-project/docling-jobkit@vku/s3_commons"],
    base_image="python:3.11",
    )
def compute_batches(
        source: Dict = {
            "s3_source_endpoint": "s3.eu-de.cloud-object-storage.appdomain.cloud",
            "s3_source_access_key": "123454321",
            "s3_source_secret_key": "secretsecret",
            "s3_source_bucket": "source-bucket",
            "s3_source_prefix": "my-docs",
            "s3_source_ssl": True
        },
        target: Dict = {
            "s3_target_endpoint": "s3.eu-de.cloud-object-storage.appdomain.cloud",
            "s3_target_access_key": "123454321",
            "s3_target_secret_key": "secretsecret",
            "s3_target_bucket": "target-bucket",
            "s3_target_prefix": "my-docs",
            "s3_target_ssl": True
        },
        batch_size: int = 10
    ) -> List[List[str]]:

    from docling_jobkit.connectors.s3_helper import (
        S3Coordinates,
        get_s3_connection,
        get_source_files,
        generate_presigns_url,
        check_target_has_source_converted
    )
    s3_target_coords = S3Coordinates(
        endpoint = target["s3_target_endpoint"],
        verify_ssl = target["s3_target_ssl"],
        access_key = target["s3_target_access_key"],
        secret_key = target["s3_target_secret_key"],
        bucket = target["s3_target_bucket"],
        key_prefix = target["s3_target_prefix"]
    )

    s3_coords_source = S3Coordinates(
        endpoint = source["s3_source_endpoint"],
        verify_ssl = source["s3_source_ssl"],
        access_key = source["s3_source_access_key"],
        secret_key = source["s3_source_secret_key"],
        bucket = source["s3_source_bucket"],
        key_prefix = source["s3_source_prefix"]
    )

    s3_source_client, s3_source_resource = get_s3_connection(s3_coords_source)
    source_objects_list = get_source_files(s3_source_client, s3_source_resource, s3_coords_source)
    filtered_source_keys = check_target_has_source_converted(s3_target_coords, source_objects_list, s3_coords_source.key_prefix)
    presigned_urls = generate_presigns_url(s3_source_client, filtered_source_keys, s3_coords_source.bucket, batch_size=batch_size, expiration_time=36000)
    
    return presigned_urls


@dsl.pipeline
def docling_hello(
        convertion_options: Dict = {
            "from_formats": ["docx","pptx","html","image","pdf","asciidoc","md","xlsx","xml_uspto","xml_jats","json_docling"],
            "to_formats": ["md"],
            "image_export_mode": "placeholder",
            "do_ocr": True,
            "force_ocr": False,
            "ocr_engine": "easyocr",
            "ocr_lang": [],
            "pdf_backend": "dlparse_v2",
            "table_mode": "accurate",
            "abort_on_error": False,
            "return_as_file": False,
            "do_table_structure": True,
            "include_images": True,
            "images_scale": 2
        },
        source: Dict = {
            "s3_source_endpoint": "s3.eu-de.cloud-object-storage.appdomain.cloud",
            "s3_source_access_key": "123454321",
            "s3_source_secret_key": "secretsecret",
            "s3_source_bucket": "source-bucket",
            "s3_source_prefix": "my-docs",
            "s3_source_ssl": True
        },
        target: Dict = {
            "s3_target_endpoint": "s3.eu-de.cloud-object-storage.appdomain.cloud",
            "s3_target_access_key": "123454321",
            "s3_target_secret_key": "secretsecret",
            "s3_target_bucket": "target-bucket",
            "s3_target_prefix": "my-docs",
            "s3_target_ssl": True
        },
        batch_size: int = 20
    ):

    import logging

    logging.basicConfig(level=logging.INFO)

    batches = compute_batches(source=source, target=target, batch_size=5)
    # disable caching on batches as cached pre-signed urls might have already expired
    batches.set_caching_options(False)

    results = []
    with dsl.ParallelFor(
        batches.output,
        parallelism=3
    ) as subbatch:
        converter = convert_payload(options=convertion_options, target=target, pre_signed_urls=subbatch)
        kubernetes.mount_pvc(
            converter,
            pvc_name="docling-pipelines-models-cache",
            mount_path='/models',
        )
        converter.set_memory_request("1G")
        converter.set_memory_limit("5G")
        converter.set_cpu_request("200m")
        converter.set_cpu_limit("1")

        results.append(converter.output)


from kfp import compiler

compiler.Compiler().compile(docling_hello, 'docling-s3in-s3out.yaml')