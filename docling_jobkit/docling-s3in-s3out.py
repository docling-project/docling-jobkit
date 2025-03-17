from kfp import dsl
from kfp import kubernetes
from pydantic import BaseModel
from typing import Dict, List, Optional



@dsl.component(
    packages_to_install=["docling==2.24.0"], #"git+https://my-repo/mycustomlibrary.git"],
    pip_index_urls=["https://download.pytorch.org/whl/cpu", "https://pypi.org/simple"],
    base_image="python:3.11",
    #base_image="quay.io/bbrowning/docling-kfp:v2.25.0",
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
        http_sources: List = [{"url": "https://arxiv.org/pdf/2408.09869"}],
        target: Dict = {
            "s3_target_endpoint": "s3.eu-de.cloud-object-storage.appdomain.cloud",
            "s3_target_access_key": "123454321",
            "s3_target_secret_key": "secretsecret",
            "s3_target_bucket": "target-bucket",
            "s3_target_prefix": "my-docs",
            "s3_target_ssl": True
        }
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

    logging.basicConfig(level=logging.INFO)

    
    easyocr_path = Path("/models/.EasyOCR")
    os.environ['MODULE_PATH'] = str(easyocr_path)
    os.environ['EASYOCR_MODULE_PATH'] = str(easyocr_path)
    # logging.info('The MODULE_PATH value: {}'.format(os.getenv('MODULE_PATH')))
    # logging.info('The EASYOCR_MODULE_PATH value: {}'.format(os.getenv('EASYOCR_MODULE_PATH')))

    models_path = download_models(output_dir=Path("/models"))
    # logging.info('The models path: {}'.format(models_path))

    # logging.info('current options: {}'.format(options))

    # payload_json = json.loads(payload)
    # options = payload_json["options"]

    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = options["do_ocr"]
    pipeline_options.ocr_options.kind = options["ocr_engine"]
    pipeline_options.do_table_structure = options["do_table_structure"]
    pipeline_options.table_structure_options.mode = TableFormerMode(options["table_mode"])
    pipeline_options.generate_page_images = options["include_images"]
    pipeline_options.artifacts_path = models_path
    

    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options,
                backend=DoclingParseDocumentBackend,
            )
        }
    )

    # s3_coords = S3Coordinates(
    #     endpoint = target["s3_target_endpoint"],
    #     verify_ssl = target["s3_target_ssl"],
    #     access_key = target["s3_target_access_key"],
    #     secret_key = target["s3_target_secret_key"],
    #     bucket = target["s3_target_bucket"],
    #     key_prefix = target["s3_target_prefix"]
    # )

    # s3_target, _ = get_s3_connection(s3_coords)


    results = []
    for url in http_sources:
        url = url["url"]
        parsed = urlparse(url)
        root, ext = os.path.splitext(parsed.path)
        # if ext[1:] not in options["from_formats"]:
        #     continue
        conv_res: ConversionResult = converter.convert(url)
        if conv_res.status == ConversionStatus.SUCCESS:
            doc_filename = conv_res.input.file.stem
            logging.info(f"Converted {doc_filename} now saving results")
            # Export Docling document format to JSON:
            # target_key = f"{s3_coords.key_prefix}/json/{doc_filename}.json"
            #  data = json.dumps(conv_res.document.export_to_dict())
            # upload_to_s3(
            #     s3_client=s3_target, 
            #     bucket=s3_coords.target,
            #     file=data,
            #     target_key=target_key,
            #     content_type="application/json",
            # )

            results.append(f"{doc_filename} - SUCCESS")

        elif conv_res.status == ConversionStatus.PARTIAL_SUCCESS:
            results.append(f"{conv_res.input.file} - PARTIAL_SUCCESS")
        else:
            results.append(f"{conv_res.input.file} - FAILURE")

    logging.info('Convertion results: {}'.format(results))

    return results


# @dsl.component(
#     packages_to_install=["pydantic", "boto3~=1.35.36"],
#     base_image="python:3.11",
#     )
# def compute_batches(
#         source: Dict = {
#             "s3_source_endpoint": "s3.eu-de.cloud-object-storage.appdomain.cloud",
#             "s3_source_access_key": "123454321",
#             "s3_source_secret_key": "secretsecret",
#             "s3_source_bucket": "source-bucket",
#             "s3_source_prefix": "my-docs",
#             "s3_source_ssl": True
#         },
#         target: Dict = {
#             "s3_target_endpoint": "s3.eu-de.cloud-object-storage.appdomain.cloud",
#             "s3_target_access_key": "123454321",
#             "s3_target_secret_key": "secretsecret",
#             "s3_target_bucket": "target-bucket",
#             "s3_target_prefix": "my-docs",
#             "s3_target_ssl": True
#         }
#     ) -> List:

#     from botocore.client import BaseClient
#     from boto3.resources.base import ServiceResource
#     from boto3.session import Session
#     from botocore.config import Config
#     from botocore.exceptions import ClientError
#     from botocore.paginate import Paginator

#     session = Session()

#     config = Config(
#         connect_timeout=30, retries={"max_attempts": 1}, signature_version="s3v4"
#     )
#     scheme = "https" if verify_ssl else "http"
#     path="/"
#     s3_endpoint = urlunsplit((scheme, endpoint, path, "", ""))

#     client: BaseClient = session.client(
#         "s3",
#         endpoint_url=s3_endpoint,
#         verify=verify_ssl,
#         aws_access_key_id=access_key,
#         aws_secret_access_key=secret_key,
#         config=config,
#     )

#     resource: ServiceResource = session.resource(
#         "s3",
#         endpoint_url=s3_endpoint,
#         verify=verify_ssl,
#         aws_access_key_id=access_key,
#         aws_secret_access_key=secret_key,
#         config=config,
#     )
#     return []
#     #return client, resource

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
        source: Dict = {},
        # #  = {
        # #     "s3_source_endpoint": "s3.eu-de.cloud-object-storage.appdomain.cloud",
        # #     "s3_source_access_key": "123454321",
        # #     "s3_source_secret_key": "secretsecret",
        # #     "s3_source_bucket": "source-bucket",
        # #     "s3_source_prefix": "my-docs",
        # #     "s3_source_ssl": True
        # # },
        target: Dict = {}
        # #  = {
        # #     "s3_target_endpoint": "s3.eu-de.cloud-object-storage.appdomain.cloud",
        # #     "s3_target_access_key": "123454321",
        # #     "s3_target_secret_key": "secretsecret",
        # #     "s3_target_bucket": "target-bucket",
        # #     "s3_target_prefix": "my-docs",
        # #     "s3_target_ssl": True
        # # },
    ) -> List:

    import json

    conv_opt = {
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
        }

    # sample = {
    #     "options": convertion_options,
    #     "http_sources": [{"url": "https://arxiv.org/pdf/2408.09869"}]
    #     #"http_sources": [{"url": "https://www.wrd.org/files/a995c8e28/Regional+Groundwater+Monitoring+Report+2013-2014.pdf"}]
    # }

    # payload_str = str(sample)


    converter = convert_payload(options=convertion_options, http_sources=[{"url": "https://arxiv.org/pdf/2408.09869"}])
    kubernetes.mount_pvc(
        converter,
        pvc_name="docling-pipelines-models-cache",
        mount_path='/models',
    )
    # kubernetes.add_ephemeral_volume(
    #     converter,
    #     volume_name="docling-models-pvc",
    #     mount_path="/models",
    #     access_modes=['ReadWriteOnce'],
    #     size='5Gi',
    # )
    converter.set_memory_request("1G")
    converter.set_memory_limit("5G")
    converter.set_cpu_request("200m")
    converter.set_cpu_limit("1")


    return converter.output

from kfp import compiler

compiler.Compiler().compile(docling_hello, 'docling-s3in-s3out.yaml')