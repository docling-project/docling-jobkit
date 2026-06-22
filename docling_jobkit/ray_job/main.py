from __future__ import annotations

import sys

if sys.version_info >= (3, 14):
    raise ImportError("ray support is not yet available for Python 3.14.")

import argparse
import json
import os
from typing import TYPE_CHECKING, Optional, List, Tuple, Union
from urllib.parse import urlparse, urlunsplit

import ray
from boto3.session import Session
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.paginate import Paginator
from pydantic import BaseModel
from ray._raylet import ObjectRefGenerator  # type: ignore

from ray.util.metrics import Counter, Gauge, Histogram

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client, S3ServiceResource

from docling.backend.docling_parse_backend import DoclingParseDocumentBackend
from docling.datamodel.base_models import ConversionStatus, InputFormat
from docling.datamodel.document import ConversionResult
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    TableFormerMode,
    TableStructureOptions,
)
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.settings import settings

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
do_ocr = bool(os.environ.get("SETTINGS_DO_OCR", True))
do_table_structure = bool(os.environ.get("SETTINGS_DO_TABLE_STRUCTURE", True))
table_structure_mode = os.environ.get("SETTINGS_TABLE_STRUCTURE_MODE", "fast")
generate_page_images = bool(os.environ.get("SETTINGS_GENERATE_PAGE_IMAGES", True))
generate_metrics = bool(os.environ.get("SETTINGS_GENERATE_METRICS", False))
metrics_port = str(os.environ.get("SETTINGS_METRICS_PORT", "8080"))


class S3Coordinates(BaseModel):
    endpoint: str
    verify_ssl: bool
    access_key: str
    secret_key: str
    bucket: str
    key_prefix: str


def get_s3_connection(coords: S3Coordinates):
    session = Session()

    config = Config(
        connect_timeout=30, retries={"max_attempts": 1}, signature_version="s3v4"
    )
    scheme = "https" if coords.verify_ssl else "http"
    path = "/"
    endpoint = urlunsplit((scheme, coords.endpoint, path, "", ""))

    client: S3Client = session.client(
        "s3",
        endpoint_url=endpoint,
        verify=coords.verify_ssl,
        aws_access_key_id=coords.access_key,
        aws_secret_access_key=coords.secret_key,
        config=config,
    )

    resource: S3ServiceResource = session.resource(
        "s3",
        endpoint_url=endpoint,
        verify=coords.verify_ssl,
        aws_access_key_id=coords.access_key,
        aws_secret_access_key=coords.secret_key,
        config=config,
    )

    return client, resource


def count_s3_objects(paginator: Paginator, bucket_name: str, prefix: str):
    response_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    count_obj = 0
    for page in response_iterator:
        if page.get("Contents"):
            count_obj += sum(1 for _ in page["Contents"])

    return count_obj


def get_keys_s3_objects_as_set(
    s3_resource: S3ServiceResource, bucket_name: str, prefix: str
):
    bucket = s3_resource.Bucket(bucket_name)
    folder_objects = list(bucket.objects.filter(Prefix=prefix))
    files_on_s3 = set()
    for file in folder_objects:
        files_on_s3.add(file.key)
    return files_on_s3


def strip_prefix_postfix(source_set, prefix="", extension=""):
    output = set()
    for key in source_set:
        output.add(key.replace(extension, "").replace(prefix, ""))
    return output


def generate_presigns_url(s3_client: S3Client, source_keys: list):
    presigned_urls = []
    counter = 0
    sub_array = []
    array_lenght = len(source_keys)
    for idx, key in enumerate(source_keys):
        try:
            url = s3_client.generate_presigned_url(
                ClientMethod="get_object",
                Params={"Bucket": s3_source_bucket, "Key": key},
                ExpiresIn=3600,
            )
        except ClientError as e:
            print(e)
        sub_array.append(url)
        counter += 1
        if counter == batch_size or (idx + 1) == array_lenght:
            presigned_urls.append(sub_array)
            sub_array = []
            counter = 0

    return presigned_urls


def get_source_files(s3_source_client, s3_source_resource):
    source_paginator = s3_source_client.get_paginator("list_objects_v2")

    # Check that source is not empty
    source_count = count_s3_objects(
        source_paginator, s3_source_bucket, s3_source_prefix + "/"
    )
    if source_count == 0:
        print("s3 source is empty")
        ray.shutdown()
    return get_keys_s3_objects_as_set(
        s3_source_resource, s3_source_bucket, s3_source_prefix
    )


def check_target_has_source_converted(coords, source_objects_list):
    s3_target_client, s3_target_resource = get_s3_connection(coords)
    target_paginator = s3_target_client.get_paginator("list_objects_v2")

    converted_prefix = s3_target_prefix + "/json/"
    target_count = count_s3_objects(
        target_paginator, s3_target_bucket, converted_prefix
    )
    print("Target contains json objects: ", target_count)
    if target_count != 0:
        print("Target contains objects, checking content...")

        # Collect target keys for iterative conversion
        existing_target_objects = get_keys_s3_objects_as_set(
            s3_target_resource, s3_target_bucket, converted_prefix
        )

        # Filter-out objects that are already processed
        target_short_key_list = strip_prefix_postfix(
            existing_target_objects, prefix=converted_prefix, extension=".json"
        )
        filtered_source_keys = []
        print("List of source keys:")
        for key in source_objects_list:
            print(key)
            clean_key = key.replace(".pdf", "").replace(s3_source_prefix + "/", "")
            if clean_key not in target_short_key_list:
                filtered_source_keys.append(key)

        print("Total keys: ", len(source_objects_list))
        print("Filtered keys to process: ", len(filtered_source_keys))
    else:
        filtered_source_keys = source_objects_list

    return filtered_source_keys


def put_object(
    client,
    bucket: str,
    object_key: str,
    file: str,
    content_type: Optional[str] = None,
) -> bool:
    """Upload a file to an S3 bucket

    :param file: File to upload
    :param bucket: Bucket to upload to
    :param object_key: S3 key to upload to
    :return: True if file was uploaded, else False
    """

    kwargs = {}

    if content_type is not None:
        kwargs["ContentType"] = content_type

    try:
        client.put_object(Body=file, Bucket=bucket, Key=object_key, **kwargs)
    except ClientError as e:
        print(e)
        return False
    return True

def calculate_stats(values: List[Union[int, float]]) -> Tuple[Union[int, float], Union[int, float], float]:
    # Validate that all values are numeric
    if not all(isinstance(v, (int, float)) for v in values):
        raise TypeError("All values must be numeric (int or float)")
    
    # Calculate min and max
    min_val = min(values)
    max_val = max(values)
    
    # Calculate median
    sorted_values = sorted(values)
    n = len(sorted_values)
    
    if n % 2 == 0:
        # Even number of elements: average of two middle values
        median = (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
    else:
        # Odd number of elements: middle value
        median = float(sorted_values[n // 2])
    
    return min_val, max_val, median


def reduce_timings(conversion_timings: dict):
    timing_stats={}
    if conversion_timings:
        if "pipeline_total" in conversion_timings:
            timing_stats["total_pipeline"] = conversion_timings["pipeline_total"].times[0]
        if "page_parse" in conversion_timings:
            page_parse_min, page_parse_max, page_parse_median  = calculate_stats(conversion_timings["page_parse"].times)
            timing_stats["page_parse"] = {"min": page_parse_min, "max": page_parse_max, "median": page_parse_median}
        if "ocr" in conversion_timings:
            ocr_min, ocr_max, ocr_median = calculate_stats(conversion_timings["ocr"].times)
            timing_stats["ocr"] = {"min": ocr_min, "max": ocr_max, "median": ocr_median}
        if "layout" in conversion_timings:
            layout_min, layout_max, layout_median = calculate_stats(conversion_timings["layout"].times)
            timing_stats["layout"] = {"min": layout_min, "max": layout_max, "median": layout_median}
        if "table_structure" in conversion_timings:
            table_structure_min, table_structure_max, table_structure_median = calculate_stats(conversion_timings["table_structure"].times)
            timing_stats["table_structure"] = {"min": table_structure_min, "max": table_structure_max, "median": table_structure_median}
        if "page_assemble" in conversion_timings:
            page_assemble_min, page_assemble_max, page_assemble_median = calculate_stats(conversion_timings["page_assemble"].times)
            timing_stats["page_assemble"] = {"min": page_assemble_min, "max": page_assemble_max, "median": page_assemble_median}
        if "doc_assemble" in conversion_timings:
            timing_stats["doc_assemble"] = conversion_timings["doc_assemble"].times[0]
        if "reading_order" in conversion_timings:
            timing_stats["reading_order"] = conversion_timings["reading_order"].times[0]
        if "doc_enrich" in conversion_timings:
            timing_stats["doc_enrich"] = conversion_timings["doc_enrich"].times[0]
    return timing_stats
    
def collect_doc_stats(conv_res: ConversionResult):
    doc_stats = {}
    if hasattr(conv_res.input, 'format'):
        doc_stats["input_format"] = conv_res.input.format
    doc = conv_res.document
    if hasattr(doc, 'pages'):
        doc_stats["num_pages"] = len(doc.pages)
        doc_stats["pictures"] = len(doc.pictures)
        doc_stats["tables"] = len(doc.tables)
        doc_stats["key_value_items"] = len(doc.key_value_items)
        doc_stats["form_items"] = len(doc.form_items)
        doc_stats["texts"] = len(doc.texts)
        doc_stats["groups"] = len(doc.groups)
    return doc_stats



@ray.remote(max_concurrency=max_concurrency)  # type: ignore
class DoclingConvert:
    def __init__(self, s3_coords: S3Coordinates, presigned_urls: list):
        self.coords = s3_coords
        self.s3_client, _ = get_s3_connection(s3_coords)
        self.presigned_urls = presigned_urls

        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = do_ocr
        pipeline_options.do_table_structure = do_table_structure
        pipeline_options.table_structure_options = TableStructureOptions(
            mode=TableFormerMode(table_structure_mode)
        )
        pipeline_options.generate_page_images = generate_page_images

        self.converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=pipeline_options,
                    backend=DoclingParseDocumentBackend,
                )
            }
        )
        self.allowed_formats = [ext.value for ext in InputFormat]


        ## ------------ metrics ---------------
        # enable timings
        if generate_metrics:
            settings.debug.profile_pipeline_timings = True
            
            tenant_id =  "dummy_id"
            #-----
            self.success_counter = Counter(
                "conversion_success",
                description="Number of successeful conversions",
                tag_keys=("tenant_id",),
            )
            self.success_counter.set_default_tags({"tenant_id": tenant_id})
            
            self.partial_counter = Counter(
                "conversion_partial",
                description="Number of partial conversions",
                tag_keys=("tenant_id",),
            )
            self.partial_counter.set_default_tags({"tenant_id": tenant_id})

            self.failed_counter = Counter(
                "conversion_failed",
                description="Number of failed conversions",
                tag_keys=("tenant_id",),
            )
            self.failed_counter.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.pipeline_total_hist = Histogram(
                "pipeline_total",
                description="Total pipeline execution time in seconds",
                #boundaries=[0.1, 1],
                tag_keys=("tenant_id",),
            )
            self.pipeline_total_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.page_parse_low_hist = Histogram(
                "page_parse_low",
                description="Lowest page parse time in seconds",
                tag_keys=("tenant_id",),
            )
            self.page_parse_low_hist.set_default_tags({"tenant_id": tenant_id})

            self.page_parse_high_hist = Histogram(
                "page_parse_high",
                description="Highest page parse time in seconds",
                tag_keys=("tenant_id",),
            )
            self.page_parse_high_hist.set_default_tags({"tenant_id": tenant_id})

            self.page_parse_median_hist = Histogram(
                "page_parse_median",
                description="Median page parse time in seconds",
                tag_keys=("tenant_id",),
            )
            self.page_parse_median_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.ocr_low_hist = Histogram(
                "ocr_low",
                description="Lowest ocr time in seconds",
                tag_keys=("tenant_id",),
            )
            self.ocr_low_hist.set_default_tags({"tenant_id": tenant_id})

            self.ocr_high_hist = Histogram(
                "ocr_high",
                description="Highest ocr time in seconds",
                tag_keys=("tenant_id",),
            )
            self.ocr_high_hist.set_default_tags({"tenant_id": tenant_id})

            self.ocr_median_hist = Histogram(
                "ocr_median",
                description="Median ocr time in seconds",
                tag_keys=("tenant_id",),
            )
            self.ocr_median_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.layout_low_hist = Histogram(
                "layout_low",
                description="Lowest layout time in seconds",
                tag_keys=("tenant_id",),
            )
            self.layout_low_hist.set_default_tags({"tenant_id": tenant_id})

            self.layout_high_hist = Histogram(
                "layout_high",
                description="Highest layout time in seconds",
                tag_keys=("tenant_id",),
            )
            self.layout_high_hist.set_default_tags({"tenant_id": tenant_id})

            self.layout_median_hist = Histogram(
                "layout_median",
                description="Median layout time in seconds",
                tag_keys=("tenant_id",),
            )
            self.layout_median_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.table_structure_low_hist = Histogram(
                "table_structure_low",
                description="Lowest table structure time in seconds",
                tag_keys=("tenant_id",),
            )
            self.table_structure_low_hist.set_default_tags({"tenant_id": tenant_id})

            self.table_structure_high_hist = Histogram(
                "table_structure_high",
                description="Highest table structure time in seconds",
                tag_keys=("tenant_id",),
            )
            self.table_structure_high_hist.set_default_tags({"tenant_id": tenant_id})

            self.table_structure_median_hist = Histogram(
                "table_structure_median",
                description="Median table structure time in seconds",
                tag_keys=("tenant_id",),
            )
            self.table_structure_median_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.page_assemble_low_hist = Histogram(
                "page_assemble_low",
                description="Lowest page assemble time in seconds",
                tag_keys=("tenant_id",),
            )
            self.page_assemble_low_hist.set_default_tags({"tenant_id": tenant_id})

            self.page_assemble_high_hist = Histogram(
                "page_assemble_high",
                description="Highest page assemble time in seconds",
                tag_keys=("tenant_id",),
            )
            self.page_assemble_high_hist.set_default_tags({"tenant_id": tenant_id})

            self.page_assemble_median_hist = Histogram(
                "page_assemble_median",
                description="Median page assemble time in seconds",
                tag_keys=("tenant_id",),
            )
            self.page_assemble_median_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.doc_assemble_hist = Histogram(
                "doc_assemble",
                description="Document assemble time in seconds",
                tag_keys=("tenant_id",),
            )
            self.doc_assemble_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.reading_order_hist = Histogram(
                "reading_order",
                description="Reading order time in seconds",
                tag_keys=("tenant_id",),
            )
            self.reading_order_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.doc_enrich_hist = Histogram(
                "doc_enrich",
                description="Document enrichment time in seconds",
                tag_keys=("tenant_id",),
            )
            self.doc_enrich_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.doc_type_pdf_counter = Counter(
                "doc_type_pdf",
                description="Number of pdf documents",
                tag_keys=("tenant_id",),
            )
            self.doc_type_pdf_counter.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.doc_type_docx_counter = Counter(
                "doc_type_docx",
                description="Number of docx documents",
                tag_keys=("tenant_id",),
            )
            self.doc_type_docx_counter.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.doc_type_pptx_counter = Counter(
                "doc_type_pptx",
                description="Number of pptx documents",
                tag_keys=("tenant_id",),
            )
            self.doc_type_pptx_counter.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.doc_type_html_counter = Counter(
                "doc_type_html",
                description="Number of html documents",
                tag_keys=("tenant_id",),
            )
            self.doc_type_html_counter.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.doc_type_image_counter = Counter(
                "doc_type_image",
                description="Number of image documents",
                tag_keys=("tenant_id",),
            )
            self.doc_type_image_counter.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.doc_type_md_counter = Counter(
                "doc_type_md",
                description="Number of md documents",
                tag_keys=("tenant_id",),
            )
            self.doc_type_md_counter.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.doc_type_xlsx_counter = Counter(
                "doc_type_xlsx",
                description="Number of xlsx documents",
                tag_keys=("tenant_id",),
            )
            self.doc_type_xlsx_counter.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.doc_type_xml_counter = Counter(
                "doc_type_xml",
                description="Number of xml documents",
                tag_keys=("tenant_id",),
            )
            self.doc_type_xml_counter.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.doc_type_doclang_counter = Counter(
                "doc_type_doclang",
                description="Number of doclang documents",
                tag_keys=("tenant_id",),
            )
            self.doc_type_doclang_counter.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.doc_type_docling_counter = Counter(
                "doc_type_docling",
                description="Number of docling type documents",
                tag_keys=("tenant_id",),
            )
            self.doc_type_docling_counter.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.doc_type_other_counter = Counter(
                "doc_type_other",
                description="Number of other type documents",
                tag_keys=("tenant_id",),
            )
            self.doc_type_other_counter.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.num_pages_hist = Histogram(
                "num_pages",
                description="Number of pages in converted document",
                tag_keys=("tenant_id",),
            )
            self.num_pages_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.pictures_hist = Histogram(
                "pictures",
                description="Number of pictures in converted document",
                tag_keys=("tenant_id",),
            )
            self.pictures_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.tables_hist = Histogram(
                "tables",
                description="Number of tables in converted document",
                tag_keys=("tenant_id",),
            )
            self.tables_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.key_value_items_hist = Histogram(
                "key_value_items",
                description="Number of key value items in converted document",
                tag_keys=("tenant_id",),
            )
            self.key_value_items_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.form_items_hist = Histogram(
                "form_items",
                description="Number of form items in converted document",
                tag_keys=("tenant_id",),
            )
            self.form_items_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.texts_hist = Histogram(
                "texts",
                description="Number of text items in converted document",
                tag_keys=("tenant_id",),
            )
            self.texts_hist.set_default_tags({"tenant_id": tenant_id})
            #-----
            self.groups_hist = Histogram(
                "groups",
                description="Number of group items in converted document",
                tag_keys=("tenant_id",),
            )
            self.groups_hist.set_default_tags({"tenant_id": tenant_id})


    def convert_document(self, index, db_ref):
        for url in db_ref[index]:
            parsed = urlparse(url)
            root, ext = os.path.splitext(parsed.path)
            if ext[1:] not in self.allowed_formats:
                continue
            conv_res: ConversionResult = self.converter.convert(url)
            
            if conv_res.status == ConversionStatus.SUCCESS:
                doc_filename = conv_res.input.file.stem
                print(f"Converted {doc_filename} now saving results")
                # Export Docling document format to JSON:
                target_key = f"{s3_target_prefix}/json/{doc_filename}.json"
                data = json.dumps(conv_res.document.export_to_dict())
                self.upload_to_s3(
                    file=data,
                    target_key=target_key,
                    content_type="application/json",
                )
                # Export Docling document format to doctags:
                target_key = f"{s3_target_prefix}/doctags/{doc_filename}.doctags.txt"
                data = conv_res.document.export_to_document_tokens()
                self.upload_to_s3(
                    file=data,
                    target_key=target_key,
                    content_type="text/plain",
                )
                # Export Docling document format to markdown:
                target_key = f"{s3_target_prefix}/md/{doc_filename}.md"
                data = conv_res.document.export_to_markdown()
                self.upload_to_s3(
                    file=data,
                    target_key=target_key,
                    content_type="text/markdown",
                )
                # Export Docling document format to text:
                target_key = f"{s3_target_prefix}/txt/{doc_filename}.txt"
                data = conv_res.document.export_to_markdown(strict_text=True)
                self.upload_to_s3(
                    file=data,
                    target_key=target_key,
                    content_type="text/plain",
                )
                yield f"{doc_filename} - SUCCESS"

            elif conv_res.status == ConversionStatus.PARTIAL_SUCCESS:
                yield f"{conv_res.input.file} - PARTIAL_SUCCESS"
            else:
                yield f"{conv_res.input.file} - FAILURE"

            # collect and push metrics
            if generate_metrics:
                pipeline_stats = reduce_timings(conv_res.timings)
                if hasattr(pipeline_stats, 'pipeline_total'):
                    self.pipeline_total_hist.observe(pipeline_stats["pipeline_total"])
                if hasattr(pipeline_stats, 'page_parse'):
                    self.page_parse_low_hist.observe(pipeline_stats["page_parse"]["min"])
                    self.page_parse_high_hist.observe(pipeline_stats["page_parse"]["max"])
                    self.page_parse_median_hist.observe(pipeline_stats["page_parse"]["median"])
                if hasattr(pipeline_stats, 'ocr'):
                    self.ocr_low_hist.observe(pipeline_stats["ocr"]["min"])
                    self.ocr_high_hist.observe(pipeline_stats["ocr"]["max"])
                    self.ocr_median_hist.observe(pipeline_stats["ocr"]["median"])
                if hasattr(pipeline_stats, 'layout'):
                    self.layout_low_hist.observe(pipeline_stats["layout"]["min"])
                    self.layout_high_hist.observe(pipeline_stats["layout"]["max"])
                    self.layout_median_hist.observe(pipeline_stats["layout"]["median"])
                if hasattr(pipeline_stats, 'table_structure'):
                    self.table_structure_low_hist.observe(pipeline_stats["table_structure"]["min"])
                    self.table_structure_high_hist.observe(pipeline_stats["table_structure"]["max"])
                    self.table_structure_median_hist.observe(pipeline_stats["table_structure"]["median"])
                if hasattr(pipeline_stats, 'page_assemble'):
                    self.page_assemble_low_hist.observe(pipeline_stats["page_assemble"]["min"])
                    self.page_assemble_high_hist.observe(pipeline_stats["page_assemble"]["max"])
                    self.page_assemble_median_hist.observe(pipeline_stats["page_assemble"]["median"])
                if hasattr(pipeline_stats, 'doc_assemble'):
                    self.doc_assemble_hist.observe(pipeline_stats["doc_assemble"])
                if hasattr(pipeline_stats, 'reading_order'):
                    self.reading_order_hist.observe(pipeline_stats["reading_order"])
                if hasattr(pipeline_stats, 'doc_enrich'):
                    self.doc_enrich_hist.observe(pipeline_stats["doc_enrich"])

                document_stats = collect_doc_stats(conv_res)
                if hasattr(document_stats, 'input_format'):
                    doc_type = document_stats["input_format"]
                    if doc_type == InputFormat.PDF :
                        self.doc_type_pdf_counter.inc()
                    elif doc_type == InputFormat.DOCX :
                        self.doc_type_docx_counter.inc()
                    elif doc_type == InputFormat.PPTX :
                        self.doc_type_pptx_counter.inc()
                    elif doc_type == InputFormat.HTML :
                        self.doc_type_html_counter.inc()
                    elif doc_type == InputFormat.IMAGE :
                        self.doc_type_image_counter.inc()
                    elif doc_type == InputFormat.MD :
                        self.doc_type_md_counter.inc()
                    elif doc_type == InputFormat.XLSX :
                        self.doc_type_xlsx_counter.inc()
                    elif doc_type in (InputFormat.XML_USPTO, InputFormat.XML_JATS, InputFormat.XML_XBRL) :
                        self.doc_type_xml_counter.inc()
                    elif doc_type == InputFormat.XML_DOCLANG :
                        self.doc_type_doclang_counter.inc()
                    elif doc_type == InputFormat.JSON_DOCLING :
                        self.doc_type_docling_counter.inc()
                    else:
                        self.doc_type_other_counter.inc()
                if hasattr(document_stats, 'num_pages'):
                    self.num_pages_hist.observe(document_stats["num_pages"])
                if hasattr(document_stats, 'pictures'):
                    self.pictures_hist.observe(document_stats["pictures"])
                if hasattr(document_stats, 'tables'):
                    self.tables_hist.observe(document_stats["tables"])
                if hasattr(document_stats, 'key_value_items'):
                    self.key_value_items_hist.observe(document_stats["key_value_items"])
                if hasattr(document_stats, 'form_items'):
                    self.form_items_hist.observe(document_stats["form_items"])
                if hasattr(document_stats, 'texts'):
                    self.texts_hist.observe(document_stats["texts"])
                if hasattr(document_stats, 'groups'):
                    self.groups_hist.observe(document_stats["groups"])   

                conv_status = conv_res.status
                if conv_status == ConversionStatus.SUCCESS:
                    self.success_counter.inc()
                elif conv_status == ConversionStatus.PARTIAL_SUCCESS:
                    self.partial_counter.inc()
                else:
                    self.failed_counter.inc()
            
            

    def upload_to_s3(self, file, target_key, content_type):
        return put_object(
            client=self.s3_client,
            bucket=self.coords.bucket,
            object_key=target_key,
            file=file,
            content_type=content_type,
        )


# This is executed on the ray-head
def main(args):
    # Init ray
    if generate_metrics:
        ray.init(
            local_mode=False,
            _metrics_export_port=metrics_port
        )
    else:
        ray.init(
            local_mode=False
        )

    # Check inputs
    if (
        (not s3_source_access_key)
        or (not s3_source_secret_key)
        or (not s3_target_access_key)
        or (not s3_target_secret_key)
    ):
        print("s3 source or target keys are missing")
        ray.shutdown()
    if (not s3_source_endpoint) or (not s3_target_endpoint):
        print("s3 source or target endpoint is missing")
        ray.shutdown()
    if (not s3_source_bucket) or (not s3_target_bucket):
        print("s3 source or target bucket is missing")
        ray.shutdown()
    if (
        (s3_source_endpoint == s3_target_endpoint)
        and (s3_source_bucket == s3_target_bucket)
        and (s3_source_prefix == s3_target_prefix)
    ):
        print("s3 source and target are the same")
        ray.shutdown()
    if batch_size == 0:
        print("batch_size have to be higher than zero")
        ray.shutdown()

    # get source keys
    s3_coords_source = S3Coordinates(
        endpoint=s3_source_endpoint,
        verify_ssl=s3_source_ssl,
        access_key=s3_source_access_key,
        secret_key=s3_source_secret_key,
        bucket=s3_source_bucket,
        key_prefix=s3_source_prefix,
    )
    s3_source_client, s3_source_resource = get_s3_connection(s3_coords_source)
    source_objects_list = get_source_files(s3_source_client, s3_source_resource)

    # filter source keys
    s3_coords_target = S3Coordinates(
        endpoint=s3_target_endpoint,
        verify_ssl=s3_target_ssl,
        access_key=s3_target_access_key,
        secret_key=s3_target_secret_key,
        bucket=s3_target_bucket,
        key_prefix=s3_target_prefix,
    )

    filtered_source_keys = check_target_has_source_converted(
        s3_coords_target, source_objects_list
    )

    # Generate pre-signed urls
    presigned_urls = generate_presigns_url(s3_source_client, filtered_source_keys)

    # Init ray actor
    c = DoclingConvert.remote(s3_coords_target, presigned_urls)
    # Send payload to ray
    db_object_ref = ray.put(presigned_urls)
    # Launch tasks
    object_references = [
        c.convert_document.remote(index, db_object_ref)
        for index in range(len(presigned_urls))
    ]

    ready, unready = [], object_references
    result = []
    while unready:
        ready, unready = ray.wait(unready)
        for r in ready:
            if isinstance(r, ObjectRefGenerator):
                try:
                    ref = next(r)
                    result.append(ray.get(ref))
                except StopIteration:
                    pass
                else:
                    print("Unready")
                    unready.append(r)
            else:
                result.append(ray.get(r))

    print(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Basic docling ray app")

    args = parser.parse_args()
    main(args)
