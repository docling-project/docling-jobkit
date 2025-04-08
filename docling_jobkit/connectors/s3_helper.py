import json
import logging
import os
from typing import Optional
from urllib.parse import urlparse, urlunsplit

from boto3.resources.base import ServiceResource
from boto3.session import Session
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.paginate import Paginator
from pydantic import BaseModel

from docling.backend.docling_parse_v4_backend import DoclingParseV4DocumentBackend
from docling.datamodel.base_models import ConversionStatus, InputFormat
from docling.datamodel.document import ConversionResult
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.exceptions import ConversionError
from docling_jobkit.model.s3_inputs import S3Coordinates

logging.basicConfig(level=logging.INFO)


def get_s3_connection(coords: S3Coordinates):
    session = Session()

    config = Config(
        connect_timeout=30, retries={"max_attempts": 1}, signature_version="s3v4"
    )
    scheme = "https" if coords.verify_ssl else "http"
    path = "/"
    endpoint = urlunsplit((scheme, coords.endpoint, path, "", ""))

    client: BaseClient = session.client(
        "s3",
        endpoint_url=endpoint,
        verify=coords.verify_ssl,
        aws_access_key_id=coords.access_key.get_secret_value(),
        aws_secret_access_key=coords.secret_key.get_secret_value(),
        config=config,
    )

    resource: ServiceResource = session.resource(
        "s3",
        endpoint_url=endpoint,
        verify=coords.verify_ssl,
        aws_access_key_id=coords.access_key.get_secret_value(),
        aws_secret_access_key=coords.secret_key.get_secret_value(),
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
    s3_resource: ServiceResource, bucket_name: str, prefix: str
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


def generate_presigns_url(
    s3_client: BaseClient,
    source_keys: list,
    s3_source_bucket: str,
    batch_size: int = 10,
    expiration_time: int = 3600,
):
    presigned_urls = []
    counter = 0
    sub_array = []
    array_lenght = len(source_keys)
    for idx, key in enumerate(source_keys):
        try:
            url = s3_client.generate_presigned_url(
                ClientMethod="get_object",
                Params={"Bucket": s3_source_bucket, "Key": key},
                ExpiresIn=expiration_time,
            )
        except ClientError as e:
            logging.error("Generation of presigned url failed: {}".format(e))
        sub_array.append(url)
        counter += 1
        if counter == batch_size or (idx + 1) == array_lenght:
            presigned_urls.append(sub_array)
            sub_array = []
            counter = 0

    return presigned_urls


def get_source_files(s3_source_client, s3_source_resource, s3_coords):
    source_paginator = s3_source_client.get_paginator("list_objects_v2")

    # Check that source is not empty
    source_count = count_s3_objects(
        source_paginator, s3_coords.bucket, s3_coords.key_prefix + "/"
    )
    if source_count == 0:
        logging.error("No documents to process in the source s3 coordinates.")
    return get_keys_s3_objects_as_set(
        s3_source_resource, s3_coords.bucket, s3_coords.key_prefix
    )


def check_target_has_source_converted(coords, source_objects_list, s3_source_prefix):
    s3_target_client, s3_target_resource = get_s3_connection(coords)
    target_paginator = s3_target_client.get_paginator("list_objects_v2")

    converted_prefix = coords.key_prefix + "/json/"
    target_count = count_s3_objects(target_paginator, coords.bucket, converted_prefix)
    logging.debug("Target contains json objects: {}".format(target_count))
    if target_count != 0:
        logging.debug("Target contains objects, checking content...")

        # Collect target keys for iterative conversion
        existing_target_objects = get_keys_s3_objects_as_set(
            s3_target_resource, coords.bucket, converted_prefix
        )

        # Filter-out objects that are already processed
        target_short_key_list = strip_prefix_postfix(
            existing_target_objects, prefix=converted_prefix, extension=".json"
        )
        filtered_source_keys = []
        logging.debug("List of source keys:")
        for key in source_objects_list:
            logging.debug("Object key: {}".format(key))
            clean_key = key.replace(".pdf", "").replace(s3_source_prefix + "/", "")
            if clean_key not in target_short_key_list:
                filtered_source_keys.append(key)

        logging.debug("Total keys: {}".format(len(source_objects_list)))
        logging.debug("Filtered keys to process: {}".format(len(filtered_source_keys)))
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
        logging.error("Put s3 object failed: {}".format(e))
        return False
    return True


class DoclingConvert:
    def __init__(self, s3_coords: S3Coordinates, pipeline_options: PdfPipelineOptions):
        self.coords = s3_coords
        self.s3_client, _ = get_s3_connection(s3_coords)

        self.converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=pipeline_options,
                    backend=DoclingParseV4DocumentBackend,
                )
            }
        )
        self.allowed_formats = [ext.value for ext in InputFormat]

    def convert_documents(self, presigned_urls):
        for url in presigned_urls:
            parsed = urlparse(url)
            root, ext = os.path.splitext(parsed.path)
            # This will skip http links that don't have file extension as part of url, arXiv have plenty of docs like this
            if ext[1:].lower() not in self.allowed_formats:
                continue
            try:
                conv_res: ConversionResult = self.converter.convert(url)
            except ConversionError as e:
                logging.error("Conversion exception: {}".format(e))
            if conv_res.status == ConversionStatus.SUCCESS:
                s3_target_prefix = self.coords.key_prefix
                doc_filename = conv_res.input.file.stem
                logging.debug(f"Converted {doc_filename} now saving results")
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
                data = conv_res.document.export_to_text()
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

    def upload_to_s3(self, file, target_key, content_type):
        return put_object(
            client=self.s3_client,
            bucket=self.coords.bucket,
            object_key=target_key,
            file=file,
            content_type=content_type,
        )
