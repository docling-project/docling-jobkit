import argparse
import json
import os
from typing import Optional
from urllib.parse import urlparse, urlunsplit

import ray
from boto3.resources.base import ServiceResource
from boto3.session import Session
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.paginate import Paginator
from pydantic import BaseModel
from ray._raylet import ObjectRefGenerator  # type: ignore

from docling.backend.docling_parse_backend import DoclingParseDocumentBackend
from docling.datamodel.base_models import ConversionStatus, InputFormat
from docling.datamodel.document import ConversionResult
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
from docling.document_converter import DocumentConverter, PdfFormatOption

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

    client: BaseClient = session.client(
        "s3",
        endpoint_url=endpoint,
        verify=coords.verify_ssl,
        aws_access_key_id=coords.access_key,
        aws_secret_access_key=coords.secret_key,
        config=config,
    )

    resource: ServiceResource = session.resource(
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


def generate_presigns_url(s3_client: BaseClient, source_keys: list):
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


@ray.remote(max_concurrency=max_concurrency)  # type: ignore
class DoclingConvert:
    def __init__(self, s3_coords: S3Coordinates, presigned_urls: list):
        self.coords = s3_coords
        self.s3_client, _ = get_s3_connection(s3_coords)
        self.presigned_urls = presigned_urls

        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = do_ocr
        pipeline_options.do_table_structure = do_table_structure
        pipeline_options.table_structure_options.mode = TableFormerMode(
            table_structure_mode
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
    ray.init(local_mode=False)

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
