import logging
import os
import tempfile
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urlparse, urlunsplit

from boto3.resources.base import ServiceResource
from boto3.session import Session
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.paginate import Paginator
from pydantic import BaseModel

from docling.datamodel.base_models import ConversionStatus, InputFormat
from docling.datamodel.document import ConversionResult
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.exceptions import ConversionError
from docling.utils.utils import create_hash
from docling_core.types.doc.base import ImageRefMode
from docling_core.types.doc.document import DoclingDocument, PageItem, PictureItem

logging.basicConfig(level=logging.INFO)


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


def generate_batch_keys(
    source_keys: list,
    batch_size: int = 10,
):
    batched_keys = []
    counter = 0
    sub_array = []
    array_lenght = len(source_keys)
    for idx, key in enumerate(source_keys):
        sub_array.append(key)
        counter += 1
        if counter == batch_size or (idx + 1) == array_lenght:
            batched_keys.append(sub_array)
            sub_array = []
            counter = 0

    return batched_keys


def generate_presign_url(
    s3_client: BaseClient,
    source_key: str,
    s3_source_bucket: str,
    expiration_time: int = 3600,
) -> Optional[str]:
    try:
        return s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": s3_source_bucket, "Key": source_key},
            ExpiresIn=expiration_time,
        )
    except ClientError as e:
        logging.error("Generation of presigned url failed: {}".format(e))
        return None


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
    """Upload a object to an S3 bucket

    :param file: Object to upload
    :param bucket: Bucket to upload to
    :param object_key: S3 key to upload to
    :return: True if object was uploaded, else False
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


def upload_file(
    client,
    bucket: str,
    object_key: str,
    file_name: Union[str, Path],
    content_type: Optional[str] = None,
):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_key: S3 key to upload to
    :param Optional[content_type]: Content type of file
    :return: True if file was uploaded, else False
    """

    kwargs = {}

    if content_type is not None:
        kwargs["ContentType"] = content_type

    try:
        client.upload_file(file_name, bucket, object_key, ExtraArgs={**kwargs})
    except ClientError as e:
        logging.error("Upload file to s3 failed: {}".format(e))
        return False
    return True


class DoclingConvert:
    def __init__(
        self,
        source_s3_coords: S3Coordinates,
        target_s3_coords: S3Coordinates,
        pipeline_options: PdfPipelineOptions,
        allowed_formats: Optional[list[str]] = None,
        to_formats: Optional[list[str]] = None,
    ):
        self.source_coords = source_s3_coords
        self.source_s3_client, _ = get_s3_connection(source_s3_coords)

        self.target_coords = target_s3_coords
        self.target_s3_client, _ = get_s3_connection(target_s3_coords)

        self.converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=pipeline_options,
                )
            }
        )
        if not allowed_formats:
            self.allowed_formats = [ext.value for ext in InputFormat]
        else:
            self.allowed_formats = [
                ext.value for ext in InputFormat if ext.value in allowed_formats
            ]

        self.to_formats = to_formats

        self.export_page_images = pipeline_options.generate_page_images
        self.export_images = pipeline_options.generate_picture_images

    def convert_documents(self, object_keys):
        for key in object_keys:
            url = generate_presign_url(
                self.source_s3_client,
                key,
                self.source_coords.bucket,
                expiration_time=36000,
            )
            if not url:
                continue
            parsed = urlparse(url)
            root, ext = os.path.splitext(parsed.path)
            # This will skip http links that don't have file extension as part of url, arXiv have plenty of docs like this
            if ext[1:] not in self.allowed_formats:
                continue
            try:
                conv_res: ConversionResult = self.converter.convert(url)
            except ConversionError as e:
                logging.error("Conversion exception: {}".format(e))
            if conv_res.status == ConversionStatus.SUCCESS:
                s3_target_prefix = self.target_coords.key_prefix
                doc_filename = conv_res.input.file.stem
                logging.debug(f"Converted {doc_filename} now saving results")

                if self.export_page_images:
                    # Export pages images:
                    self.upload_page_images(
                        conv_res.document.pages,
                        conv_res.input.document_hash,
                    )

                if self.export_images:
                    # Export pictures
                    self.upload_pictures(
                        conv_res.document,
                        conv_res.input.document_hash,
                    )

                if self.to_formats is None or (
                    self.to_formats and "json" in self.to_formats
                ):
                    # Export Docling document format to JSON:
                    target_key = f"{s3_target_prefix}/json/{doc_filename}.json"
                    with tempfile.NamedTemporaryFile() as temp_json_file:
                        conv_res.document.save_as_json(
                            filename=Path(temp_json_file.name),
                            image_mode=ImageRefMode.REFERENCED,
                        )
                        self.upload_file_to_s3(
                            file=temp_json_file.name,
                            target_key=target_key,
                            content_type="application/json",
                        )
                if self.to_formats is None or (
                    self.to_formats and "doctags" in self.to_formats
                ):
                    # Export Docling document format to doctags:
                    target_key = (
                        f"{s3_target_prefix}/doctags/{doc_filename}.doctags.txt"
                    )
                    data = conv_res.document.export_to_document_tokens()
                    self.upload_object_to_s3(
                        file=data,
                        target_key=target_key,
                        content_type="text/plain",
                    )
                if self.to_formats is None or (
                    self.to_formats and "md" in self.to_formats
                ):
                    # Export Docling document format to markdown:
                    target_key = f"{s3_target_prefix}/md/{doc_filename}.md"
                    data = conv_res.document.export_to_markdown()
                    self.upload_object_to_s3(
                        file=data,
                        target_key=target_key,
                        content_type="text/markdown",
                    )
                if self.to_formats is None or (
                    self.to_formats and "html" in self.to_formats
                ):
                    # Export Docling document format to html:
                    target_key = f"{s3_target_prefix}/html/{doc_filename}.html"
                    with tempfile.NamedTemporaryFile() as temp_html_file:
                        conv_res.document.save_as_html(Path(temp_html_file.name))
                        self.upload_file_to_s3(
                            file=temp_html_file.name,
                            target_key=target_key,
                            content_type="text/html",
                        )
                if self.to_formats is None or (
                    self.to_formats and "text" in self.to_formats
                ):
                    # Export Docling document format to text:
                    target_key = f"{s3_target_prefix}/txt/{doc_filename}.txt"
                    data = conv_res.document.export_to_text()
                    self.upload_object_to_s3(
                        file=data,
                        target_key=target_key,
                        content_type="text/plain",
                    )
                yield f"{doc_filename} - SUCCESS"

            elif conv_res.status == ConversionStatus.PARTIAL_SUCCESS:
                yield f"{conv_res.input.file} - PARTIAL_SUCCESS"
            else:
                yield f"{conv_res.input.file} - FAILURE"

    def upload_object_to_s3(self, file, target_key, content_type):
        success = put_object(
            client=self.target_s3_client,
            bucket=self.target_coords.bucket,
            object_key=target_key,
            file=file,
            content_type=content_type,
        )
        if not success:
            logging.error(
                f"{file} - UPLOAD-FAIL: an error occour uploading object type {content_type} to s3"
            )
        return success

    def upload_file_to_s3(self, file, target_key, content_type):
        success = upload_file(
            client=self.target_s3_client,
            bucket=self.target_coords.bucket,
            object_key=target_key,
            file_name=file,
            content_type=content_type,
        )
        if not success:
            logging.error(
                f"{file} - UPLOAD-FAIL: an error occour uploading file type {content_type} to s3"
            )
        return success

    def upload_page_images(self, pages: dict[int, PageItem], doc_hash: str):
        for page_no, page in pages.items():
            try:
                if page.image and page.image.pil_image:
                    page_hash = create_hash(f"{doc_hash}_page_no_{page_no}")
                    page_image_key = f"../pages/{page_hash}.png"

                    self.upload_object_to_s3(
                        file=page.image.pil_image.tobytes(),
                        target_key=page_image_key,
                        content_type="application/png",
                    )
                    page.image.uri = Path(page_image_key)

            except Exception as exc:
                logging.error(
                    "Upload image of page with hash %r raised error: %r",
                    page_hash,
                    exc,
                )

    def upload_pictures(self, document: DoclingDocument, doc_hash: str):
        picture_number = 0
        for element, _level in document.iterate_items():
            if isinstance(element, PictureItem):
                if element.image and element.image.pil_image:
                    try:
                        element_hash = create_hash(f"{doc_hash}_img_{picture_number}")
                        element_image_key = f"../images/{element_hash}.png"

                        self.upload_object_to_s3(
                            file=element.image.pil_image.tobytes(),
                            target_key=element_image_key,
                            content_type="application/png",
                        )
                        element.image.uri = Path(element_image_key)

                    except Exception as exc:
                        logging.error(
                            "Upload picture with hash %r raised error: %r",
                            element_hash,
                            exc,
                        )
                    picture_number += 1
