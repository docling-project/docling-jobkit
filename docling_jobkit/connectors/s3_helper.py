import json
import logging
import os
import tempfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunsplit

import httpx
import pandas as pd
from boto3.resources.base import ServiceResource
from boto3.session import Session
from botocore.client import BaseClient
from botocore.config import Config
from botocore.paginate import Paginator
from pandas import DataFrame

from docling.backend.docling_parse_v4_backend import DoclingParseV4DocumentBackend
from docling.backend.pdf_backend import PdfDocumentBackend
from docling.datamodel.base_models import ConversionStatus, InputFormat
from docling.datamodel.document import ConversionResult
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.utils.utils import create_hash
from docling_core.types.doc.base import ImageRefMode
from docling_core.types.doc.document import (
    DocItem,
    DoclingDocument,
    PageItem,
    PictureClassificationData,
    PictureItem,
)
from docling_core.types.doc.labels import DocItemLabel

from docling_jobkit.datamodel.s3_coords import S3Coordinates

logging.basicConfig(level=logging.INFO)

# Set the maximum file size of parquet to 500MB
MAX_PARQUET_FILE_SIZE = 500 * 1024 * 1024

classifier_labels = [
    "bar_chart",
    "bar_code",
    "chemistry_markush_structure",
    "chemistry_molecular_structure",
    "flow_chart",
    "icon",
    "line_chart",
    "logo",
    "map",
    "other",
    "pie_chart",
    "qr_code",
    "remote_sensing",
    "screenshot",
    "signature",
    "stamp",
]


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


def strip_prefix_postfix(source_set: set, prefix: str = "", extension: str = ""):
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
) -> str | None:
    try:
        return s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": s3_source_bucket, "Key": source_key},
            ExpiresIn=expiration_time,
        )
    except Exception as e:
        logging.error("Generation of presigned url failed", exc_info=e)
        return None


def get_source_files(
    s3_source_client: BaseClient,
    s3_source_resource: ServiceResource,
    s3_coords: S3Coordinates,
):
    source_paginator = s3_source_client.get_paginator("list_objects_v2")

    key_prefix = (
        s3_coords.key_prefix
        if s3_coords.key_prefix.endswith("/")
        else s3_coords.key_prefix + "/"
    )
    # Check that source is not empty
    source_count = count_s3_objects(source_paginator, s3_coords.bucket, key_prefix)
    if source_count == 0:
        logging.error("No documents to process in the source s3 coordinates.")
    return get_keys_s3_objects_as_set(s3_source_resource, s3_coords.bucket, key_prefix)


def check_target_has_source_converted(
    coords: S3Coordinates,
    source_objects_list: list,
    s3_source_prefix: str,
):
    s3_target_client, s3_target_resource = get_s3_connection(coords)
    target_paginator = s3_target_client.get_paginator("list_objects_v2")

    converted_prefix = (
        coords.key_prefix + "json/"
        if coords.key_prefix.endswith("/")
        else coords.key_prefix + "/json/"
    )

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
    client: BaseClient,
    bucket: str,
    object_key: str,
    file: str,
    content_type: str | None = None,
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
    except Exception as e:
        logging.error("Put s3 object failed", exc_info=e)
        return False
    return True


def upload_file(
    client: BaseClient,
    bucket: str,
    object_key: str,
    file_name: str | Path,
    content_type: str | None = None,
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
    except Exception as e:
        logging.error("Upload file to s3 failed", exc_info=e)
        return False
    return True


class DoclingConvert:
    def __init__(
        self,
        source_s3_coords: S3Coordinates,
        target_s3_coords: S3Coordinates,
        pipeline_options: PdfPipelineOptions,
        allowed_formats: list[str] | None = None,
        to_formats: list[str] | None = None,
        backend: type[PdfDocumentBackend] | None = None,
        export_parquet_file: bool = True,
    ):
        self.source_coords = source_s3_coords
        self.source_s3_client, _ = get_s3_connection(source_s3_coords)

        self.target_coords = target_s3_coords
        self.target_s3_client, _ = get_s3_connection(target_s3_coords)

        self.converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=pipeline_options,
                    backend=backend if backend else DoclingParseV4DocumentBackend,
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
        self.export_parquet_file = export_parquet_file

        self.max_file_size = 1073741824  # TODO: be set from ENV

    def convert_documents(self, object_keys):
        pd_d = DataFrame()  # DataFrame to append parquet info
        try:
            for i, key in enumerate(object_keys):
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
                with tempfile.TemporaryDirectory() as tmpdirname:
                    temp_dir = Path(tmpdirname)
                    try:  # download file to be able to upload it later to s3
                        file_name = temp_dir / os.path.basename(parsed.path)
                        r = httpx.get(url, timeout=30)
                        content_length = r.headers.get("Content-Length", None)

                        if (
                            not content_length
                            or int(content_length) < self.max_file_size
                        ):
                            with open(file_name, "wb") as writer:
                                writer.write(r.content)
                    except Exception as exc:
                        logging.error(
                            "An error occurred downloading file.", exc_info=exc
                        )
                        yield f"{parsed.path} - FAILURE"
                        continue
                    try:
                        conv_res: ConversionResult = self.converter.convert(file_name)
                    except Exception as e:
                        logging.error(
                            "An error occurred while converting document.", exc_info=e
                        )
                        yield f"{parsed.path} - FAILURE"
                        continue
                    if conv_res.status == ConversionStatus.SUCCESS:
                        s3_target_prefix = self.target_coords.key_prefix
                        doc_hash = conv_res.input.document_hash
                        name_without_ext = os.path.splitext(file_name.name)[0]
                        logging.debug(f"Converted {doc_hash} now saving results")

                        if os.path.exists(conv_res.input.file):
                            self.upload_file_to_s3(
                                file=conv_res.input.file,
                                target_key=f"{s3_target_prefix}/pdf/{name_without_ext}.pdf",
                                content_type="application/pdf",
                            )

                        if self.export_page_images:
                            # Export pages images:
                            self.upload_page_images(
                                conv_res.document.pages,
                                s3_target_prefix,
                                conv_res.input.document_hash,
                            )

                        if self.export_images:
                            # Export pictures
                            self.upload_pictures(
                                conv_res.document,
                                s3_target_prefix,
                                conv_res.input.document_hash,
                            )

                        if self.to_formats is None or (
                            self.to_formats and "json" in self.to_formats
                        ):
                            # Export Docling document format to JSON:
                            target_key = (
                                f"{s3_target_prefix}/json/{name_without_ext}.json"
                            )
                            temp_json_file = temp_dir / f"{name_without_ext}.json"

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
                            target_key = f"{s3_target_prefix}/doctags/{name_without_ext}.doctags.txt"

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
                            target_key = f"{s3_target_prefix}/md/{name_without_ext}.md"

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
                            target_key = (
                                f"{s3_target_prefix}/html/{name_without_ext}.html"
                            )
                            temp_html_file = temp_dir / f"{name_without_ext}.html"

                            conv_res.document.save_as_html(temp_html_file)
                            self.upload_file_to_s3(
                                file=temp_html_file,
                                target_key=target_key,
                                content_type="text/html",
                            )

                        if self.to_formats is None or (
                            self.to_formats and "text" in self.to_formats
                        ):
                            # Export Docling document format to text:
                            target_key = (
                                f"{s3_target_prefix}/txt/{name_without_ext}.txt"
                            )

                            data = conv_res.document.export_to_text()
                            self.upload_object_to_s3(
                                file=data,
                                target_key=target_key,
                                content_type="text/plain",
                            )
                        if self.export_parquet_file:
                            logging.info("saving document info in dataframe...")
                            # Save Docling parquet info into DataFrame:
                            pd_d = self.document_to_dataframe(
                                conv_res=conv_res,
                                pd_dataframe=pd_d,
                                filename=name_without_ext,
                            )

                        yield f"{doc_hash} - SUCCESS"

                    elif conv_res.status == ConversionStatus.PARTIAL_SUCCESS:
                        yield f"{conv_res.input.file} - PARTIAL_SUCCESS"
                    else:
                        yield f"{conv_res.input.file} - FAILURE"

        finally:
            if self.export_parquet_file and not pd_d.empty:
                self.upload_parquet_file(pd_d, self.target_coords.key_prefix)

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

    def upload_page_images(
        self,
        pages: dict[int, PageItem],
        s3_target_prefix: str,
        doc_hash: str,
    ):
        for page_no, page in pages.items():
            try:
                if page.image and page.image.pil_image:
                    page_hash = create_hash(f"{doc_hash}_page_no_{page_no}")
                    page_dpi = page.image.dpi
                    page_path_suffix = f"/pages/{page_hash}_{page_dpi}.png"
                    byteIO = BytesIO()
                    page.image.pil_image.save(byteIO, format="PNG")
                    self.upload_object_to_s3(
                        file=byteIO.getvalue(),
                        target_key=f"{s3_target_prefix}" + page_path_suffix,
                        content_type="application/png",
                    )
                    page.image.uri = Path(".." + page_path_suffix)

            except Exception as exc:
                logging.error(
                    "Upload image of page with hash %r raised error: %r",
                    page_hash,
                    exc,
                )

    def upload_pictures(
        self,
        document: DoclingDocument,
        s3_target_prefix: str,
        doc_hash: str,
    ):
        picture_number = 0
        for element, _level in document.iterate_items():
            if isinstance(element, PictureItem):
                if element.image and element.image.pil_image:
                    try:
                        element_hash = create_hash(f"{doc_hash}_img_{picture_number}")
                        element_dpi = element.image.dpi
                        element_path_suffix = (
                            f"/images/{element_hash}_{element_dpi}.png"
                        )
                        byteIO = BytesIO()
                        element.image.pil_image.save(byteIO, format="PNG")
                        self.upload_object_to_s3(
                            file=byteIO.getvalue(),
                            target_key=f"{s3_target_prefix}" + element_path_suffix,
                            content_type="application/png",
                        )
                        element.image.uri = Path(".." + element_path_suffix)

                    except Exception as exc:
                        logging.error(
                            "Upload picture with hash %r raised error: %r",
                            element_hash,
                            exc,
                        )
                    picture_number += 1

    def document_to_dataframe(
        self, conv_res: ConversionResult, pd_dataframe: DataFrame, filename: str
    ) -> DataFrame:
        result_table: list[dict[str, Any]] = []

        page_images = []
        for page_no, page in conv_res.document.pages.items():
            if page.image is not None and page.image.pil_image is not None:
                page_images.append(page.image.pil_image.tobytes())

        # Count the number of picture of each type
        num_formulas = 0
        num_codes = 0
        picture_classes = dict.fromkeys(classifier_labels, 0)
        for element, _level in conv_res.document.iterate_items():
            if isinstance(element, PictureItem):
                element.image = None  # reset images
                classification = next(
                    (
                        annot
                        for annot in element.annotations
                        if isinstance(annot, PictureClassificationData)
                    ),
                    None,
                )
                if classification is None or len(classification.predicted_classes) == 0:
                    continue

                predicted_class = classification.predicted_classes[0].class_name
                if predicted_class in picture_classes:
                    picture_classes[predicted_class] += 1

            elif isinstance(element, DocItem):
                if element.label == DocItemLabel.FORMULA:
                    num_formulas += 1
                elif element.label == DocItemLabel.CODE:
                    num_codes += 1

        num_pages = len(conv_res.document.pages)
        num_tables = len(conv_res.document.tables)
        num_elements = len(conv_res.document.texts)
        num_pictures = len(conv_res.document.pictures)

        # All features
        features = [
            num_pages,
            num_elements,
            num_tables,
            num_pictures,
            num_formulas,
            num_codes,
            *picture_classes.values(),
        ]

        doc_hash = (
            conv_res.document.origin.binary_hash
            if conv_res.document.origin
            else "unknown_hash"
        )
        doc_json = json.dumps(conv_res.document.export_to_dict())

        pdf_byte_array: bytearray | None = None
        if os.path.exists(conv_res.input.file):
            with open(conv_res.input.file, "rb") as file:
                pdf_byte_array = bytearray(file.read())

        result_table.append(
            {
                "filename": filename,
                "pdf": pdf_byte_array,
                "doc_hash": doc_hash,
                "document": doc_json,
                "page_images": page_images,
                "features": features,
                "doctags": str.encode(conv_res.document.export_to_document_tokens()),
            }
        )

        pd_df = pd.json_normalize(result_table)
        pd_df = pd_dataframe._append(pd_df)

        return pd_df

    def upload_parquet_file(self, pd_dataframe: DataFrame, s3_target_prefix: str):
        # Variables to track the file writing process
        file_index = 0
        current_file_size = 0
        current_df = pd.DataFrame()
        # Manifest dictionary
        manifest = {}
        # Current time
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        while len(pd_dataframe) > 0:
            # Get a chunk of the DataFrame that fits within the file size limit
            chunk_size = min(
                len(pd_dataframe), MAX_PARQUET_FILE_SIZE // (current_file_size + 1)
            )

            # If the chunk size is 0, it means the current file size has exceeded the limit
            if chunk_size == 0:
                with tempfile.NamedTemporaryFile(
                    suffix=f".parquet_{file_index}"
                ) as temp_file:
                    pd_dataframe.to_parquet(temp_file.name)
                    current_file_size += os.path.getsize(temp_file.name)
                    file_index += 1

                    parquet_file_name = f"{timestamp}_{file_index}.parquet"
                    target_key = f"{s3_target_prefix}/parquet/{parquet_file_name}"
                    self.upload_file_to_s3(
                        file=temp_file.name,
                        target_key=target_key,
                        content_type="application/vnd.apache.parquet",
                    )

                    manifest[f"{parquet_file_name}"] = {
                        "filename": pd_dataframe["filename"].tolist(),
                        "doc_hash": pd_dataframe["doc_hash"].tolist(),
                        "row_number": 3,
                        "timestamp": timestamp,
                    }

                pd_dataframe = pd.DataFrame()
            else:
                # Get the current chunk of the DataFrame
                current_df = pd_dataframe.iloc[:chunk_size]
                pd_dataframe = pd_dataframe.iloc[chunk_size:]

                with tempfile.NamedTemporaryFile(
                    suffix=f".parquet_{file_index}"
                ) as temp_file:
                    current_df.to_parquet(temp_file.name)
                    current_file_size += os.path.getsize(temp_file.name)
                    file_index += 1

                    parquet_file_name = f"{timestamp}_{file_index}.parquet"
                    target_key = f"{s3_target_prefix}/parquet/{parquet_file_name}"
                    self.upload_file_to_s3(
                        file=temp_file.name,
                        target_key=target_key,
                        content_type="application/vnd.apache.parquet",
                    )

                    manifest[f"{parquet_file_name}"] = {
                        "filenames": current_df["filename"].tolist(),
                        "doc_hashes": current_df["doc_hash"].tolist(),
                        "row_number": 3,
                        "timestamp": timestamp,
                    }

        logging.info(f"Total parquet files uploaded: {file_index}")

        # Export manifest file:
        with tempfile.NamedTemporaryFile(suffix=".json") as temp_file_json:
            with open(temp_file_json.name, "w") as file:
                json.dump(manifest, file, indent=4)
            self.upload_file_to_s3(
                file=temp_file_json.name,
                target_key=f"{s3_target_prefix}/manifest/{timestamp}.json",
                content_type="application/json",
            )
