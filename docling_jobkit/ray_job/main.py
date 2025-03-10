import argparse
import json
import os
from typing import Annotated, Literal, Optional
from urllib.parse import urlparse, urlunsplit

import ray
from boto3.resources.base import ServiceResource
from boto3.session import Session
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.paginate import Paginator
from pydantic import BaseModel, Field, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict, SettingsError
from ray._raylet import ObjectRefGenerator
from typing_extensions import Self

from docling.backend.docling_parse_backend import DoclingParseDocumentBackend
from docling.datamodel.base_models import ConversionStatus, InputFormat
from docling.datamodel.document import ConversionResult
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
from docling.document_converter import DocumentConverter, PdfFormatOption


class ConvertDocumentsOptions(BaseModel):
    do_ocr: Annotated[
        bool,
        Field(
            description=(
                "If enabled, the bitmap content will be processed using OCR. "
                "Boolean. Optional, defaults to true"
            ),
            examples=[True],
        ),
    ] = True

    ocr_kind: Annotated[
        Literal["easyocr", "tesseract", "rapidocr"],
        Field(
            description=(
                "The OCR engine to use. String. "
                "Allowed values: easyocr, tesseract, rapidocr. "
                "Optional, defaults to easyocr."
            ),
            examples=["easyocr"],
        ),
    ] = "easyocr"

    table_mode: Annotated[
        TableFormerMode,
        Field(
            description=(
                "Mode to use for table structure, String. "
                "Allowed values: fast, accurate. "
                "Optional, defaults to fast."
            ),
            examples=["fast"],
        ),
    ] = TableFormerMode.FAST.value

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

    @field_validator("table_mode", mode="before")
    def check_table_structure_mode(cls, v, info: ValidationInfo):
        if isinstance(v, str):
            return TableFormerMode(v)
        else:
            return v


class RequestSettings(BaseSettings):
    model_config = SettingsConfigDict()

    source_access_key: str = Field(validation_alias="S3_SOURCE_ACCESS_KEY")
    source_secret_key: str = Field(validation_alias="S3_SOURCE_SECRET_KEY")
    source_endpoints: str = Field(validation_alias="S3_SOURCE_ENDPOINTS")
    source_bucket: str = Field(validation_alias="S3_SOURCE_BUCKET")
    source_prefix: str = Field(validation_alias="S3_SOURCE_PREFIX")
    source_ssl: bool = Field(validation_alias="S3_SOURCE_SSL")
    target_access_key: str = Field(validation_alias="S3_TARGET_ACCESS_KEY")
    target_secret_key: str = Field(validation_alias="S3_TARGET_SECRET_KEY")
    target_endpoints: str = Field(validation_alias="S3_TARGET_ENDPOINTS")
    target_bucket: str = Field(validation_alias="S3_TARGET_BUCKET")
    target_prefix: str = Field(validation_alias="S3_TARGET_PREFIX")
    target_ssl: bool = Field(validation_alias="S3_TARGET_SSL")
    omp_num_threads: int = Field(validation_alias="OMP_NUM_THREADS")
    batch_size: int = Field(validation_alias="BATCH_SIZE")

    do_ocr: bool = Field(True, validation_alias="SETTINGS_DO_OCR")
    ocr_kind: str = Field("easyocr" , validation_alias="SETTINGS_OCR_KIND")
    do_table_structure: bool = Field(True , validation_alias="SETTINGS_DO_TABLE_STRUCTURE")
    table_structure_mode: str = Field("fast" , validation_alias="SETTINGS_TABLE_STRUCTURE_MODE")
    generate_page_images: bool = Field(True , validation_alias="SETTINGS_GENERATE_PAGE_IMAGES")

    @field_validator("batch_size")
    def check_batch_size(cls, v, info: ValidationInfo):
        if v <= 0:
            raise SettingsError("batch_size have to be higher than zero")
        else:
            return v

    @model_validator(mode="after")
    def check_source_target_is_not_same(self) -> Self:
        if (self.source_endpoints == self.target_endpoints) and (
            self.source_bucket == self.target_bucket) and (
            self.source_prefix == self.target_prefix):
            raise SettingsError("s3 source and target are the same")
        else:
            return self


settings = RequestSettings()

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
    path="/"
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
    count_obj=0
    for page in response_iterator:
        if page.get("Contents"):
            count_obj += sum(1 for _ in page["Contents"])

    return count_obj


def get_keys_s3_objects_as_set(s3_resource: ServiceResource, bucket_name: str, prefix: str):
    bucket = s3_resource.Bucket(bucket_name)
    folder_objects = list(bucket.objects.filter(Prefix=prefix))
    files_on_s3 = set()
    for file in folder_objects:
        files_on_s3.add(file.key)
    return files_on_s3


def strip_prefix_postfix(source_set, prefix = "", extension = ""):
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
                Params={
                    "Bucket": settings.source_bucket,
                    "Key": key
                },
                ExpiresIn=3600
            )
        except ClientError as e:
            print(e)
        sub_array.append(url)
        counter += 1
        if counter == settings.batch_size or (idx + 1) == array_lenght:
            presigned_urls.append(sub_array)
            sub_array = []
            counter = 0

    return presigned_urls


def get_source_files(s3_source_client: BaseClient, s3_source_resource: ServiceResource):
    source_paginator = s3_source_client.get_paginator("list_objects_v2")

    # Check that source is not empty
    source_count = count_s3_objects(source_paginator, settings.source_bucket, settings.source_prefix + "/")
    if source_count == 0:
        print("s3 source is empty")
        ray.shutdown()
    return get_keys_s3_objects_as_set(s3_source_resource, settings.source_bucket, settings.source_prefix)


def check_target_has_source_converted(coords: S3Coordinates, source_objects_list: list):
    s3_target_client, s3_target_resource = get_s3_connection(coords)
    target_paginator = s3_target_client.get_paginator("list_objects_v2")

    converted_prefix = settings.target_prefix + "/json/"
    target_count = count_s3_objects(target_paginator, settings.target_bucket, converted_prefix)
    print("Target contains json objects: ",target_count)
    if target_count != 0:
        print("Target contains objects, checking content...")

        # Collect target keys for iterative conversion
        existing_target_objects = get_keys_s3_objects_as_set(s3_target_resource, settings.target_bucket, converted_prefix)

        # Filter-out objects that are already processed
        target_short_key_list = strip_prefix_postfix(existing_target_objects, prefix=converted_prefix, extension=".json")
        filtered_source_keys = []
        print("List of source keys:")
        for key in source_objects_list:
            print(key)
            clean_key = key.replace(".pdf", "").replace(settings.source_prefix + "/", "")
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
)->bool:
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


@ray.remote(max_concurrency=settings.omp_num_threads)
class DoclingConvert:
    def __init__(self, s3_coords: S3Coordinates, conversion_options: ConvertDocumentsOptions):
        self.coords = s3_coords
        self.s3_client, _ = get_s3_connection(s3_coords)

        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = conversion_options.do_ocr
        pipeline_options.ocr_options.kind = conversion_options.ocr_kind
        pipeline_options.do_table_structure = conversion_options.do_table_structure
        pipeline_options.table_structure_options.mode = conversion_options.table_mode
        pipeline_options.generate_page_images = conversion_options.include_images

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
                target_key = f"{settings.target_prefix}/json/{doc_filename}.json"
                data = json.dumps(conv_res.document.export_to_dict())
                self.upload_to_s3(
                    file=data,
                    target_key=target_key,
                    content_type="application/json",
                )
                # Export Docling document format to doctags:
                target_key = f"{settings.target_prefix}/doctags/{doc_filename}.doctags.txt"
                data = conv_res.document.export_to_document_tokens()
                self.upload_to_s3(
                    file=data,
                    target_key=target_key,
                    content_type="text/plain",
                )
                # Export Docling document format to markdown:
                target_key = f"{settings.target_prefix}/md/{doc_filename}.md"
                data = conv_res.document.export_to_markdown()
                self.upload_to_s3(
                    file=data,
                    target_key=target_key,
                    content_type="text/markdown",
                )
                # Export Docling document format to text:
                target_key = f"{settings.target_prefix}/txt/{doc_filename}.txt"
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

    conversion_options = ConvertDocumentsOptions(
        do_ocr=settings.do_ocr,
        ocr_kind=settings.ocr_kind,
        table_mode=settings.table_structure_mode,
        do_table_structure=settings.do_table_structure,
        include_images=settings.generate_page_images,
    )

    # get source keys
    s3_coords_source = S3Coordinates(
        endpoint=settings.source_endpoints,
        verify_ssl=settings.source_ssl,
        access_key=settings.source_access_key,
        secret_key=settings.source_secret_key,
        bucket=settings.source_bucket,
        key_prefix=settings.source_prefix,
    )
    s3_source_client, s3_source_resource = get_s3_connection(s3_coords_source)
    source_objects_list = get_source_files(s3_source_client, s3_source_resource)

    # filter source keys
    s3_coords_target = S3Coordinates(
        endpoint=settings.target_endpoints,
        verify_ssl=settings.target_ssl,
        access_key=settings.target_access_key,
        secret_key=settings.target_secret_key,
        bucket=settings.target_bucket,
        key_prefix=settings.target_prefix,
    )

    filtered_source_keys = check_target_has_source_converted(s3_coords_target, source_objects_list)

    # Generate pre-signed urls
    presigned_urls = generate_presigns_url(s3_source_client, filtered_source_keys)

    # Init ray actor
    c = DoclingConvert.remote(s3_coords_target, conversion_options)
    # Send payload to ray
    db_object_ref = ray.put(presigned_urls)
    # Launch tasks
    object_references = [
        c.convert_document.remote(
            index, db_object_ref
        ) for index in range(len(presigned_urls))
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
    parser = argparse.ArgumentParser(
        description="Basic docling ray app"
    )

    args = parser.parse_args()
    main(args)
