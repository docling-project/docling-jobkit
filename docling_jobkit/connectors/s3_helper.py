import logging
from pathlib import Path
from urllib.parse import urlunsplit

from boto3.session import Session
from botocore.config import Config
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.paginator import ListObjectsV2Paginator
from mypy_boto3_s3.service_resource import S3ServiceResource

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

    client: S3Client = session.client(
        "s3",
        endpoint_url=endpoint,
        verify=coords.verify_ssl,
        aws_access_key_id=coords.access_key.get_secret_value(),
        aws_secret_access_key=coords.secret_key.get_secret_value(),
        config=config,
    )

    resource: S3ServiceResource = session.resource(
        "s3",
        endpoint_url=endpoint,
        verify=coords.verify_ssl,
        aws_access_key_id=coords.access_key.get_secret_value(),
        aws_secret_access_key=coords.secret_key.get_secret_value(),
        config=config,
    )

    return client, resource


def count_s3_objects(paginator: ListObjectsV2Paginator, bucket_name: str, prefix: str):
    response_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    count_obj = 0
    for page in response_iterator:
        if page.get("Contents"):
            count_obj += sum(1 for _ in page["Contents"])

    return count_obj


def get_keys_s3_objects_as_set(
    s3_resource: S3ServiceResource, bucket_name: str, prefix: str
) -> set[str]:
    bucket = s3_resource.Bucket(bucket_name)
    folder_objects = list(bucket.objects.filter(Prefix=prefix))
    files_on_s3 = set()
    for file in folder_objects:
        files_on_s3.add(file.key)
    return files_on_s3


def strip_prefix_postfix(source_set: set[str], prefix: str = "", extension: str = ""):
    output = set()
    for key in source_set:
        output.add(key.replace(extension, "").replace(prefix, ""))
    return output


def generate_batch_keys(
    source_keys: list[str],
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


# TODO: raised default expiration_time raised due to presign being generated
# in compute batches with new convert manager. This probably is not be enough
def generate_presign_url(
    client: S3Client,
    object_key: str,
    bucket: str,
    expiration_time: int = 21600,
) -> str | None:
    try:
        return client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket, "Key": object_key},
            ExpiresIn=expiration_time,
        )
    except Exception as e:
        logging.error("Generation of presigned url failed", exc_info=e)
        return None


def get_source_files(
    s3_source_client: S3Client,
    s3_source_resource: S3ServiceResource,
    s3_coords: S3Coordinates,
):
    source_paginator = s3_source_client.get_paginator("list_objects_v2")

    key_prefix = (
        s3_coords.key_prefix
        if s3_coords.key_prefix.endswith("/")
        else s3_coords.key_prefix + "/"
    )
    if key_prefix == "/":
        key_prefix = ""
    # Check that source is not empty
    source_count = count_s3_objects(source_paginator, s3_coords.bucket, key_prefix)
    if source_count == 0:
        logging.error("No documents to process in the source s3 coordinates.")
    return get_keys_s3_objects_as_set(s3_source_resource, s3_coords.bucket, key_prefix)


def check_target_has_source_converted(
    coords: S3Coordinates,
    source_objects_list: list[str],
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

        # At this point we should be targeting keys in the json "folder"
        target_short_key_list = []
        for item in existing_target_objects:
            clean_name = str(Path(item).stem)
            target_short_key_list.append(clean_name)

        filtered_source_keys = []
        logging.debug("List of source keys:")
        for key in source_objects_list:
            logging.debug("Object key: {}".format(key))
            # This covers the case when source docs have "folder" hierarchy in the key
            # we don't preserve key part between prefix and "file", this part of key is not added as prefix for target
            clean_key = str(Path(key).stem)
            if clean_key not in target_short_key_list:
                filtered_source_keys.append(key)

        logging.debug("Total keys: {}".format(len(source_objects_list)))
        logging.debug("Filtered keys to process: {}".format(len(filtered_source_keys)))
    else:
        filtered_source_keys = source_objects_list

    return filtered_source_keys
