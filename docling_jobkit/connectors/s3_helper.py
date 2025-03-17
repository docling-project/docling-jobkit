from botocore.client import BaseClient
from boto3.resources.base import ServiceResource
from boto3.session import Session
from pydantic import BaseModel


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


def upload_to_s3(s3_client, bucket, file, target_key, content_type):
    return put_object(
        client=s3_client,
        bucket=bucket,
        object_key=target_key,
        file=file,
        content_type=content_type,
    )