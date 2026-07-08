from io import BytesIO
from pathlib import Path
from typing import BinaryIO

from azure.storage.blob import BlobClient, ContentSettings


def upload_azure_blob_file(
    blob_client: BlobClient,
    filename: str | Path,
    content_type: str,
    metadata: dict[str, str] | None = None,
) -> None:
    content_settings = ContentSettings(content_type=content_type)

    with open(filename, "rb") as data:
        blob_client.upload_blob(
            data,
            overwrite=True,
            content_settings=content_settings,
            metadata=metadata,
        )


def upload_azure_blob_object(
    blob_client: BlobClient,
    obj: str | bytes | BinaryIO,
    content_type: str,
    metadata: dict[str, str] | None = None,
) -> None:
    if isinstance(obj, str):
        data: BinaryIO = BytesIO(obj.encode())
    elif isinstance(obj, bytes):
        data = BytesIO(obj)
    else:
        data = obj

    content_settings = ContentSettings(content_type=content_type)

    blob_client.upload_blob(
        data,
        overwrite=True,
        content_settings=content_settings,
        metadata=metadata,
    )
