from io import BytesIO
from pathlib import Path
from typing import BinaryIO

from docling_jobkit.connectors.s3_helper import get_s3_connection
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.s3_coords import S3Coordinates


class S3TargetProcessor(BaseTargetProcessor):
    def __init__(self, coords: S3Coordinates):
        super().__init__()
        self._coords = coords

    def _initialize(self):
        self._client, self._resource = get_s3_connection(self._coords)

    def _finalize(self):
        self._client.close()

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        """
        Upload a local file from disk into the S3 bucket.
        """
        full_key = (
            f"{self._coords.key_prefix}{target_filename}"
            if self._coords.key_prefix
            else target_filename
        )
        self._client.upload_file(
            Filename=filename,
            Bucket=self._coords.bucket,
            Key=full_key,
            ExtraArgs={"ContentType": content_type},
        )

    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
    ) -> None:
        """
        Upload an in-memory object (bytes or file-like) into the S3 bucket.
        """
        full_key = (
            f"{self._coords.key_prefix}{target_filename}"
            if self._coords.key_prefix
            else target_filename
        )
        if isinstance(obj, (bytes, bytearray)):
            body: BinaryIO = BytesIO(obj)
        elif isinstance(obj, str):
            body = BytesIO(obj.encode())
        else:
            body = obj  # assume it's a file-like object

        self._client.upload_fileobj(
            Fileobj=body,
            Bucket=self._coords.bucket,
            Key=full_key,
            ExtraArgs={"ContentType": content_type},
        )
