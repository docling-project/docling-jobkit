from pathlib import Path
from typing import BinaryIO

from docling.datamodel.service.sources import S3Coordinates

from docling_jobkit.connectors.artifact_paths import ArtifactType
from docling_jobkit.connectors.s3_helper import get_s3_connection
from docling_jobkit.connectors.s3_upload_support import (
    upload_s3_file,
    upload_s3_object,
)
from docling_jobkit.connectors.target_processor import BaseTargetProcessor


class S3TargetProcessor(BaseTargetProcessor):
    def __init__(self, coords: S3Coordinates):
        super().__init__()
        self._coords = coords

    def _initialize(self):
        self._client, self._resource = get_s3_connection(self._coords)

    def _finalize(self):
        self._client.close()

    def _build_full_key(self, target_filename: str) -> str:
        return (
            f"{self._coords.key_prefix}{target_filename}"
            if self._coords.key_prefix
            else target_filename
        )

    def build_artifact_uri(self, target_filename: str) -> str:
        return f"s3://{self._coords.bucket}/{self._build_full_key(target_filename)}"

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
        *,
        artifact_type: ArtifactType | None = None,
        source_index: int | None = None,
        source_uri: str | None = None,
    ) -> None:
        """
        Upload a local file from disk into the S3 bucket.
        """
        full_key = self._build_full_key(target_filename)
        upload_s3_file(
            self._client,
            bucket=self._coords.bucket,
            key=full_key,
            filename=filename,
            content_type=content_type,
        )

    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
        *,
        artifact_type: ArtifactType | None = None,
        source_index: int | None = None,
        source_uri: str | None = None,
    ) -> None:
        """
        Upload an in-memory object (bytes or file-like) into the S3 bucket.
        """
        full_key = self._build_full_key(target_filename)
        upload_s3_object(
            self._client,
            bucket=self._coords.bucket,
            key=full_key,
            obj=obj,
            content_type=content_type,
        )
