from pathlib import Path
from typing import BinaryIO

from pydantic import BaseModel

from docling.datamodel.service.sources import (
    GoogleCloudStorageCoordinates,
)
from docling.datamodel.service.targets import GoogleCloudStorageTarget

from docling_jobkit.connectors.google_cloud_storage_helper import get_client
from docling_jobkit.connectors.target_processor import BaseTargetProcessor


class GoogleCloudStorageTargetProcessor(BaseTargetProcessor):
    def __init__(self, coords: GoogleCloudStorageCoordinates):
        super().__init__()
        self._coords = coords

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (GoogleCloudStorageTarget,)

    def _initialize(self):
        self._client = get_client(self._coords)

    def _finalize(self):
        self._client.close()

    def _build_full_key(self, target_filename: str) -> str:
        return (
            f"{self._coords.key_prefix}{target_filename}"
            if self._coords.key_prefix
            else target_filename
        )

    def build_artifact_uri(self, target_filename: str) -> str:
        return f"gs://{self._coords.bucket}/{self._build_full_key(target_filename)}"

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        """
        Upload a local file from disk to Google Cloud Storage.
        """
        blob = self._client.bucket(self._coords.bucket).blob(
            self._build_full_key(target_filename)
        )

        blob.upload_from_filename(str(filename), content_type=content_type)

    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
    ) -> None:
        """
        Upload an in-memory object (bytes or file-like) to Google Cloud Storage.
        """
        blob = self._client.bucket(self._coords.bucket).blob(
            self._build_full_key(target_filename)
        )

        if isinstance(obj, (str, bytes)):
            blob.upload_from_string(obj, content_type=content_type)
        else:
            blob.upload_from_file(obj, content_type=content_type)
