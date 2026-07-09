from pathlib import Path
from typing import BinaryIO

from pydantic import BaseModel

from docling_jobkit.connectors.google_cloud_storage_helper import get_client
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.google_cloud_storage_coords import (
    GoogleCloudStorageCoordinates,
)
from docling_jobkit.datamodel.task_targets import GoogleCloudStorageTarget


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

    def build_artifact_uri(self, target_filename: str) -> str:
        # TODO: return f"gs://{bucket}/{key_prefix}{target_filename}" for source lineage
        raise NotImplementedError

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        """
        Upload a local file from disk to Google Cloud Storage.
        """
        # TODO: call upload_file from helper
        raise NotImplementedError

    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
    ) -> None:
        """
        Upload an in-memory object (bytes or file-like) to Google Cloud Storage.
        """
        # TODO: normalize obj to a file-like stream, then call upload_file from helper
        raise NotImplementedError
