import logging
from pathlib import Path
from typing import BinaryIO

from pydantic import BaseModel

from docling_jobkit.connectors.azure_blob_helper import get_azure_blob_connection
from docling_jobkit.connectors.azure_blob_upload_support import (
    upload_azure_blob_file,
    upload_azure_blob_object,
)
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.azure_blob_coords import AzureBlobCoordinates
from docling_jobkit.datamodel.task_targets import AzureBlobTarget

_log = logging.getLogger(__name__)


class AzureBlobTargetProcessor(BaseTargetProcessor):
    def __init__(self, coords: AzureBlobCoordinates):
        super().__init__()
        self._coords = coords

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (AzureBlobTarget,)

    def _initialize(self):
        self._service_client, self._container_client = get_azure_blob_connection(
            self._coords
        )

    def _finalize(self):
        self._service_client.close()

    def _build_full_blob_name(self, target_filename: str) -> str:
        return (
            f"{self._coords.blob_prefix}{target_filename}"
            if self._coords.blob_prefix
            else target_filename
        )

    def build_artifact_uri(self, target_filename: str) -> str:
        full_name = self._build_full_blob_name(target_filename)
        return (
            f"azure://{self._coords.account_name}/{self._coords.container}/{full_name}"
        )

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        """Upload a local file from disk into Azure Blob Storage."""
        full_name = self._build_full_blob_name(target_filename)
        blob_client = self._container_client.get_blob_client(full_name)
        _log.info(
            "Uploading to azure://%s/%s/%s",
            self._coords.account_name,
            self._coords.container,
            full_name,
        )
        upload_azure_blob_file(
            blob_client,
            filename=filename,
            content_type=content_type,
        )

    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
    ) -> None:
        """Upload an in-memory object into Azure Blob Storage."""
        full_name = self._build_full_blob_name(target_filename)
        blob_client = self._container_client.get_blob_client(full_name)
        _log.info(
            "Uploading to azure://%s/%s/%s",
            self._coords.account_name,
            self._coords.container,
            full_name,
        )
        upload_azure_blob_object(
            blob_client,
            obj=obj,
            content_type=content_type,
        )
