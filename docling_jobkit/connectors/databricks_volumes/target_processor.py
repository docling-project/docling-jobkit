import logging
from pathlib import Path
from typing import BinaryIO, Union

from pydantic import BaseModel

from docling_jobkit.connectors.databricks_volumes.helper import (
    ensure_directory,
    is_databricks_target_authentication_error,
    upload_document,
)
from docling_jobkit.connectors.databricks_volumes.models import (
    DatabricksVolumesCoordinates,
    TaskDatabricksVolumesTarget,
)
from docling_jobkit.connectors.errors import map_connector_authentication_errors
from docling_jobkit.connectors.target_processor import BaseTargetProcessor

_log = logging.getLogger(__name__)


def _is_authentication_error(exc: BaseException) -> bool:
    return is_databricks_target_authentication_error(exc)


class DatabricksVolumesTargetProcessor(BaseTargetProcessor):
    def __init__(self, coords: DatabricksVolumesCoordinates) -> None:
        super().__init__()
        self._coords = coords

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (TaskDatabricksVolumesTarget,)

    @map_connector_authentication_errors("Databricks Volumes", _is_authentication_error)
    def _initialize(self) -> None:
        ensure_directory(
            self._coords.workspace_host,
            self._coords.token.get_secret_value(),
            self._coords.volume_path,
        )
        _log.info(
            "Connected to Databricks Volumes: %s%s",
            self._coords.workspace_host,
            self._coords.volume_path,
        )

    def _finalize(self) -> None:
        pass

    def _build_full_path(self, target_filename: str) -> str:
        return f"{self._coords.volume_path}/{target_filename}"

    def build_artifact_uri(self, target_filename: str) -> str:
        return (
            f"databricks_volumes://{self._coords.workspace_host}"
            f"{self._build_full_path(target_filename)}"
        )

    @map_connector_authentication_errors("Databricks Volumes", _is_authentication_error)
    def upload_file(
        self,
        filename: Union[str, Path],
        target_filename: str,
        content_type: str,
    ) -> None:
        """Upload a local file from disk into a Databricks Volume."""
        full_path = self._build_full_path(target_filename)
        _log.info("Uploading to %s", self.build_artifact_uri(target_filename))
        with open(filename, "rb") as f:
            upload_document(
                self._coords.workspace_host,
                self._coords.token.get_secret_value(),
                full_path,
                f,
                content_type=content_type,
            )

    @map_connector_authentication_errors("Databricks Volumes", _is_authentication_error)
    def upload_object(
        self,
        obj: Union[str, bytes, BinaryIO],
        target_filename: str,
        content_type: str,
    ) -> None:
        """Upload an in-memory object into a Databricks Volume."""
        full_path = self._build_full_path(target_filename)
        _log.info("Uploading to %s", self.build_artifact_uri(target_filename))
        if isinstance(obj, str):
            obj = obj.encode("utf-8")
        upload_document(
            self._coords.workspace_host,
            self._coords.token.get_secret_value(),
            full_path,
            obj,
            content_type=content_type,
        )
