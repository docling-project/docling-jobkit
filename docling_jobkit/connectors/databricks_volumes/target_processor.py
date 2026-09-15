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
        # Directories already created during this open/close cycle. The
        # processor stays open for a whole batch, so this collapses the per-file
        # mkdir into one call per distinct output prefix (pdf/, json/, md/, ...).
        self._ensured_directories: set[str] = set()

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (TaskDatabricksVolumesTarget,)

    @map_connector_authentication_errors("Databricks Volumes", _is_authentication_error)
    def _initialize(self) -> None:
        self._ensured_directories.clear()
        self._ensure_directory(self._coords.volume_path)
        _log.info(
            "Connected to Databricks Volumes: %s%s",
            self._coords.workspace_host,
            self._coords.volume_path,
        )

    def _finalize(self) -> None:
        self._ensured_directories.clear()

    def _ensure_directory(self, directory: str) -> None:
        """Create ``directory`` (and any missing parents) once per open cycle.

        ``PUT /api/2.0/fs/directories`` is documented as ``mkdir -p`` and as
        idempotent, so a single call per prefix covers the whole chain.
        """
        if directory in self._ensured_directories:
            return
        ensure_directory(
            self._coords.workspace_host,
            self._coords.token.get_secret_value(),
            directory,
        )
        self._ensured_directories.add(directory)

    def _build_full_path(self, target_filename: str) -> str:
        return f"{self._coords.volume_path}/{target_filename}"

    def _prepare_upload_path(self, target_filename: str) -> str:
        """Resolve the absolute volume path and make sure its parent exists.

        ``target_filename`` is always nested (``json/doc.json``, ``md/doc.md``,
        optionally under an artifact root prefix), and the Files API upload
        endpoint does not document creating missing parent directories — unlike
        object stores, which have no real directory concept. So the parent is
        created before every write, the same way LocalPathTargetProcessor does.
        """
        full_path = self._build_full_path(target_filename)
        parent, _, _ = full_path.rpartition("/")
        if parent:
            self._ensure_directory(parent)
        return full_path

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
        full_path = self._prepare_upload_path(target_filename)
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
        full_path = self._prepare_upload_path(target_filename)
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
