from pathlib import Path
from typing import BinaryIO

from pydantic import BaseModel

from docling_jobkit.connectors.errors import map_connector_authentication_errors
from docling_jobkit.connectors.sharepoint.helper import (
    is_sharepoint_authentication_error,
)
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.sharepoint_coords import (
    SharePointTargetCoordinates,
    TaskSharePointTarget,
)


class SharePointTargetProcessor(BaseTargetProcessor):
    def __init__(self, coords: SharePointTargetCoordinates):
        super().__init__()
        self._coords = coords

    @classmethod
    def check_dependencies(cls) -> None:
        from office365.graph_client import GraphClient  # noqa: F401

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (TaskSharePointTarget,)

    @map_connector_authentication_errors(
        "SharePoint", is_sharepoint_authentication_error
    )
    def _initialize(self):
        from docling_jobkit.connectors.sharepoint.helper import (
            check_connection,
            get_client,
            resolve_drive,
        )

        self._client = get_client(self._coords)
        self._drive = resolve_drive(self._client, self._coords)
        check_connection(
            self._client, self._drive
        )  # NOTE: do we not need check_connection here

    def _finalize(self):
        return

    @map_connector_authentication_errors(
        "SharePoint", is_sharepoint_authentication_error
    )
    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        """
        Upload a local file from disk to SharePoint (or OneDrive).
        """
        from docling_jobkit.connectors.sharepoint.helper import upload_file

        upload_file(self._drive, filename, self._coords.folder_path, target_filename)

    @map_connector_authentication_errors(
        "SharePoint", is_sharepoint_authentication_error
    )
    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
    ) -> None:
        """Upload an in-memory object (bytes or file-like) to SharePoint (or OneDrive)."""
        from docling_jobkit.connectors.sharepoint.helper import upload_object

        upload_object(self._drive, obj, self._coords.folder_path, target_filename)
