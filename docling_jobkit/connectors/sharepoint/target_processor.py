from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Optional

from pydantic import BaseModel

from docling_jobkit.connectors.errors import (
    TargetConnectorConfigError,
    map_connector_authentication_errors,
)
from docling_jobkit.connectors.sharepoint.helper import (
    is_sharepoint_authentication_error,
)
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.sharepoint_coords import (
    SharePointTargetCoordinates,
    TaskSharePointTarget,
)

if TYPE_CHECKING:
    from office365.onedrive.driveitems.driveItem import DriveItem


class SharePointTargetProcessor(BaseTargetProcessor):
    def __init__(self, coords: SharePointTargetCoordinates):
        super().__init__()
        self._coords = coords
        self._ensured_folders: set[Optional[str]] = set()

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
            SharePointDriveNotFoundError,
            check_connection,
            get_client,
            resolve_drive,
        )

        self._ensured_folders = set()
        self._client = get_client(self._coords)
        try:
            self._drive = resolve_drive(self._client, self._coords)
        except SharePointDriveNotFoundError as exc:
            # A drive that does not resolve is a bad *target* config; the shared
            # helper stays neutral so the source connector can classify the same
            # failure as a source-side policy error instead.
            raise TargetConnectorConfigError(str(exc)) from exc

        # Same as the source connector: fail at open time on bad credentials rather
        # than on the first artifact of the first document.
        check_connection(self._client, self._drive)

    def _finalize(self):
        self._ensured_folders = set()

    def _resolve_target(self, target_filename: str) -> tuple["DriveItem", str]:
        """Resolve *target_filename* into its (destination folder, leaf name).

        ``get_or_create_folder`` costs one Graph round-trip per path segment, and the
        results processor calls ``upload_*`` once per artifact — including once per
        page image — so without memoisation a single document re-walks the same handful
        of folders dozens of times. Folders are only ever created here, never removed,
        so "this path exists" holds for as long as the processor is open.

        What is memoised is the *path*, not the handle: building a handle costs no
        request, while holding one leaks — ``DriveItem.upload()`` appends to its
        ``children`` collection, so a shared handle would retain an entry per uploaded
        artifact for the whole batch.
        """
        from docling_jobkit.connectors.sharepoint.helper import (
            folder_handle,
            get_or_create_folder,
            resolve_destination,
        )

        dest_folder, name = resolve_destination(
            self._coords.folder_path, target_filename
        )
        if dest_folder not in self._ensured_folders:
            get_or_create_folder(self._drive, dest_folder)
            self._ensured_folders.add(dest_folder)

        return folder_handle(self._drive, dest_folder), name

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

        folder, name = self._resolve_target(target_filename)
        upload_file(folder, filename, name)

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

        folder, name = self._resolve_target(target_filename)
        upload_object(folder, obj, name)
