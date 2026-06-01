from io import BytesIO
from pathlib import Path
from typing import BinaryIO

from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.google_drive_coords import GoogleDriveCoordinates


class GoogleDriveTargetProcessor(BaseTargetProcessor):
    def __init__(self, coords: GoogleDriveCoordinates):
        super().__init__()
        self._coords = coords

    def _initialize(self):
        from docling_jobkit.connectors.google_drive_helper import get_service

        self._service = get_service(self._coords)

    def _finalize(self):
        return

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        """
        Upload a local file from disk to Google Drive.
        """
        from docling_jobkit.connectors.google_drive_helper import upload_file

        upload_file(
            service=self._service,
            filename=filename,
            target_filename=target_filename,
            content_type=content_type,
            coords=self._coords,
        )

    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
    ) -> None:
        """
        Upload an in-memory object (bytes or file-like) to Google Drive.
        """
        from docling_jobkit.connectors.google_drive_helper import upload_file

        if isinstance(obj, (bytes, bytearray)):
            body: BinaryIO = BytesIO(obj)
        elif isinstance(obj, str):
            body = BytesIO(obj.encode())
        else:
            body = obj  # assume it's a file-like object

        upload_file(
            service=self._service,
            file_stream=body,
            target_filename=target_filename,
            content_type=content_type,
            coords=self._coords,
        )
