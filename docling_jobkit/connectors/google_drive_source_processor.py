from io import BytesIO
from typing import Iterator

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.google_drive_helper import GoogleDriveFileIdentifier
from docling_jobkit.connectors.source_processor import BaseSourceProcessor
from docling_jobkit.datamodel.google_drive_coords import GoogleDriveCoordinates


class GoogleDriveSourceProcessor(BaseSourceProcessor[GoogleDriveFileIdentifier]):
    def __init__(self, coords: GoogleDriveCoordinates):
        super().__init__()
        self._coords = coords

    def _initialize(self):
        from docling_jobkit.connectors.google_drive_helper import get_service

        self._service = get_service(self._coords)

    def _finalize(self):
        return

    def _fetch_documents(self) -> Iterator[DocumentStream]:
        from docling_jobkit.connectors.google_drive_helper import (
            download_file,
            get_source_files_infos,
        )

        files_infos = get_source_files_infos(
            service=self._service,
            coords=self._coords,
        )

        # download and yield one document at the time
        for file_info in files_infos:
            buffer = BytesIO()
            download_file(
                service=self._service,
                file_info=file_info,
                file_stream=buffer,
            )
            buffer.seek(0)

            yield DocumentStream(
                name=file_info["name"],
                stream=buffer,
            )

    def _list_document_ids(self) -> Iterator[GoogleDriveFileIdentifier]:
        from docling_jobkit.connectors.google_drive_helper import get_source_files_infos

        for info in get_source_files_infos(self._service, self._coords):
            yield GoogleDriveFileIdentifier(
                id=info["id"],
                name=info["name"],
                mimeType=info["mimeType"],
                path=info["path"],
            )

    def _fetch_document_by_id(self, info: GoogleDriveFileIdentifier) -> DocumentStream:
        from docling_jobkit.connectors.google_drive_helper import download_file

        buffer = BytesIO()

        download_file(
            service=self._service,
            file_info=info,
            file_stream=buffer,
        )
        buffer.seek(0)

        return DocumentStream(
            name=info["name"],
            stream=buffer,
        )
