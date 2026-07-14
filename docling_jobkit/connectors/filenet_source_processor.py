import logging
from typing import Iterator

from pydantic import BaseModel

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.filenet_helper import (
    check_connection,
    download_document,
    get_document_metadata,
    get_filenet_auth_header,
    list_folder_documents,
    list_repository_documents,
)
from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
)
from docling_jobkit.convert.materialization import (
    SourceLimitExceededError,
    normalize_max_file_size,
)
from docling_jobkit.datamodel.filenet_coords import FileNetCoordinates
from docling_jobkit.datamodel.task_sources import TaskFileNetSource

_log = logging.getLogger(__name__)


class FileNetFileIdentifier(BaseModel):
    id: str
    name: str
    size: int
    mime_type: str | None = None
    download_url: str


class FileNetSourceProcessor(
    BaseSourceProcessor[FileNetCoordinates, FileNetFileIdentifier]
):
    def __init__(self, coords: FileNetCoordinates):
        super().__init__(coords)
        self._coords = coords
        self._auth_header: str = ""
        self._graphql_url: str = ""

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (TaskFileNetSource,)

    def _initialize(self):
        self._auth_header = get_filenet_auth_header(
            self._coords.username,
            self._coords.api_key.get_secret_value(),
        )
        self._graphql_url = f"{self._coords.base_url.rstrip('/')}/graphql"

        check_connection(self._coords, self._auth_header)
        _log.info(
            "Connected to FileNet: %s (repository: %s)",
            self._coords.base_url,
            self._coords.repository_id,
        )

    def _finalize(self):
        pass

    def _list_document_ids(self) -> Iterator[FileNetFileIdentifier]:
        """List document IDs based on source configuration."""
        yielded = 0
        max_elements = self._coords.max_num_elements

        # Single document mode
        if self._coords.document_id:
            metadata = get_document_metadata(
                self._coords,
                self._auth_header,
                self._coords.document_id,
            )
            yield FileNetFileIdentifier(
                id=metadata["id"],
                name=metadata["name"],
                size=metadata["contentSize"],
                mime_type=metadata.get("mimeType"),
                download_url=metadata["downloadUrl"],
            )
            return

        # Folder mode
        if self._coords.folder_id:
            docs = list_folder_documents(
                self._coords,
                self._auth_header,
                self._coords.folder_id,
            )
        else:
            # Repository mode
            docs = list_repository_documents(
                self._coords,
                self._auth_header,
            )

        for doc in docs:
            if max_elements is not None and yielded >= max_elements:
                return

            metadata = get_document_metadata(
                self._coords,
                self._auth_header,
                doc["id"],
            )

            yielded += 1
            yield FileNetFileIdentifier(
                id=metadata["id"],
                name=metadata["name"],
                size=metadata["contentSize"],
                mime_type=metadata.get("mimeType"),
                download_url=metadata["downloadUrl"],
            )

    def _count_documents(self) -> int:
        """Count total documents by consuming the iterator."""
        max_elements = self._coords.max_num_elements

        if self._coords.folder_id:
            docs = list_folder_documents(
                self._coords, self._auth_header, self._coords.folder_id
            )
        else:
            docs = list_repository_documents(
                self._coords,
                self._auth_header,
            )

        if max_elements is None:
            return sum(1 for _ in docs)

        # Count up to max_elements
        count = 0
        for _ in docs:
            count += 1
            if count >= max_elements:
                break
        return count

    def _fetch_document_by_id(
        self,
        identifier: FileNetFileIdentifier,
        *,
        max_file_size: int | None = None,
    ) -> DocumentStream:
        """Download a document by its identifier."""
        limit = normalize_max_file_size(max_file_size)
        if limit is not None and identifier.size > limit:
            raise SourceLimitExceededError(
                f"Document '{identifier.name}' ({identifier.size} bytes) "
                f"exceeds max_file_size={limit} bytes"
            )

        _log.info(
            "Downloading document %s from FileNet repository %s",
            identifier.name,
            self._coords.repository_id,
        )

        buffer = download_document(
            self._coords.base_url,
            self._auth_header,
            identifier.download_url,
        )

        return DocumentStream(name=identifier.name, stream=buffer)

    def _fetch_documents(
        self, *, max_file_size: int | None = None
    ) -> Iterator[DocumentStream]:
        for doc_id in self._list_document_ids():
            yield self._fetch_document_by_id(doc_id, max_file_size=max_file_size)
