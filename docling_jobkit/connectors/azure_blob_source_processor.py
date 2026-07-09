import logging
from datetime import datetime
from io import BytesIO
from typing import Iterator

from azure.core.exceptions import ResourceNotFoundError, ServiceRequestError
from pydantic import BaseModel
from typing_extensions import override

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.azure_blob_helper import get_azure_blob_connection
from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    SourceDocumentRef,
)
from docling_jobkit.convert.materialization import (
    SourceLimitExceededError,
    normalize_max_file_size,
)
from docling_jobkit.datamodel.azure_blob_coords import AzureBlobCoordinates
from docling_jobkit.datamodel.task_sources import TaskAzureBlobSource

_log = logging.getLogger(__name__)


class AzureBlobFileIdentifier(BaseModel):
    name: str  # blob name ~= s3 key
    size: int
    last_modified: datetime | None = None


class AzureBlobSourceProcessor(
    BaseSourceProcessor[AzureBlobCoordinates, AzureBlobFileIdentifier]
):
    def __init__(self, coords: AzureBlobCoordinates):
        super().__init__(coords)
        self._coords = coords

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        # Note: Using TaskAzureBlobSource (not AzureBlobSourceRequest) to follow
        # the naming pattern for connectors defined in docling-jobkit (Google Drive, Local Path)
        # S3 has S3SourceRequest defined in docling core library. No azure equivalent.

        return (TaskAzureBlobSource,)

    def _initialize(self):
        self._service_client, self._container_client = get_azure_blob_connection(
            self._coords
        )
        _log.info(
            "Connected to Azure Blob Storage: azure://%s/%s",
            self._coords.account_name,
            self._coords.container,
        )

    def _finalize(self):
        self._service_client.close()

    def _list_document_ids(self) -> Iterator[AzureBlobFileIdentifier]:
        yielded = 0
        max_num_elements = self._coords.max_num_elements

        for blob in self._container_client.list_blobs(
            name_starts_with=self._coords.blob_prefix or None
        ):
            if max_num_elements is not None and yielded >= max_num_elements:
                return

            yielded += 1
            yield AzureBlobFileIdentifier(
                name=blob.name,
                size=blob.size or 0,
                last_modified=blob.last_modified,
            )

    def _count_documents(self) -> int:
        total = 0
        max_num_elements = self._coords.max_num_elements

        for blob in self._container_client.list_blobs(
            name_starts_with=self._coords.blob_prefix or None
        ):
            if max_num_elements is not None and total >= max_num_elements:
                return max_num_elements
            total += 1

        return total

    @override
    def _make_document_ref(
        self, identifier: AzureBlobFileIdentifier, source_index: int
    ) -> SourceDocumentRef[AzureBlobFileIdentifier]:
        return SourceDocumentRef(
            id=identifier,
            source_index=source_index,
            source_uri=f"azure://{self._coords.account_name}/{self._coords.container}/{identifier.name}",
            filename=identifier.name,
        )

    def _fetch_document_by_id(
        self,
        identifier: AzureBlobFileIdentifier,
        *,
        max_file_size: int | None = None,
    ) -> DocumentStream:
        limit = normalize_max_file_size(max_file_size)
        if limit is not None and identifier.size > limit:
            raise SourceLimitExceededError(
                f"Source '{identifier.name}' exceeds max_file_size={limit} bytes"
            )

        _log.info(
            "Downloading from azure://%s/%s/%s",
            self._coords.account_name,
            self._coords.container,
            identifier.name,
        )
        buffer = BytesIO()
        try:
            blob_client = self._container_client.get_blob_client(identifier.name)
            download_stream = blob_client.download_blob()
            download_stream.readinto(buffer)
        except (ResourceNotFoundError, ServiceRequestError):
            _log.warning(
                "Failed to download azure://%s/%s/%s",
                self._coords.account_name,
                self._coords.container,
                identifier.name,
                exc_info=True,
            )
            raise

        buffer.seek(0)
        return DocumentStream(name=identifier.name, stream=buffer)

    def _fetch_documents(
        self, *, max_file_size: int | None = None
    ) -> Iterator[DocumentStream]:
        for blob_id in self._list_document_ids():
            yield self._fetch_document_by_id(
                blob_id,
                max_file_size=max_file_size,
            )
