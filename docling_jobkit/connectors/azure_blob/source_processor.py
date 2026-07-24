import logging
from datetime import datetime
from io import BytesIO
from typing import Iterator

from pydantic import BaseModel
from typing_extensions import override

from docling.datamodel.service.requests import AzureBlobSourceRequest
from docling.datamodel.service.sources import AzureBlobCoordinates
from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.errors import map_connector_authentication_errors
from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    SourceDocumentRef,
)
from docling_jobkit.convert.materialization import (
    SourceLimitExceededError,
    normalize_max_file_size,
)

_log = logging.getLogger(__name__)


def _is_authentication_error(exc: BaseException) -> bool:
    from docling_jobkit.connectors.azure_blob.helper import (
        is_azure_blob_authentication_error,
    )

    return is_azure_blob_authentication_error(exc)


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
        return (AzureBlobSourceRequest,)

    @map_connector_authentication_errors(
        "Azure Blob Storage", _is_authentication_error, source=True
    )
    def _initialize(self):
        from docling_jobkit.connectors.azure_blob.helper import (
            get_azure_blob_connection,
        )

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

    @map_connector_authentication_errors(
        "Azure Blob Storage", _is_authentication_error, source=True
    )
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

    @map_connector_authentication_errors(
        "Azure Blob Storage", _is_authentication_error, source=True
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

    @map_connector_authentication_errors(
        "Azure Blob Storage", _is_authentication_error, source=True
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
        from azure.core.exceptions import ResourceNotFoundError, ServiceRequestError

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

    @override
    def fetch_by_locator(
        self, locator: str, *, max_file_size: int | None = None
    ) -> DocumentStream:
        prefix = self._coords.blob_prefix or ""
        name = f"{prefix.rstrip('/')}/{locator.lstrip('/')}" if prefix else locator
        return self._fetch_document_by_id(
            AzureBlobFileIdentifier(name=name, size=0), max_file_size=max_file_size
        )
