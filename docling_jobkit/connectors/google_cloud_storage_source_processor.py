from __future__ import annotations

import logging
from io import BytesIO
from typing import Iterator

from google.api_core.exceptions import GoogleAPICallError
from pydantic import BaseModel
from typing_extensions import override

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.google_cloud_storage_helper import (
    GoogleCloudStorageFileIdentifier,
    get_client,
)
from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    SourceDocumentRef,
)
from docling_jobkit.convert.materialization import (
    SourceLimitExceededError,
    normalize_max_file_size,
)
from docling_jobkit.datamodel.google_cloud_storage_coords import (
    GoogleCloudStorageCoordinates,
)
from docling_jobkit.datamodel.task_sources import TaskGoogleCloudStorageSource

_log = logging.getLogger(__name__)


class GoogleCloudStorageSourceProcessor(
    BaseSourceProcessor[
        GoogleCloudStorageCoordinates, "GoogleCloudStorageFileIdentifier"
    ]
):
    def __init__(self, coords: GoogleCloudStorageCoordinates):
        super().__init__(coords)
        self._coords = coords

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (TaskGoogleCloudStorageSource,)

    def _initialize(self):
        self._client = get_client(self._coords)

    def _finalize(self):
        self._client.close()

    def _list_document_ids(self) -> Iterator[GoogleCloudStorageFileIdentifier]:
        yielded = 0
        max_num = self._coords.max_num_elements
        for blob in self._client.list_blobs(
            self._coords.bucket,
            prefix=self._coords.key_prefix,
        ):
            if blob.name.endswith("/"):
                continue  # skipping folder-placeholder / directory-marker objects
            if max_num is not None and yielded >= max_num:
                return
            yielded += 1
            yield GoogleCloudStorageFileIdentifier(
                name=blob.name, size=blob.size, last_modified=blob.updated
            )

        if yielded == 0:
            _log.warning(
                "No objects matched gs://%s/%s - nothing to process.",
                self._coords.bucket,
                self._coords.key_prefix,
            )

    @override
    def _make_document_ref(
        self, identifier: GoogleCloudStorageFileIdentifier, source_index: int
    ) -> SourceDocumentRef[GoogleCloudStorageFileIdentifier]:
        name = identifier.name
        return SourceDocumentRef(
            id=identifier,
            source_index=source_index,
            source_uri=f"gs://{self._coords.bucket}/{name}",
            filename=name,
        )

    # ----------------- Document fetch -----------------

    def _fetch_document_by_id(
        self,
        identifier: GoogleCloudStorageFileIdentifier,
        *,
        max_file_size: int | None = None,
    ) -> DocumentStream:
        limit = normalize_max_file_size(max_file_size)
        if limit is not None and identifier.size > limit:
            raise SourceLimitExceededError(
                f"Source '{identifier.name}' exceeds max_file_size={limit} bytes"
            )

        buffer = BytesIO()
        try:
            self._client.bucket(self._coords.bucket).blob(
                identifier.name
            ).download_to_file(buffer)
        except GoogleAPICallError:
            _log.warning(
                "Failed to download gs://%s/%s",
                self._coords.bucket,
                identifier.name,
                exc_info=True,
            )
            raise
        buffer.seek(0)
        return DocumentStream(name=identifier.name, stream=buffer)

    def _fetch_documents(
        self, *, max_file_size: int | None = None
    ) -> Iterator[DocumentStream]:
        for key in self._list_document_ids():
            yield self._fetch_document_by_id(key, max_file_size=max_file_size)
