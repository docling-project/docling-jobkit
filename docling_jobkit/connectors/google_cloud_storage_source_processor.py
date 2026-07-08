from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from typing_extensions import override

from docling.datamodel.base_models import DocumentStream

if TYPE_CHECKING:
    from docling_jobkit.connectors.google_cloud_storage_helper import (
        GoogleCloudStorageFileIdentifier,
    )

from pydantic import BaseModel

from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    SourceDocumentRef,
)
from docling_jobkit.datamodel.google_cloud_storage_coords import (
    GoogleCloudStorageCoordinates,
)
from docling_jobkit.datamodel.task_sources import TaskGoogleCloudStorageSource


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
        # TODO: build the client via get_client(self._coords)
        raise NotImplementedError

    def _finalize(self):
        return

    def _fetch_documents(
        self, *, max_file_size: int | None = None
    ) -> Iterator[DocumentStream]:
        # TODO: list blobs under the prefix and yield one DocumentStream at a time
        raise NotImplementedError

    def _list_document_ids(self) -> Iterator[GoogleCloudStorageFileIdentifier]:
        # TODO:
        raise NotImplementedError

    def _fetch_document_by_id(
        self,
        identifier: GoogleCloudStorageFileIdentifier,
        *,
        max_file_size: int | None = None,
    ) -> DocumentStream:
        # TODO: enforce max_file_size, download blob into BytesIO, return DocumentStream
        raise NotImplementedError

    @override
    def _make_document_ref(
        self, info: GoogleCloudStorageFileIdentifier, source_index: int
    ) -> SourceDocumentRef[GoogleCloudStorageFileIdentifier]:
        # TODO: build source uri = f"gs://{self._coords.bucket}/{info.name}" for source lineage
        raise NotImplementedError
