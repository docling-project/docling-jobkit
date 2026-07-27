import logging
from typing import Iterator

from pydantic import BaseModel
from typing_extensions import override

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    SourceDocumentRef,
)
from docling_jobkit.datamodel.sharepoint_coords import (
    SharePointCoordinates,
    TaskSharePointSource,
)

_log = logging.getLogger(__name__)


class SharePointFileIdentifier(BaseModel): ...


class SharePointSourceProcessor(
    BaseSourceProcessor[SharePointCoordinates, SharePointFileIdentifier]
):
    def __init__(self, coords: SharePointCoordinates):
        super().__init__(coords)
        self._coords = coords

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (TaskSharePointSource,)

    def _initialize(self):
        pass

    def _finalize(self):
        pass

    def _list_document_ids(self) -> Iterator[SharePointFileIdentifier]:
        """List document IDs based on source configuration."""
        yield SharePointFileIdentifier()

    def _count_documents(self) -> int:
        """Count total documents by consuming the iterator."""
        return 0  # placeholder cause ruff keeps removing my "..." placeholder

    @override
    def _make_document_ref(
        self, identifier: SharePointFileIdentifier, source_index: int
    ) -> SourceDocumentRef[SharePointFileIdentifier]:
        return SourceDocumentRef(
            id=identifier,
            source_index=source_index,
            source_uri="placeholder",
            filename="placeholder",
        )

    def _fetch_documents(
        self, *, max_file_size: int | None = None
    ) -> Iterator[DocumentStream]:
        for doc_id in self._list_document_ids():
            yield self._fetch_document_by_id(doc_id, max_file_size=max_file_size)
