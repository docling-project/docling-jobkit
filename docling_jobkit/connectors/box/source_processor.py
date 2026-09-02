from datetime import datetime
from itertools import islice
from typing import Iterator

from pydantic import BaseModel
from typing_extensions import override

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.box.helper import (
    is_box_authentication_error,
    is_box_unavailable_error,
)
from docling_jobkit.connectors.box.models import BoxSource
from docling_jobkit.connectors.errors import map_connector_authentication_errors
from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    SourceDocumentRef,
)
from docling_jobkit.convert.materialization import (
    SourceLimitExceededError,
    normalize_max_file_size,
)


class BoxFileIdentifier(BaseModel):
    id: str
    name: str
    size: int
    modified_at: datetime | None = None


class BoxSourceProcessor(BaseSourceProcessor[BoxSource, BoxFileIdentifier]):
    def __init__(self, source: BoxSource):
        super().__init__(source)
        self._config = source

    @classmethod
    def check_dependencies(cls) -> None:
        import box_sdk_gen  # noqa: F401

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (BoxSource,)

    @map_connector_authentication_errors(
        "Box",
        is_box_authentication_error,
        source=True,
        source_kind="box",
        is_unavailable_error=is_box_unavailable_error,
    )
    def _initialize(self):
        from docling_jobkit.connectors.box.helper import check_connection, get_client

        self._client = get_client(self._config)
        check_connection(self._client)

    def _finalize(self):
        return

    @map_connector_authentication_errors(
        "Box",
        is_box_authentication_error,
        source=True,
        source_kind="box",
        is_unavailable_error=is_box_unavailable_error,
    )
    def _list_document_ids(self) -> Iterator[BoxFileIdentifier]:
        """List document IDs based on source configuration."""
        from docling_jobkit.connectors.box.helper import (
            fetch_file_by_id,
            list_folder_items,
        )

        max_num = self._config.max_num_elements
        if self._config.file_ids:
            metas: Iterator[dict] = (
                fetch_file_by_id(self._client, file_id)
                for file_id in self._config.file_ids
            )
            if max_num is not None:
                metas = islice(metas, max_num)
        else:
            # Pushed into the walk rather than applied afterwards, so a capped run
            # stops enumerating instead of listing the whole tree and truncating.
            metas = list_folder_items(
                self._client, self._config.folder_id, limit=max_num
            )

        for meta in metas:
            yield BoxFileIdentifier(**meta)

    @map_connector_authentication_errors(
        "Box",
        is_box_authentication_error,
        source=True,
        source_kind="box",
        is_unavailable_error=is_box_unavailable_error,
    )
    def _count_documents(self) -> int:
        return sum(1 for _ in self._list_document_ids())

    @map_connector_authentication_errors(
        "Box",
        is_box_authentication_error,
        source=True,
        source_kind="box",
        is_unavailable_error=is_box_unavailable_error,
    )
    def _fetch_document_by_id(
        self, identifier: BoxFileIdentifier, *, max_file_size: int | None = None
    ) -> DocumentStream:
        from docling_jobkit.connectors.box.helper import download_file

        limit = normalize_max_file_size(max_file_size)
        if limit is not None and identifier.size > limit:
            raise SourceLimitExceededError(
                f"Document '{identifier.name}' ({identifier.size} bytes) "
                f"exceeds max_file_size={limit} bytes"
            )

        buffer = download_file(self._client, identifier.id)

        return DocumentStream(name=identifier.name, stream=buffer)

    @override
    def _make_document_ref(
        self, identifier: BoxFileIdentifier, source_index: int
    ) -> SourceDocumentRef[BoxFileIdentifier]:
        return SourceDocumentRef(
            id=identifier,
            source_index=source_index,
            source_uri=f"box://{identifier.id}",
            filename=identifier.name,
        )

    def _fetch_documents(
        self, *, max_file_size: int | None = None
    ) -> Iterator[DocumentStream]:
        for doc_id in self._list_document_ids():
            yield self._fetch_document_by_id(doc_id, max_file_size=max_file_size)
