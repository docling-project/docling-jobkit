import logging
from datetime import datetime
from itertools import islice
from typing import Iterator

from pydantic import BaseModel
from typing_extensions import override

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.errors import (
    SourceConnectorPolicyError,
    map_connector_authentication_errors,
)
from docling_jobkit.connectors.sharepoint.helper import (
    is_sharepoint_authentication_error,
    is_sharepoint_unavailable_error,
)
from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    SourceDocumentRef,
)
from docling_jobkit.convert.materialization import (
    SourceLimitExceededError,
    normalize_max_file_size,
)
from docling_jobkit.datamodel.sharepoint_coords import (
    SharePointSourceCoordinates,
    TaskSharePointSource,
)

_log = logging.getLogger(__name__)


class SharePointFileIdentifier(BaseModel):
    id: str
    name: str
    size: int
    last_modified: datetime | None = None


class SharePointSourceProcessor(
    BaseSourceProcessor[SharePointSourceCoordinates, SharePointFileIdentifier]
):
    def __init__(self, coords: SharePointSourceCoordinates):
        super().__init__(coords)
        self._coords = coords

    @classmethod
    def check_dependencies(cls) -> None:
        from office365.graph_client import GraphClient  # noqa: F401

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (TaskSharePointSource,)

    @map_connector_authentication_errors(
        "SharePoint",
        is_sharepoint_authentication_error,
        source=True,
        source_kind="sharepoint",
        is_unavailable_error=is_sharepoint_unavailable_error,
    )
    def _initialize(self):
        from docling_jobkit.connectors.sharepoint.helper import (
            SharePointDriveNotFoundError,
            check_connection,
            get_client,
            resolve_drive,
        )

        self._client = get_client(self._coords)
        try:
            self._drive = resolve_drive(self._client, self._coords)
        except SharePointDriveNotFoundError as exc:
            # The shared helper stays neutral so the target connector can classify
            # the same failure as a target config error instead.
            raise SourceConnectorPolicyError(
                str(exc), source_kind="sharepoint"
            ) from exc
        check_connection(self._client, self._drive)

    def _finalize(self):
        return

    @map_connector_authentication_errors(
        "SharePoint",
        is_sharepoint_authentication_error,
        source=True,
        source_kind="sharepoint",
        is_unavailable_error=is_sharepoint_unavailable_error,
    )
    def _list_document_ids(self) -> Iterator[SharePointFileIdentifier]:
        """List document IDs based on source configuration."""
        from docling_jobkit.connectors.sharepoint.helper import (
            list_folder_items,
            list_items_by_id,
        )

        max_num = self._coords.max_num_elements
        if self._coords.file_ids:
            metas: Iterator[dict] = list_items_by_id(
                self._client, self._drive, self._coords.file_ids
            )
            if max_num is not None:
                metas = islice(metas, max_num)
        else:
            # Pushed into the walk rather than applied afterwards, so a capped run
            # stops enumerating instead of listing the whole library and truncating.
            metas = list_folder_items(
                self._drive, self._coords.folder_path, limit=max_num
            )

        for meta in metas:
            yield SharePointFileIdentifier(**meta)

    @map_connector_authentication_errors(
        "SharePoint",
        is_sharepoint_authentication_error,
        source=True,
        source_kind="sharepoint",
        is_unavailable_error=is_sharepoint_unavailable_error,
    )
    def _fetch_document_by_id(
        self, identifier: SharePointFileIdentifier, *, max_file_size: int | None = None
    ) -> DocumentStream:
        from docling_jobkit.connectors.sharepoint.helper import download_item

        limit = normalize_max_file_size(max_file_size)
        if limit is not None and identifier.size > limit:
            raise SourceLimitExceededError(
                f"Document '{identifier.name}' ({identifier.size} bytes) "
                f"exceeds max_file_size={limit} bytes"
            )

        buffer = download_item(self._client, self._drive, identifier.id)

        return DocumentStream(name=identifier.name, stream=buffer)

    @override
    def _make_document_ref(
        self, identifier: SharePointFileIdentifier, source_index: int
    ) -> SourceDocumentRef[SharePointFileIdentifier]:
        return SourceDocumentRef(
            id=identifier,
            source_index=source_index,
            source_uri=f"sharepoint://{identifier.id}",  # NOTE: Think more deeply about lineage
            filename=identifier.name,
        )

    def _fetch_documents(
        self, *, max_file_size: int | None = None
    ) -> Iterator[DocumentStream]:
        for doc_id in self._list_document_ids():
            yield self._fetch_document_by_id(doc_id, max_file_size=max_file_size)
