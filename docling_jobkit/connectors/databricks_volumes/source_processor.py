import logging
from typing import Iterator, Optional

from pydantic import BaseModel
from typing_extensions import override

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.databricks_volumes.helper import (
    download_document,
    iter_directory,
)
from docling_jobkit.connectors.databricks_volumes.models import (
    DatabricksVolumesSourceCoordinates,
    TaskDatabricksVolumesSource,
)
from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    SourceDocumentRef,
)
from docling_jobkit.convert.materialization import (
    SourceLimitExceededError,
    normalize_max_file_size,
)

_log = logging.getLogger(__name__)


class DatabricksVolumeFileIdentifier(BaseModel):
    path: str
    name: str
    size: int
    last_modified: Optional[int] = None


class DatabricksVolumesSourceProcessor(
    BaseSourceProcessor[
        DatabricksVolumesSourceCoordinates, DatabricksVolumeFileIdentifier
    ]
):
    def __init__(self, coords: DatabricksVolumesSourceCoordinates):
        super().__init__(coords)
        self._coords = coords

    @classmethod
    def get_config_types(cls) -> tuple:
        return (TaskDatabricksVolumesSource,)

    def _initialize(self) -> None:
        next(
            iter_directory(
                self._coords.workspace_host,
                self._coords.token.get_secret_value(),
                self._coords.volume_path,
            ),
            None,
        )
        _log.info(
            "Connected to Databricks Volumes: %s%s",
            self._coords.workspace_host,
            self._coords.volume_path,
        )

    def _finalize(self) -> None:
        pass

    def _iter_entries(self) -> Iterator[dict]:
        """Walk the volume subtree depth-first, yielding file entries only.

        The stack holds directory paths, not files, so peak memory is bounded by
        the directory count rather than the file count, and only one directory
        listing generator is live at a time.
        """
        stack = [self._coords.volume_path]
        while stack:
            current = stack.pop()
            for entry in iter_directory(
                self._coords.workspace_host,
                self._coords.token.get_secret_value(),
                current,
            ):
                path = entry.get("path")
                if not path:
                    # A listing entry we cannot address is not actionable; skip
                    # it rather than dying on a KeyError that would surface to
                    # the client as an internal error.
                    _log.warning(
                        "Databricks Volumes: listing entry without a path under "
                        "%s, skipping",
                        current,
                    )
                    continue
                if entry.get("is_directory"):
                    stack.append(path)
                    continue
                yield entry

    def _list_document_ids(self) -> Iterator[DatabricksVolumeFileIdentifier]:
        max_elements = self._coords.max_num_elements
        yielded = 0
        for entry in self._iter_entries():
            if max_elements is not None and yielded >= max_elements:
                return
            path = entry["path"]
            yielded += 1
            yield DatabricksVolumeFileIdentifier(
                path=path,
                # `name` is documented but not worth a KeyError if absent —
                # the trailing path segment is the same value.
                name=entry.get("name") or path.rsplit("/", 1)[-1],
                size=entry.get("file_size", 0),
                last_modified=entry.get("last_modified"),
            )

    def _count_documents(self) -> int:
        # Counts raw listing entries instead of going through
        # _list_document_ids(), which would build and immediately discard one
        # pydantic model per file just to increment a counter.
        max_elements = self._coords.max_num_elements
        count = 0
        for _ in self._iter_entries():
            count += 1
            if max_elements is not None and count >= max_elements:
                return max_elements
        return count

    @override
    def _make_document_ref(
        self, identifier: DatabricksVolumeFileIdentifier, source_index: int
    ) -> SourceDocumentRef[DatabricksVolumeFileIdentifier]:
        return SourceDocumentRef(
            id=identifier,
            source_index=source_index,
            source_uri=(
                f"databricks_volumes://{self._coords.workspace_host}{identifier.path}"
            ),
            filename=identifier.name,
        )

    def _fetch_document_by_id(
        self,
        identifier: DatabricksVolumeFileIdentifier,
        *,
        max_file_size: Optional[int] = None,
    ) -> DocumentStream:
        limit = normalize_max_file_size(max_file_size)
        if limit is not None and identifier.size > limit:
            raise SourceLimitExceededError(
                f"Source '{identifier.path}' exceeds max_file_size={limit} bytes"
            )

        _log.info(
            "Downloading databricks_volumes://%s%s",
            self._coords.workspace_host,
            identifier.path,
        )

        buffer = download_document(
            self._coords.workspace_host,
            self._coords.token.get_secret_value(),
            identifier.path,
            max_file_size=max_file_size,
        )
        return DocumentStream(name=identifier.name, stream=buffer)

    def _fetch_documents(
        self, *, max_file_size: Optional[int] = None
    ) -> Iterator[DocumentStream]:
        for file_id in self._list_document_ids():
            yield self._fetch_document_by_id(file_id, max_file_size=max_file_size)
