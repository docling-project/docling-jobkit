import logging
from typing import Iterator, Optional

from pydantic import BaseModel
from typing_extensions import override

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.databricks_volumes.helper import (
    download_document_from_url,
    iter_directory,
)
from docling_jobkit.connectors.databricks_volumes.models import (
    DatabricksVolumesCoordinates,
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
    BaseSourceProcessor[DatabricksVolumesCoordinates, DatabricksVolumeFileIdentifier]
):
    def __init__(self, coords: DatabricksVolumesCoordinates):
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

    def _list_document_ids(self) -> Iterator[DatabricksVolumeFileIdentifier]:
        max_elements = self._coords.max_num_elements
        yielded = 0
        stack = [self._coords.volume_path]
        while stack:
            current = stack.pop()
            for entry in iter_directory(
                self._coords.workspace_host,
                self._coords.token.get_secret_value(),
                current,
            ):
                if max_elements is not None and yielded >= max_elements:
                    return
                if entry.get("is_directory"):
                    stack.append(entry["path"])
                    continue
                yielded += 1
                yield DatabricksVolumeFileIdentifier(
                    path=entry["path"],
                    name=entry["name"],
                    size=entry.get("file_size", 0),
                    last_modified=entry.get("last_modified"),
                )

    def _count_documents(self) -> int:
        max_elements = self._coords.max_num_elements
        count = 0
        for _ in self._list_document_ids():
            count += 1
        return min(count, max_elements) if max_elements is not None else count

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

        url = f"https://{self._coords.workspace_host}/api/2.0/fs/files{identifier.path}"
        buffer = download_document_from_url(
            url,
            self._coords.token.get_secret_value(),
            expected_host=self._coords.workspace_host,
            max_file_size=max_file_size,
        )
        return DocumentStream(name=identifier.name, stream=buffer)

    def _fetch_documents(
        self, *, max_file_size: Optional[int] = None
    ) -> Iterator[DocumentStream]:
        for file_id in self._list_document_ids():
            yield self._fetch_document_by_id(file_id, max_file_size=max_file_size)
