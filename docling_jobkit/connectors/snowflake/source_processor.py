import logging
from io import BytesIO
from pathlib import Path
from typing import Iterator

from pydantic import BaseModel
from typing_extensions import override

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.errors import map_connector_authentication_errors
from docling_jobkit.connectors.snowflake.helper import (
    download_stage_file,
    get_snowflake_connection,
    is_snowflake_authentication_error,
    is_snowflake_unavailable_error,
    list_stage_files,
    relative_path_from_list_name,
    with_retry,
)
from docling_jobkit.connectors.snowflake.models import (
    SnowflakeCoordinates,
    TaskSnowflakeSource,
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

_map_snowflake_source_errors = map_connector_authentication_errors(
    "Snowflake",
    is_snowflake_authentication_error,
    source=True,
    source_kind="snowflake",
    is_unavailable_error=is_snowflake_unavailable_error,
    unavailable_message="Snowflake source could not be reached.",
)


class SnowflakeFileIdentifier(BaseModel):
    relative_path: str  # path within the stage, e.g. "subfolder/file.pdf"
    size: int
    last_modified: str | None = None


class SnowflakeSourceProcessor(
    BaseSourceProcessor[SnowflakeCoordinates, SnowflakeFileIdentifier]
):
    def __init__(self, coords: SnowflakeCoordinates):
        super().__init__(coords)
        self._coords = coords

    @classmethod
    def check_dependencies(cls) -> None:
        import snowflake.snowpark  # noqa: F401

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (TaskSnowflakeSource,)

    @_map_snowflake_source_errors
    def _initialize(self):
        self._session = get_snowflake_connection(self._coords)
        _log.info(
            "Connected to Snowflake stage: %s.%s.%s",
            self._coords.database,
            self._coords.db_schema,
            self._coords.stage,
        )

    def _finalize(self):
        self._session.close()

    @_map_snowflake_source_errors
    def _list_document_ids(self) -> Iterator[SnowflakeFileIdentifier]:
        for row in list_stage_files(self._session, self._coords):
            size = row.get("size")
            last_modified = row.get("last_modified")
            yield SnowflakeFileIdentifier(
                relative_path=relative_path_from_list_name(str(row["name"])),
                size=int(str(size)) if size is not None else 0,
                last_modified=str(last_modified) if last_modified else None,
            )

    @_map_snowflake_source_errors
    def _count_documents(self) -> int:
        total = 0
        for _ in list_stage_files(self._session, self._coords):
            total += 1
        return total

    @override
    def _make_document_ref(
        self, identifier: SnowflakeFileIdentifier, source_index: int
    ) -> SourceDocumentRef[SnowflakeFileIdentifier]:
        return SourceDocumentRef(
            id=identifier,
            source_index=source_index,
            source_uri=(
                f"snowflake://{self._coords.database}/{self._coords.db_schema}"
                f"/{self._coords.stage}/{identifier.relative_path}"
            ),
            filename=Path(identifier.relative_path).name,
        )

    @_map_snowflake_source_errors
    def _fetch_document_by_id(
        self,
        identifier: SnowflakeFileIdentifier,
        *,
        max_file_size: int | None = None,
    ) -> DocumentStream:
        limit = normalize_max_file_size(max_file_size)
        if limit is not None and identifier.size > limit:
            raise SourceLimitExceededError(
                f"Source '{identifier.relative_path}' ({identifier.size} bytes) "
                f"exceeds max_file_size={limit} bytes"
            )

        _log.info(
            "Downloading from snowflake stage %s.%s.%s: %s",
            self._coords.database,
            self._coords.db_schema,
            self._coords.stage,
            identifier.relative_path,
        )

        # Download with retry on transient failures
        def _download():
            return download_stage_file(
                self._session,
                self._coords,
                identifier.relative_path,
                max_file_size=limit,
            )

        data, display_name = with_retry(
            _download, f"download {identifier.relative_path}"
        )
        return DocumentStream(name=display_name, stream=BytesIO(data))

    def _fetch_documents(
        self, *, max_file_size: int | None = None
    ) -> Iterator[DocumentStream]:
        for identifier in self._list_document_ids():
            yield self._fetch_document_by_id(identifier, max_file_size=max_file_size)


__all__ = ["SnowflakeFileIdentifier", "SnowflakeSourceProcessor"]
