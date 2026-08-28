import hashlib
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

from docling_jobkit.connectors.database_target_processor import (
    BaseDatabaseTargetProcessor,
)
from docling_jobkit.connectors.errors import map_connector_authentication_errors
from docling_jobkit.connectors.snowflake.helper import (
    get_snowflake_connection,
    is_snowflake_authentication_error,
    upsert_table_row,
    upsert_table_rows,
)
from docling_jobkit.connectors.snowflake.models import (
    SnowflakeChunkTarget,
    SnowflakeDocTarget,
)
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem
from docling_jobkit.datamodel.target_field_slots import FieldMappings
from docling_jobkit.public_errors import TargetWriteError

if TYPE_CHECKING:
    from snowflake.snowpark import Session

_log = logging.getLogger(__name__)

# Both target types inherit FieldMappings so the bound _T=FieldMappings
# constraint of BaseDatabaseTargetProcessor is satisfied.
_SnowflakeTarget = Union[SnowflakeDocTarget, SnowflakeChunkTarget]

_map_snowflake_target_errors = map_connector_authentication_errors(
    "Snowflake",
    is_snowflake_authentication_error,
    source=False,
)


class SnowflakeTargetProcessor(BaseDatabaseTargetProcessor[_SnowflakeTarget]):
    def __init__(self, target: _SnowflakeTarget) -> None:
        super().__init__(target)
        self._session: Optional["Session"] = None
        self._current_document_hash: Optional[str] = None
        self._chunk_buffer: list[dict[str, Any]] = []

    @classmethod
    def check_dependencies(cls) -> None:
        import snowflake.snowpark  # noqa: F401

    @classmethod
    def get_config_types(cls) -> tuple[type[FieldMappings], ...]:
        return (SnowflakeDocTarget, SnowflakeChunkTarget)

    @_map_snowflake_target_errors
    def _initialize(self) -> None:
        self._session = get_snowflake_connection(self._target)
        _log.info(
            "Connected to Snowflake table: %s.%s.%s",
            self._target.database,
            self._target.db_schema,
            self._target.table,
        )

    def _finalize(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None

    # Whole-doc

    def upsert_row(self, row: dict[str, Any]) -> None:
        if not isinstance(self._target, SnowflakeDocTarget):
            # Only reachable if a snowflake_chunks target is also given
            # whole-document `mappings` -- not supported yet, unlike chunk
            # fields which always go through consume_chunk().
            raise NotImplementedError(
                "snowflake_chunks does not support whole-document `mappings` "
                "in addition to chunks yet - only chunk fields are written."
            )
        if self._session is None:
            raise RuntimeError("SnowflakeTargetProcessor not initialized")

        id_field = self._target.id_field
        if id_field in row:
            row_id = str(row[id_field])
        elif self._pending_doc_id is not None:
            row_id = self._pending_doc_id
        else:
            row_id = self._row_content_hash(row)
        row = {**row, id_field: row_id}

        try:
            upsert_table_row(self._session, self._target, id_field, row)
        except Exception as exc:
            raise TargetWriteError(
                f"Failed to write document row to Snowflake table "
                f"{self._target.table!r}."
            ) from exc

    # Chunk streaming

    def instance_requires_chunks(self) -> bool:
        """True when this processor is configured as a snowflake_chunks target."""
        return isinstance(self._target, SnowflakeChunkTarget)

    def begin_chunks(
        self,
        filename: str,
        temp_dir: Path,
        chunk_target_key: Optional[str] = None,
        document_hash: Optional[str] = None,
    ) -> None:
        """Start buffering chunk rows for a new document."""
        self._current_document_hash = document_hash
        self._chunk_buffer = []

    def consume_chunk(self, chunk: ChunkedDocumentResultItem) -> None:
        """Buffer one chunk row; the document's chunks are upserted together
        as a single multi-row MERGE in end_chunks() rather than one MERGE
        per chunk. Per-chunk MERGE very slow."""
        from docling_jobkit.convert.chunking import _chunk_row_payload

        if not isinstance(self._target, SnowflakeChunkTarget):
            raise TypeError(
                f"snowflake_chunks processor requires a SnowflakeChunkTarget, "
                f"got {type(self._target)!r}"
            )

        row = _chunk_row_payload(chunk, self._target)
        row[self._target.chunk_id_field] = self._stable_chunk_id(
            binary_hash=self._current_document_hash,
            filename=chunk.filename,
            chunk_index=chunk.chunk_index,
        )
        self._chunk_buffer.append(row)

    def end_chunks(self) -> None:
        """Flush the document's buffered chunk rows as one multi-row MERGE.

        A single MERGE succeeds or fails as a unit, so this is atomic per
        document: either every chunk lands, or none do.
        """
        try:
            if self._chunk_buffer:
                if self._session is None:
                    raise RuntimeError("SnowflakeTargetProcessor not initialized")
                if not isinstance(self._target, SnowflakeChunkTarget):
                    raise TypeError(
                        f"Expected SnowflakeChunkTarget for chunk operations, got {type(self._target)}"
                    )

                _log.debug(
                    "Flushing %d chunk rows to Snowflake table %s",
                    len(self._chunk_buffer),
                    self._target.table,
                )

                try:
                    upsert_table_rows(
                        self._session,
                        self._target,
                        self._target.chunk_id_field,
                        self._chunk_buffer,
                    )
                except Exception as exc:
                    raise TargetWriteError(
                        f"Failed to write chunk rows to Snowflake table "
                        f"{self._target.table!r}."
                    ) from exc
        finally:
            self._chunk_buffer = []
            self._current_document_hash = None

    def abort_chunks(self) -> None:
        """Discard the document's buffered chunk rows.

        Nothing is written until end_chunks() flushes, so an abort here
        means none of this document's chunks reach the table.
        """
        self._chunk_buffer = []
        self._current_document_hash = None

    # Helpers

    @staticmethod
    def _stable_chunk_id(
        binary_hash: Optional[str],
        filename: str,
        chunk_index: int,
    ) -> str:
        """Derive a deterministic, content-addressed ID for a single chunk.

        The ID is a SHA-256 hex digest of "<binary_hash>:<filename>:<chunk_index>".
        Using binary_hash means the same file uploaded under different names
        still produces the same chunk IDs, avoiding duplicate rows on
        re-ingestion. filename is a tiebreaker for the rare case binary_hash
        is unavailable. chunk_index scopes the ID within the document.
        """
        key = f"{binary_hash or filename}:{filename}:{chunk_index}"
        return hashlib.sha256(key.encode(), usedforsecurity=False).hexdigest()

    @staticmethod
    def _row_content_hash(row: dict[str, Any]) -> str:
        """Fallback ID for a document row with no id_field value and no pending doc id."""
        payload = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(payload.encode(), usedforsecurity=False).hexdigest()


__all__ = ["SnowflakeTargetProcessor"]
