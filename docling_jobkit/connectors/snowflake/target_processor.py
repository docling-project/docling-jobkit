import hashlib
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

from docling_jobkit.connectors.database_target_processor import (
    BaseDatabaseTargetProcessor,
)
from docling_jobkit.connectors.snowflake.helper import (
    get_snowflake_connection,
    upsert_table_row,
)
from docling_jobkit.connectors.snowflake.models import (
    SnowflakeChunkTarget,
    SnowflakeDocTarget,
)
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem
from docling_jobkit.datamodel.target_field_slots import FieldMappings
from docling_jobkit.public_errors import TargetWriteError

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection

_log = logging.getLogger(__name__)

# Both target types inherit FieldMappings so the bound _T=FieldMappings
# constraint of BaseDatabaseTargetProcessor is satisfied.
_SnowflakeTarget = Union[SnowflakeDocTarget, SnowflakeChunkTarget]


class SnowflakeTargetProcessor(BaseDatabaseTargetProcessor[_SnowflakeTarget]):
    def __init__(self, target: _SnowflakeTarget) -> None:
        super().__init__(target)
        self._connection: Optional["SnowflakeConnection"] = None
        self._current_document_hash: Optional[str] = None

    @classmethod
    def check_dependencies(cls) -> None:
        import snowflake.connector  # noqa: F401

    @classmethod
    def get_config_types(cls) -> tuple[type[FieldMappings], ...]:
        return (SnowflakeDocTarget, SnowflakeChunkTarget)

    def _initialize(self) -> None:
        try:
            self._connection = get_snowflake_connection(self._target)
        except Exception as exc:
            raise TargetWriteError(
                f"Could not connect to Snowflake table {self._target.table!r}."
            ) from exc

    def _finalize(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    # Whole-doc

    def upsert_row(self, row: dict[str, Any]) -> None:
        if not isinstance(self._target, SnowflakeDocTarget):
            # Only reachable if a snowflake_chunks target is also given
            # whole-document `mappings` -- not supported yet, unlike chunk
            # fields which always go through consume_chunk().
            raise NotImplementedError(
                "snowflake_chunks does not support whole-document `mappings` "
                "in addition to chunks yet -- only chunk fields are written."
            )
        assert self._connection is not None

        id_field = self._target.id_field
        if id_field in row:
            row_id = str(row[id_field])
        elif self._pending_doc_id is not None:
            row_id = self._pending_doc_id
        else:
            row_id = self._row_content_hash(row)
        row = {**row, id_field: row_id}

        try:
            upsert_table_row(self._connection, self._target, id_field, row)
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
        """No file needed -- each chunk is upserted directly in consume_chunk()."""
        self._current_document_hash = document_hash

    def consume_chunk(self, chunk: ChunkedDocumentResultItem) -> None:
        """Upsert one chunk row into Snowflake immediately."""
        from docling_jobkit.convert.chunking import _chunk_row_payload

        if not isinstance(self._target, SnowflakeChunkTarget):
            raise TypeError(
                f"snowflake_chunks processor requires a SnowflakeChunkTarget, "
                f"got {type(self._target)!r}"
            )
        assert self._connection is not None

        row = _chunk_row_payload(chunk, self._target)
        chunk_id_field = self._target.chunk_id_field
        row[chunk_id_field] = self._stable_chunk_id(
            binary_hash=self._current_document_hash,
            filename=chunk.filename,
            chunk_index=chunk.chunk_index,
        )

        try:
            upsert_table_row(self._connection, self._target, chunk_id_field, row)
        except Exception as exc:
            raise TargetWriteError(
                f"Failed to write chunk row to Snowflake table {self._target.table!r}."
            ) from exc

    def end_chunks(self) -> None:
        """Nothing to flush -- each chunk was written in consume_chunk()."""
        self._current_document_hash = None

    def abort_chunks(self) -> None:
        """Nothing buffered to discard -- drop the per-document state only.

        Chunks already written before the failure stay in the table; they
        carry deterministic IDs (see _stable_chunk_id), so a re-run overwrites
        rather than duplicates them.
        """
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
