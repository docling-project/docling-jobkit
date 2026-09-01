from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any, Optional, Union

from docling_jobkit.connectors.database_target_processor import (
    BaseDatabaseTargetProcessor,
)
from docling_jobkit.connectors.errors import map_connector_authentication_errors
from docling_jobkit.connectors.spark.backend_factory import SparkBackend, get_backend
from docling_jobkit.connectors.spark.helper import (
    is_spark_authentication_error,
    normalize_row,
)
from docling_jobkit.connectors.spark.models import SparkChunkTarget, SparkDocTarget
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem
from docling_jobkit.datamodel.target_field_slots import FieldMappings

_log = logging.getLogger(__name__)

# Both target kinds inherit FieldMappings so the bound _T=FieldMappings
# constraint of BaseDatabaseTargetProcessor is satisfied.
_SparkTarget = Union[SparkDocTarget, SparkChunkTarget]

_map_spark_target_errors = map_connector_authentication_errors(
    "Spark", is_spark_authentication_error
)


class SparkTargetProcessor(BaseDatabaseTargetProcessor[_SparkTarget]):
    def __init__(self, target: _SparkTarget) -> None:
        super().__init__(target)
        self._backend: SparkBackend | None = None
        self._buffer: list[dict[str, Any]] = []
        self._document_hash: Optional[str] = None

    @property
    def _row(self) -> dict[str, Any]:
        if self._pending_row is None:
            raise RuntimeError("no active document; begin_document() was not called")
        return self._pending_row

    @property
    def _active_backend(self) -> SparkBackend:
        if self._backend is None:
            raise RuntimeError(
                "Spark target not connected; _initialize() was not called"
            )
        return self._backend

    @classmethod
    def check_dependencies(cls) -> None:
        import pyspark  # noqa: F401

    @classmethod
    def get_config_types(cls) -> tuple[type[FieldMappings], ...]:
        return (SparkDocTarget, SparkChunkTarget)

    @_map_spark_target_errors
    def _initialize(self) -> None:
        self._backend = get_backend(self._target)
        exists = self._backend.table_exists(self._target.table)
        _log.info(
            "Spark target connected; table %s %s",
            self._target.table,
            "exists" if exists else "will be created on first flush",
        )

    def _finalize(self) -> None:
        self._flush()
        if self._backend is not None:
            self._backend.close()

        self._backend = None
        self._buffer = []

    def _columns(self) -> list[str]:
        if isinstance(self._target, SparkChunkTarget):
            t = self._target
            cols = [
                t.chunk_id_field,
                t.text_field,
                t.metadata_field,
                t.doc_id_field,
                t.chunk_index_field,
            ]
            if t.page_field:
                cols.append(t.page_field)
            if t.headings_field:
                cols.append(t.headings_field)
            return cols
        return [self._target.doc_id_field, *self._target.mappings.values()]

    # ------------------------------------------------------------------
    # Doc path
    # ------------------------------------------------------------------

    def begin_document(self, doc_id: str) -> None:
        super().begin_document(doc_id)
        if isinstance(self._target, SparkDocTarget):
            self._row[self._target.doc_id_field] = doc_id

    def upsert_row(self, row: dict[str, Any]) -> None:
        self._add_row(row)

    # ------------------------------------------------------------------
    # Chunk path
    # ------------------------------------------------------------------

    def instance_requires_chunks(self) -> bool:
        """True when this processor is configured as an spark_chunks target"""
        return isinstance(self._target, SparkChunkTarget)

    def begin_chunks(
        self,
        filename: str,
        temp_dir: Path,
        chunk_target_key: str | None = None,
        document_hash: str | None = None,
    ) -> None:
        """Capture the source document hash for content-addressed chunk ids."""
        self._document_hash = document_hash

    def consume_chunk(self, chunk: ChunkedDocumentResultItem) -> None:
        """Build one content-addressed chunk row and buffer it."""
        from docling_jobkit.convert.chunking import _chunk_row_payload

        if not isinstance(self._target, SparkChunkTarget):
            raise TypeError(
                f"spark_chunks processor requires a SparkChunkTarget, "
                f"got {type(self._target)!r}"
            )
        row = _chunk_row_payload(chunk, self._target)
        if self._document_hash:
            # overriding id field set by chunk_row_payload
            # where doc_id is set to filename unconditionally
            # TODO: Fix chunk_row_payload to accept a hash, fall back to filename
            row[self._target.doc_id_field] = self._document_hash
        row[self._target.chunk_id_field] = self._stable_chunk_id(
            self._document_hash, chunk.filename, chunk.chunk_index
        )
        self._add_row(row)

    def end_chunks(self) -> None:
        """Nothing to flush, each chunk was written in consume_chunk()."""
        self._document_hash = None

    def abort_chunks(self) -> None:
        """Drop per-document state; buffered chunks carry stable ids, so a re-run upserts."""
        self._document_hash = None

    @staticmethod
    def _stable_chunk_id(
        binary_hash: Optional[str],
        filename: str,
        chunk_index: int,
    ) -> str:
        """Deterministic, content-addressed chunk id for a single chunk

        The ID is a SHA-256 of ``"<binary_hash|filename>:<filename>:<chunk_index>"``.
        """
        key = f"{binary_hash or filename}:{filename}:{chunk_index}"
        return hashlib.sha256(key.encode(), usedforsecurity=False).hexdigest()

    # ------------------------------------------------------------------
    # Buffering + format-aware write
    # ------------------------------------------------------------------

    def _add_row(self, row: dict[str, Any]) -> None:
        """Normalize and buffer one row; flush when the batch fills."""
        self._buffer.append(normalize_row(row, self._columns()))
        if len(self._buffer) >= self._target.flush_batch_size:
            self._flush()

    @_map_spark_target_errors
    def _flush(self) -> None:
        if not self._buffer:
            return

        rows, self._buffer = self._buffer, []
        int_cols = (
            {self._target.chunk_index_field}
            if isinstance(self._target, SparkChunkTarget)
            else set()
        )
        key = (
            self._target.chunk_id_field
            if isinstance(self._target, SparkChunkTarget)
            else self._target.doc_id_field
        )
        _log.info(
            "Spark target writing %d row(s) to %s (format=%s)",
            len(rows),
            self._target.table,
            self._target.table_format,
        )
        self._active_backend.write_rows(
            self._target.table,
            self._columns(),
            int_cols,
            rows,
            key=key,
            table_format=self._target.table_format,
        )
