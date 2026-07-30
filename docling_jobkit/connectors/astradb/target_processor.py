import hashlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from docling_jobkit.connectors.astradb.models import AstraDBChunkTarget
from docling_jobkit.connectors.database_target_processor import (
    BaseDatabaseTargetProcessor,
)
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem
from docling_jobkit.datamodel.target_field_slots import FieldMappings
from docling_jobkit.public_errors import TargetWriteError

if TYPE_CHECKING:
    from astrapy import Collection, DataAPIClient

_log = logging.getLogger(__name__)


class AstraDBTargetProcessor(BaseDatabaseTargetProcessor[AstraDBChunkTarget]):
    def __init__(self, target: AstraDBChunkTarget) -> None:
        super().__init__(target)
        self._client: Optional["DataAPIClient"] = None
        self._collection: Optional["Collection"] = None
        self._current_document_hash: Optional[str] = None

    @classmethod
    def check_dependencies(cls) -> None:
        import astrapy  # noqa: F401

    @classmethod
    def get_config_types(cls) -> tuple[type[FieldMappings], ...]:
        return (AstraDBChunkTarget,)

    def _initialize(self) -> None:
        try:
            from astrapy import DataAPIClient
            from astrapy.info import CollectionVectorServiceOptions

            self._client = DataAPIClient(token=self._target.token.get_secret_value())
            db = self._client.get_database(
                str(self._target.api_endpoint),
                keyspace=self._target.keyspace,
            )

            # Configure server-side vectorization
            vectorize_config = CollectionVectorServiceOptions(
                provider=self._target.vectorize_provider,
                model_name=self._target.vectorize_model,
                authentication=self._target.vectorize_authentication,
            )

            # Try to get existing collection first, create if it doesn't exist
            try:
                self._collection = db.get_collection(self._target.collection_name)
                _log.info(
                    "AstraDB: using existing collection '%s'",
                    self._target.collection_name,
                )
            except Exception:
                # Collection doesn't exist, create it
                self._collection = db.create_collection(
                    self._target.collection_name,
                    service=vectorize_config,
                    check_exists=False,
                )
                _log.info(
                    "AstraDB: created new collection '%s'",
                    self._target.collection_name,
                )

            _log.info(
                "AstraDB: connected to collection '%s' in keyspace '%s' "
                "with vectorize provider '%s' model '%s'",
                self._target.collection_name,
                self._target.keyspace,
                self._target.vectorize_provider,
                self._target.vectorize_model,
            )
        except Exception as exc:
            raise TargetWriteError(
                f"Could not connect to AstraDB collection "
                f"{self._target.collection_name!r}"
            ) from exc

    def _finalize(self) -> None:
        """Clean up AstraDB connection references."""
        self._client = None
        self._collection = None

    # ------------------------------------------------------------------
    # Streaming chunk protocol
    # ------------------------------------------------------------------

    def instance_requires_chunks(self) -> bool:
        """AstraDB always requires chunks (it's a chunk-only target)."""
        return True

    def begin_chunks(
        self,
        filename: str,
        temp_dir: Path,
        chunk_target_key: Optional[str] = None,
        document_hash: Optional[str] = None,
    ) -> None:
        """No file needed — each chunk is upserted directly in consume_chunk()."""
        self._current_document_hash = document_hash

    def consume_chunk(self, chunk: ChunkedDocumentResultItem) -> None:  # type: ignore[name-defined]
        """Upsert one chunk directly to AstraDB with server-side vectorization."""
        from docling_jobkit.convert.chunking import _chunk_row_payload
        from docling_jobkit.datamodel.result import ChunkedDocumentResultItem

        if not isinstance(chunk, ChunkedDocumentResultItem):
            raise TypeError(f"Expected ChunkedDocumentResultItem, got {type(chunk)!r}")

        # Build the row payload using the shared helper
        row = _chunk_row_payload(chunk, self._target)

        # Generate stable, content-addressed chunk ID
        chunk_id = self._stable_chunk_id(
            binary_hash=self._current_document_hash,
            filename=chunk.filename,
            chunk_index=chunk.chunk_index,
        )

        # AstraDB will automatically vectorize the $vectorize field server-side
        # The text is stored in both 'text' field (for retrieval) and '$vectorize' (for embedding)
        record = {
            "_id": chunk_id,
            "$vectorize": chunk.text,  # AstraDB generates vector from this
            **row,
        }

        self._upsert_with_retry(record)

    def end_chunks(self) -> None:
        """Clean up per-document state.

        Nothing to flush — each chunk was written immediately in consume_chunk().
        """
        self._current_document_hash = None

    def abort_chunks(self) -> None:
        """Drop per-document state without flushing.

        Chunks already indexed before the failure stay in the collection; they
        carry deterministic IDs (see _stable_chunk_id), so a re-run overwrites
        rather than duplicates them.
        """
        self._current_document_hash = None

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    @staticmethod
    def _stable_chunk_id(
        binary_hash: Optional[str],
        filename: str,
        chunk_index: int,
    ) -> str:
        """Derive a deterministic, content-addressed ID for a single chunk.

        The ID is a SHA-256 hex digest of ``"<binary_hash>:<filename>:<chunk_index>"``.
        Using ``binary_hash`` (SHA-256 of the raw input bytes) means the same
        file uploaded under different names still produces the same chunk IDs,
        avoiding duplicate entries on re-ingestion. ``filename`` is included as
        a tiebreaker for the rare case where ``binary_hash`` is unavailable (falls
        back to filename-only stability). ``chunk_index`` scopes the ID to the
        specific chunk within the document.
        """
        key = f"{binary_hash or filename}:{filename}:{chunk_index}"
        return hashlib.sha256(key.encode(), usedforsecurity=False).hexdigest()

    def _upsert_with_retry(self, record: dict[str, Any]) -> None:
        """Upsert a record with exponential backoff on transient errors."""
        from docling_jobkit.connectors.astradb.helper import upsert_record_with_retry

        try:
            upsert_record_with_retry(
                collection=self._collection,
                record=record,
                max_retries=3,
            )
        except Exception as exc:
            raise TargetWriteError(
                f"Failed to upsert chunk to AstraDB collection "
                f"{self._target.collection_name!r}"
            ) from exc

    # ------------------------------------------------------------------
    # Database target protocol (unused here)
    # ------------------------------------------------------------------

    def upsert_row(self, row: dict[str, Any]) -> None:
        """Not used — AstraDB target only supports chunks, not full documents."""
        raise NotImplementedError(
            "AstraDB target only supports chunks, not full documents. "
            "Use 'astradb_chunks' target kind and enable chunking in your task."
        )


__all__ = ["AstraDBTargetProcessor"]
