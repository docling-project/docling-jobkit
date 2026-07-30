import json as _json
from pathlib import Path
from typing import Any, BinaryIO, Generic, Literal, Optional, TypeVar

from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.target_field_slots import (
    OUTPUT_FORMAT_MIME,
    FieldMappings,
    coerce_large_ints_inplace,
)

_T = TypeVar("_T", bound=FieldMappings)

# Maps content-type → OutputFormat name used in FieldMappings.mappings.
# Derived from OUTPUT_FORMAT_MIME so there is a single source of truth.
# Where two formats share a MIME type (DOCTAGS and TEXT both use text/plain),
# the first entry wins — TEXT is the user-facing mapping key for plain text.
CONTENT_TYPE_TO_FORMAT: dict[str, str] = {
    mime: fmt.name for fmt, mime in reversed(list(OUTPUT_FORMAT_MIME.items()))
}


class BaseDatabaseTargetProcessor(BaseTargetProcessor, Generic[_T]):
    """Base class for database-backed target processors.

    All output formats produced for a single input document are accumulated
    into one row dict during ``upload_file``/``upload_object`` calls (bracketed
    by :meth:`begin_document` / :meth:`end_document`) and flushed as a single
    :meth:`upsert_row` call when the document boundary is signalled.

    Subclasses must implement :meth:`upsert_row`.  They receive the ``target``
    model which carries the ``mappings`` configuration that controls which
    format keys map to which field names in the row.
    """

    def __init__(self, target: _T) -> None:
        super().__init__()
        self._target: _T = target
        self._pending_row: Optional[dict[str, Any]] = None
        self._pending_doc_id: Optional[str] = None

    @classmethod
    def get_config_types(cls) -> tuple[type[FieldMappings], ...]:
        return ()

    @classmethod
    def result_mode(cls) -> Literal["database"]:
        return "database"

    def _initialize(self) -> None:
        pass

    def _finalize(self) -> None:
        pass

    # ------------------------------------------------------------------
    # Document-boundary helpers
    # ------------------------------------------------------------------

    def begin_document(self, doc_id: str) -> None:
        """Start accumulating fields for a new document row."""
        self._pending_doc_id = doc_id
        self._pending_row = {}

    def end_document(self, doc_id: str) -> None:
        """Flush the accumulated row for the current document."""
        if self._pending_row:
            self.upsert_row(self._pending_row)
        self._pending_row = None
        self._pending_doc_id = None

    def abort_document(self, doc_id: str) -> None:
        """Drop the half-populated row for a document that failed mid-upload."""
        self._pending_row = None
        self._pending_doc_id = None

    # ------------------------------------------------------------------
    # Upload helpers — accumulate into the pending row when inside a
    # begin_document/end_document bracket; otherwise fall through to the
    # old per-call path so callers that don't bracket still work.
    # ------------------------------------------------------------------

    def _mappings(self) -> dict[str, str]:
        """Return the format→field mappings from the target config."""
        return self._target.mappings or {}

    def _mapped_field(self, content_type: str) -> Optional[str]:
        """Row field this *content_type* writes to, or None when it is not stored.

        Resolving this *before* touching the payload lets ``upload_file`` skip
        reading artifacts the target config does not map at all — previously
        every exported format was read into memory in full only to be discarded.
        """
        format_key = CONTENT_TYPE_TO_FORMAT.get(content_type)
        if format_key is None:
            return None  # binary artifact — skip
        return self._mappings().get(format_key)

    def _store_json(self, field_name: str, parsed: Any) -> None:
        """Store an exclusively-owned parsed JSON value on the pending row."""
        assert self._pending_row is not None
        if self._target.coerce_large_ints_to_str:
            # In-place: ``parsed`` was just decoded for this call and is owned
            # here, so rewriting it avoids a second full copy of the document.
            parsed = coerce_large_ints_inplace(parsed)
        self._pending_row[field_name] = parsed

    def _accumulate(self, content_type: str, obj: "str | bytes | BinaryIO") -> None:
        """Add one format's content to the pending row if mapped."""
        if self._pending_row is None:
            return  # not inside a begin/end bracket — subclass handles directly

        field_name = self._mapped_field(content_type)
        if field_name is None:
            return  # unmapped format or binary artifact — skip

        if hasattr(obj, "read"):
            payload: str | bytes = obj.read()  # type: ignore[union-attr]
        else:
            payload = obj  # type: ignore[assignment]

        if content_type == "application/json":
            # json.loads accepts str and bytes directly — decoding first would
            # allocate a second full-size copy of the document for nothing.
            self._store_json(field_name, _json.loads(payload))
            return

        self._pending_row[field_name] = (
            payload.decode("utf-8")
            if isinstance(payload, (bytes, bytearray))
            else payload
        )

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        path = Path(filename)

        if self._pending_row is not None:
            field_name = self._mapped_field(content_type)
            if field_name is None:
                return  # not stored by this target — never read the file
            if content_type == "application/json":
                # Parse straight off the file handle: the decoder consumes the
                # stream incrementally, so the document exists once (as the
                # parsed structure) rather than as text *and* bytes *and* tree.
                with path.open("rb") as fh:
                    self._store_json(field_name, _json.load(fh))
            else:
                self._pending_row[field_name] = path.read_text(encoding="utf-8")
            return

        # Outside a begin/end bracket the subclass writes the payload directly.
        if content_type == "application/json":
            self.upload_object(
                path.read_text(encoding="utf-8"), target_filename, content_type
            )
        else:
            self.upload_object(path.read_bytes(), target_filename, content_type)

    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
    ) -> None:
        self._accumulate(content_type, obj)
        # If not inside a bracket the subclass is expected to override this.

    def upsert_row(self, row: dict[str, Any]) -> None:
        raise NotImplementedError


__all__ = ["CONTENT_TYPE_TO_FORMAT", "BaseDatabaseTargetProcessor"]
