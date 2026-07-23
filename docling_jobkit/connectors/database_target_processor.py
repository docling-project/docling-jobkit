import json as _json
from pathlib import Path
from typing import Any, BinaryIO, Generic, Optional, TypeVar

from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.target_field_slots import (
    OUTPUT_FORMAT_MIME,
    FieldMappings,
    coerce_large_ints,
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

    # ------------------------------------------------------------------
    # Upload helpers — accumulate into the pending row when inside a
    # begin_document/end_document bracket; otherwise fall through to the
    # old per-call path so callers that don't bracket still work.
    # ------------------------------------------------------------------

    def _mappings(self) -> dict[str, str]:
        """Return the format→field mappings from the target config."""
        return self._target.mappings or {}

    def _accumulate(self, content_type: str, obj: "str | bytes | BinaryIO") -> None:
        """Add one format's content to the pending row if mapped."""
        if self._pending_row is None:
            return  # not inside a begin/end bracket — subclass handles directly

        format_key = CONTENT_TYPE_TO_FORMAT.get(content_type)
        if format_key is None:
            return  # binary artifact — skip

        field_name = self._mappings().get(format_key)
        if field_name is None:
            return  # format not mapped in config — skip

        if hasattr(obj, "read"):
            raw: bytes = obj.read()  # type: ignore[union-attr]
        elif isinstance(obj, str):
            raw = obj.encode("utf-8")
        else:
            raw = obj  # type: ignore[assignment]

        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw

        if content_type == "application/json":
            parsed = _json.loads(text)
            if self._target.coerce_large_ints_to_str:
                parsed = coerce_large_ints(parsed)
            self._pending_row[field_name] = parsed
        else:
            self._pending_row[field_name] = text

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        path = Path(filename)
        if content_type == "application/json":
            text = path.read_text(encoding="utf-8")
            self._accumulate(content_type, text)
            if self._pending_row is None:
                self.upload_object(text, target_filename, content_type)
            return
        raw = path.read_bytes()
        self._accumulate(content_type, raw)
        if self._pending_row is None:
            self.upload_object(raw, target_filename, content_type)

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
