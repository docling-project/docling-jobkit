import hashlib
import json
import logging
from typing import TYPE_CHECKING, Any, Optional

from docling_jobkit.connectors.database_target_processor import (
    BaseDatabaseTargetProcessor,
)
from docling_jobkit.connectors.snowflake.helper import (
    get_snowflake_connection,
    upsert_document_row,
)
from docling_jobkit.connectors.snowflake.models import SnowflakeDocTarget
from docling_jobkit.datamodel.target_field_slots import FieldMappings
from docling_jobkit.public_errors import TargetWriteError

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection

_log = logging.getLogger(__name__)


class SnowflakeTargetProcessor(BaseDatabaseTargetProcessor[SnowflakeDocTarget]):
    def __init__(self, target: SnowflakeDocTarget) -> None:
        super().__init__(target)
        self._connection: Optional["SnowflakeConnection"] = None

    @classmethod
    def check_dependencies(cls) -> None:
        import snowflake.connector  # noqa: F401

    @classmethod
    def get_config_types(cls) -> tuple[type[FieldMappings], ...]:
        return (SnowflakeDocTarget,)

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

    def upsert_row(self, row: dict[str, Any]) -> None:
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
            upsert_document_row(self._connection, self._target, row)
        except Exception as exc:
            raise TargetWriteError(
                f"Failed to write document row to Snowflake table "
                f"{self._target.table!r}."
            ) from exc

    @staticmethod
    def _row_content_hash(row: dict[str, Any]) -> str:
        """Fallback ID for a row with no id_field value and no pending doc id."""
        payload = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(payload.encode(), usedforsecurity=False).hexdigest()


__all__ = ["SnowflakeTargetProcessor"]
