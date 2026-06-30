import logging
from pathlib import Path
from typing import BinaryIO

from pydantic import BaseModel

from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.astradb_coords import AstraDBCoordinates

_log = logging.getLogger(__name__)


class AstraDBTargetProcessor(BaseTargetProcessor):
    def __init__(self, coords: AstraDBCoordinates):
        super().__init__()
        self._coords = coords
        self._collection = None

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        from docling_jobkit.datamodel.task_targets import AstraDBTarget

        return (AstraDBTarget,)

    def _initialize(self) -> None:
        from docling_jobkit.connectors.astradb_helper import get_collection

        self._collection = get_collection(self._coords)

    def _finalize(self) -> None:
        self._collection = None

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        if content_type != "application/json":
            _log.debug("AstraDB: skipping non-JSON artifact '%s'", target_filename)
            return
        with open(filename, "rb") as f:
            self.upload_object(f.read(), target_filename, content_type)

    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
    ) -> None:
        if content_type != "application/json":
            _log.debug("AstraDB: skipping non-JSON artifact '%s'", target_filename)
            return

        from docling_jobkit.connectors.astradb_helper import (
            build_chunk_records,
            insert_records,
        )

        if hasattr(obj, "read"):
            raw: bytes = obj.read()
        elif isinstance(obj, str):
            raw = obj.encode()
        else:
            raw = obj

        records = build_chunk_records(raw, source_name=target_filename)
        insert_records(self._collection, records, source_name=target_filename)
