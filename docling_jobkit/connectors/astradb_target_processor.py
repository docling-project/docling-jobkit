import logging
from pathlib import Path
from typing import BinaryIO

from pydantic import BaseModel

from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.astradb_coords import AstraDBCoordinates
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem


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

    def upload_chunks(
        self,
        chunks: list[ChunkedDocumentResultItem],
        doc_id: str,
        source_name: str,
    ) -> None:
        """Chunk records into AstraDB. Called by the pipeline with pre-built chunks."""
        from docling_jobkit.connectors.astradb_helper import (
            build_records_from_chunks,
            insert_records,
        )

        if not chunks:
            logging.warning("AstraDB: no chunks to insert for '%s'", source_name)
            return

        records = build_records_from_chunks(chunks, doc_id=doc_id, source_name=source_name)
        insert_records(self._collection, records, source_name=source_name)

    # TODO: These are dead - leftover from abstract base class. Likely should create new base class for 
    # different storage types. 

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        logging.debug("AstraDB: upload_file is a no-op for '%s'", target_filename)

    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
    ) -> None:
        logging.debug("AstraDB: upload_object is a no-op for '%s'", target_filename)
