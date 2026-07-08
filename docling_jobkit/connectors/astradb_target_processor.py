import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO

from astrapy import Collection
from pydantic import BaseModel

from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.astradb_coords import AstraDBCoordinates
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem

if TYPE_CHECKING:
    from docling_jobkit.convert.chunking import DocumentChunkerManager


class AstraDBTargetProcessor(BaseTargetProcessor):
    def __init__(self, coords: AstraDBCoordinates, chunking_options: Any = None):
        super().__init__()
        self._coords: AstraDBCoordinates = coords
        self._chunking_options = chunking_options
        self._collection: Collection[Any] | None = None
        self._chunker_manager: "DocumentChunkerManager | None" = None
        self._embedding_model: str | None = None
        self._embedding_kwargs: dict = {}
        self._max_tokens: int = 8000

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        from docling_jobkit.datamodel.task_targets import AstraDBTarget

        return (AstraDBTarget,)

    def _initialize(self) -> None:
        from docling_jobkit.connectors.astradb_helper import get_collection
        from docling_jobkit.convert.chunking import DocumentChunkerManager

        # these args are only necessary if the user chooses to use the external embedding provider
        if self._coords.enable_external_provider:
            from docling_jobkit.convert.embedding import generate_text_embedding

            if not self._coords.external_provider_config:
                raise RuntimeError(
                    "external_provider_config is required when enable_external_provider is true"
                )
            external_emb_config = self._coords.external_provider_config
            provider = external_emb_config.provider

            if provider == "ollama":
                self._embedding_model = (
                    f"ollama/{external_emb_config.ollama.embedding_model}"
                )
                self._embedding_kwargs = {
                    "api_base": external_emb_config.ollama.endpoint
                }
                self._max_tokens = (
                    2000  # ollama specifically has a max_tokens of 2000 instead of 8000
                )
            elif provider == "openai":
                self._embedding_model = (
                    f"openai/{external_emb_config.openai.embedding_model}"
                )
                self._embedding_kwargs = {
                    "api_key": external_emb_config.openai.api_key.get_secret_value()
                }
            elif provider == "watsonx":
                self._embedding_model = (
                    f"watsonx/{external_emb_config.watsonx.embedding_model}"
                )
                self._embedding_kwargs = {
                    "api_key": external_emb_config.watsonx.api_key.get_secret_value(),
                    "api_base": external_emb_config.watsonx.endpoint,
                    "project_id": external_emb_config.watsonx.project_id,
                }

            if not self._embedding_model:
                raise RuntimeError("Embedding model was not set")

            emb_dim = len(
                generate_text_embedding(
                    self._embedding_model, ["test"], **self._embedding_kwargs
                )[0]
            )
            if not emb_dim:
                raise RuntimeError("Could not determine embedding dimension from model")

            self._collection = get_collection(self._coords, emb_dim)
        else:
            self._collection = get_collection(self._coords)

        self._chunker_manager = DocumentChunkerManager()

    def _finalize(self) -> None:
        self._collection = None
        self._chunker_manager = None
        self._embedding_model = None
        self._embedding_kwargs = {}
        self._max_tokens = 8000

    def upload_chunks(
        self,
        chunks: list[ChunkedDocumentResultItem],
        doc_id: str,
        source_name: str,
    ) -> None:
        """Embed and upsert pre-built chunk items into AstraDB."""
        from docling_jobkit.connectors.astradb_helper import (
            build_records_from_chunks,
            insert_records,
        )

        if not self._collection:
            raise RuntimeError("Collection not initialized")

        if not chunks:
            logging.warning("AstraDB: no chunks to insert for '%s'", source_name)
            return

        records = build_records_from_chunks(
            chunks,
            doc_id=doc_id,
            source_name=source_name,
            emb_model=self._embedding_model,
            emb_kwargs=self._embedding_kwargs,
            emb_max_tokens=self._max_tokens,
        )
        insert_records(self._collection, records, source_name=source_name)

    def chunk_and_upload(
        self,
        exportable_document: Any,
        chunking_options: Any = None,
    ) -> None:
        """Chunk a live ``ExportableDocument`` and upload to AstraDB.

        Called by the export path before the document reference is released,
        so chunking operates directly on the in-memory DoclingDocument.
        """
        from docling_jobkit.convert.export import _is_exportable_status

        if not _is_exportable_status(exportable_document.status):
            return
        if exportable_document.document is None:
            return
        if self._chunker_manager is None:
            raise RuntimeError("Chunker manager not initialized")

        from docling.datamodel.service.chunking import HybridChunkerOptions

        options = chunking_options or self._chunking_options or HybridChunkerOptions()
        doc = exportable_document.document
        source_name = exportable_document.file.name
        doc_id = str(doc.origin.binary_hash) if doc.origin else source_name

        chunks = list(
            self._chunker_manager.chunk_document(
                document=doc,
                filename=source_name,
                options=options,
            )
        )

        if not chunks:
            logging.warning("AstraDB: no chunks produced for '%s'", source_name)
            return

        self.upload_chunks(chunks, doc_id=doc_id, source_name=source_name)

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
