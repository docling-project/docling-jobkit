from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from itertools import islice
from typing import Generic, Iterator, Sequence, TypeVar

from pydantic import BaseModel, ConfigDict

from docling.datamodel.base_models import DocumentStream

FileIdentifierT = TypeVar("FileIdentifierT")  # identifier type per connector
SourceT = TypeVar("SourceT")  # root source type per connector
ConverterSource = str | DocumentStream


class SourceDocumentRef(BaseModel, Generic[FileIdentifierT]):
    """Connector-native document reference safe to pass between processes."""

    id: FileIdentifierT
    source_index: int
    source_uri: str
    filename: str


class DocumentChunk(BaseModel, Generic[SourceT, FileIdentifierT]):
    """A serializable batch of connector-native document references.

    A chunk carries only the root ``source`` plus the ``refs`` needed to fetch its
    documents, so it is always safe to send across a process boundary (CLI
    ``mp.Pool`` pickling or Ray cloudpickle). Workers reconstruct their own
    processor via ``get_source_processor(chunk.source)`` and fetch each ref with
    ``fetch_converter_source_by_ref``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source: SourceT
    refs: Sequence[SourceDocumentRef[FileIdentifierT]]
    chunk_index: int

    @property
    def ids(self) -> list[FileIdentifierT]:
        return [ref.id for ref in self.refs]

    @property
    def index(self) -> int:
        return self.chunk_index


class BaseSourceProcessor(
    Generic[SourceT, FileIdentifierT], AbstractContextManager, ABC
):
    """
    Base class for source processors.
    Handles initialization state and context management.
    """

    def __init__(self, source: SourceT):
        self._processor_source = source
        self._initialized = False  # Track whether the processor is ready

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        """Config (pydantic) models this processor accepts.

        Each returned model must carry a ``Literal`` ``kind`` field. The connector
        factory keys its registry on these types, so a processor that handles
        several config shapes (e.g. file + http) returns a multi-element tuple.
        """
        raise NotImplementedError(
            f"{cls.__name__} must implement get_config_types() to be registered "
            "as a connector plugin."
        )

    @classmethod
    def is_expandable(cls, config: BaseModel) -> bool:
        del config
        return True

    def __enter__(self):
        self._initialize()
        self._initialized = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._finalize()
        self._initialized = False

    @abstractmethod
    def _initialize(self):
        """Prepare the processor (authenticate, open SDK clients, etc.)."""

    @abstractmethod
    def _finalize(self):
        """Clean up resources."""

    @abstractmethod
    def _fetch_documents(
        self, *, max_file_size: int | None = None
    ) -> Iterator[DocumentStream]:
        """Yield documents from the source."""

    def _list_document_ids(self) -> Iterator[FileIdentifierT] | None:
        return None

    def _fetch_document_by_id(
        self,
        identifier: FileIdentifierT,
        *,
        max_file_size: int | None = None,
    ) -> DocumentStream:
        del max_file_size
        raise NotImplementedError

    def fetch_by_locator(
        self, locator: str, *, max_file_size: int | None = None
    ) -> ConverterSource:
        """Resolve a backend-relative locator to a single ``ConverterSource``.

        Used by the Kafka connector to fetch the document a trigger event points
        at against its configured connector_config. Only single-document resolution
        is supported right now, folder/multi-file ingestion could be added later.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support being a Kafka connector "
            "(no fetch_by_locator)."
        )

    def _make_document_ref(
        self, identifier: FileIdentifierT, source_index: int
    ) -> SourceDocumentRef[FileIdentifierT]:
        """Build a process-safe reference for a connector document."""
        filename = str(identifier)
        return SourceDocumentRef(
            id=identifier,
            source_index=source_index,
            source_uri=filename,
            filename=filename,
        )

    @property
    def source(self) -> SourceT:
        """Return the root source needed to reconstruct this processor."""
        return self._processor_source

    def _count_documents(self) -> int | None:
        return None

    def fetch_converter_source_by_ref(
        self,
        ref: SourceDocumentRef[FileIdentifierT],
        *,
        max_file_size: int | None = None,
    ) -> ConverterSource:
        """Resolve a ref into the converter input expected by the backend.

        Most connectors materialize a ``DocumentStream`` from the ref's identifier.
        Connectors with remote-fetch semantics may override this to return a lighter
        representation such as a source URL.
        """
        return self._fetch_document_by_id(ref.id, max_file_size=max_file_size)

    def headers_for_ref(
        self, ref: SourceDocumentRef[FileIdentifierT]
    ) -> dict[str, object] | None:
        """Return per-ref request headers when the converter should fetch remotely."""
        del ref
        return None

    def iterate_documents(
        self, *, max_file_size: int | None = None
    ) -> Iterator[DocumentStream]:
        if not self._initialized:
            raise RuntimeError(
                "Processor not initialized. Use 'with' to open it first."
            )
        yield from self._fetch_documents(max_file_size=max_file_size)

    def iterate_converter_sources(
        self, *, max_file_size: int | None = None
    ) -> Iterator[ConverterSource]:
        yield from self.iterate_documents(max_file_size=max_file_size)

    def converter_headers(self) -> dict[str, object] | None:
        return None

    def iterate_document_chunks(
        self, chunk_size: int
    ) -> Iterator[DocumentChunk[SourceT, FileIdentifierT]]:
        ids_gen = self._list_document_ids()
        if ids_gen is None:
            raise RuntimeError("Connector does not support chunking.")

        chunk_index = 0
        source_index = 0

        while True:
            ids = list(islice(ids_gen, chunk_size))
            if not ids:
                break
            refs = [
                self._make_document_ref(identifier, source_index + offset)
                for offset, identifier in enumerate(ids)
            ]

            yield DocumentChunk(
                source=self.source,
                refs=refs,
                chunk_index=chunk_index,
            )

            chunk_index += 1
            source_index += len(ids)
