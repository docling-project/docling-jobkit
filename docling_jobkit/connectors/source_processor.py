from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from itertools import islice
from typing import Any, Callable, Generic, Iterator, Sequence, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from docling.datamodel.base_models import DocumentStream

FileIdentifierT = TypeVar("FileIdentifierT")  # identifier type per connector
SourceT = TypeVar("SourceT")  # root source type per connector
ConverterSource = str | DocumentStream


class SourceDocumentRef(BaseModel, Generic[FileIdentifierT]):
    """Connector-native document reference safe to pass between processes."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: FileIdentifierT
    source_index: int
    source_uri: str
    filename: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class DocumentChunk(BaseModel, Generic[SourceT, FileIdentifierT]):
    """A data-only source chunk plus a local fetcher convenience."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source: SourceT
    refs: Sequence[SourceDocumentRef[FileIdentifierT]]
    chunk_index: int
    _fetcher: Callable[[FileIdentifierT], DocumentStream] | None = None

    def __init__(
        self,
        source: SourceT,
        refs: Sequence[SourceDocumentRef[FileIdentifierT]],
        chunk_index: int,
        fetcher: Callable[[FileIdentifierT], DocumentStream] | None = None,
    ):
        super().__init__(source=source, refs=refs, chunk_index=chunk_index)
        self._fetcher = fetcher

    @property
    def ids(self) -> list[FileIdentifierT]:
        return [ref.id for ref in self.refs]

    @property
    def index(self) -> int:
        return self.chunk_index

    def iter_documents(self) -> Iterator[DocumentStream]:
        if self._fetcher is None:
            raise RuntimeError("DocumentChunk does not have an attached fetcher.")
        for ref in self.refs:
            yield self._fetcher(ref.id)


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
    def _fetch_documents(self) -> Iterator[DocumentStream]:
        """Yield documents from the source."""

    def _list_document_ids(self) -> Iterator[FileIdentifierT] | None:
        return None

    def _fetch_document_by_id(self, identifier: FileIdentifierT) -> DocumentStream:
        raise NotImplementedError

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

    def fetch_document_by_ref(
        self, ref: SourceDocumentRef[FileIdentifierT]
    ) -> DocumentStream:
        return self._fetch_document_by_id(ref.id)

    def fetch_converter_source_by_ref(
        self, ref: SourceDocumentRef[FileIdentifierT]
    ) -> ConverterSource:
        return self.fetch_document_by_ref(ref)

    def headers_for_ref(
        self, ref: SourceDocumentRef[FileIdentifierT]
    ) -> dict[str, Any] | None:
        del ref
        return None

    def iterate_documents(self) -> Iterator[DocumentStream]:
        if not self._initialized:
            raise RuntimeError(
                "Processor not initialized. Use 'with' to open it first."
            )
        yield from self._fetch_documents()

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
                fetcher=self._fetch_document_by_id,
            )

            chunk_index += 1
            source_index += len(ids)
