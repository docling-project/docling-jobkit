from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from itertools import islice
from typing import Callable, Generic, Iterator, Sequence, TypeVar

from docling.datamodel.base_models import DocumentStream

FileIdentifierT = TypeVar("FileIdentifierT")  # identifier type per connector


class DocumentChunk(Generic[FileIdentifierT]):
    def __init__(
        self,
        ids: Sequence[FileIdentifierT],
        fetcher: Callable[[FileIdentifierT], DocumentStream],
        chunk_index: int,
    ):
        self.ids = ids
        self._fetcher = fetcher
        self.index = chunk_index

    def iter_documents(self) -> Iterator[DocumentStream]:
        for doc_id in self.ids:
            yield self._fetcher(doc_id)


class BaseSourceProcessor(Generic[FileIdentifierT], AbstractContextManager, ABC):
    """
    Base class for source processors.
    Handles initialization state and context management.
    """

    def __init__(self):
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

    def _count_documents(self) -> int | None:
        return None

    def iterate_documents(self) -> Iterator[DocumentStream]:
        if not self._initialized:
            raise RuntimeError(
                "Processor not initialized. Use 'with' to open it first."
            )
        yield from self._fetch_documents()

    def iterate_document_chunks(
        self, chunk_size: int
    ) -> Iterator[DocumentChunk[FileIdentifierT]]:
        ids_gen = self._list_document_ids()
        if ids_gen is None:
            raise RuntimeError("Connector does not support chunking.")

        chunk_index = 0

        while True:
            ids = list(islice(ids_gen, chunk_size))
            if not ids:
                break

            yield DocumentChunk(
                ids=ids,
                fetcher=self._fetch_document_by_id,
                chunk_index=chunk_index,
            )

            chunk_index += 1
