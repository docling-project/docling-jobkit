from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from typing import Iterator

from docling.datamodel.base_models import DocumentStream


class BaseSourceProcessor(AbstractContextManager, ABC):
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

    def iterate_documents(self) -> Iterator[DocumentStream]:
        if not self._initialized:
            raise RuntimeError(
                "Processor not initialized. Use 'with' to open it first."
            )
        yield from self._fetch_documents()
