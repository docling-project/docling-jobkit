from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO

from pydantic import BaseModel

if TYPE_CHECKING:
    from docling_jobkit.datamodel.result import ChunkedDocumentResultItem


class BaseTargetProcessor(AbstractContextManager, ABC):
    """
    Abstract base class for target processors that handle writing/uploading
    objects to a storage backend (e.g. S3, local FS, GCS).
    """

    def __init__(self):
        self._initialized = False

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        """Config (pydantic) models this processor accepts.

        Each returned model must carry a ``Literal`` ``kind`` field. The connector
        factory keys its registry on these types.
        """
        raise NotImplementedError(
            f"{cls.__name__} must implement get_config_types() to be registered "
            "as a connector plugin."
        )

    def __enter__(self):
        self._initialize()
        self._initialized = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._finalize()
        self._initialized = False

    @abstractmethod
    def _initialize(self) -> None:
        """
        Subclasses must implement setup (e.g. create boto3 client).
        """
        ...

    @abstractmethod
    def _finalize(self) -> None:
        """
        Subclasses must implement teardown (e.g. close client).
        """
        ...

    @abstractmethod
    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        """
        Upload a file from local filesystem.
        """
        ...

    @abstractmethod
    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
    ) -> None:
        """
        Upload an in-memory object (bytes or file-like) to the target.
        """
        ...

    def upload_chunks(
        self,
        chunks: "list[ChunkedDocumentResultItem]",
        doc_id: str,
        source_name: str,
    ) -> None:
        """Upload pre-built chunk items to the target.

        File-based targets (S3, local, …) do not store chunks and can leave
        this as a no-op.  Vector-store targets (AstraDB, …) override it.
        """
