from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from pathlib import Path
from typing import BinaryIO, Literal

from pydantic import BaseModel


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

    @classmethod
    def result_mode(cls) -> Literal["artifacts", "archive", "presigned", "database"]:
        return "artifacts"

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

    def upload_archive(self, filename: Path) -> None:
        self.upload_file(filename, filename.name, "application/zip")

    def begin_document(self, doc_id: str) -> None:
        """Signal the start of a new document's uploads.

        Processors that accumulate multiple format uploads into a single record
        (e.g. database targets) should override this to initialise per-document
        state.  File/object-storage targets can leave this as a no-op.
        """

    def end_document(self, doc_id: str) -> None:
        """Signal that all uploads for the current document are complete.

        Database targets should flush the accumulated row here.  File/object-
        storage targets can leave this as a no-op.
        """
