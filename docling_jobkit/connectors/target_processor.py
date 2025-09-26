from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from pathlib import Path
from typing import BinaryIO


class BaseTargetProcessor(AbstractContextManager, ABC):
    """
    Abstract base class for target processors that handle writing/uploading
    objects to a storage backend (e.g. S3, local FS, GCS).
    """

    def __init__(self):
        self._initialized = False

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
