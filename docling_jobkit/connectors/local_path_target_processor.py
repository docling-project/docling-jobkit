from pathlib import Path
from typing import BinaryIO

from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.task_targets import LocalPathTarget


class LocalPathTargetProcessor(BaseTargetProcessor):
    def __init__(self, target: LocalPathTarget):
        super().__init__()
        self._target = target

    def _initialize(self) -> None:
        """
        Ensure the target directory exists.
        If path is a directory, create it. If it's a file path, create parent directories.
        """
        path = self._target.path

        # If path looks like a directory (ends with / or has no extension), treat as directory
        # Otherwise, create parent directories for the file
        if path.suffix == "" or str(path).endswith("/"):
            # Treat as directory
            path.mkdir(parents=True, exist_ok=True)
        else:
            # Treat as file - create parent directories
            path.parent.mkdir(parents=True, exist_ok=True)

    def _finalize(self) -> None:
        """No cleanup needed for local filesystem."""

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        """
        Copy a file from local filesystem to the target location.
        """
        source_path = Path(filename)
        target_path = self._get_target_path(target_filename)

        # Ensure parent directory exists
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy file content
        with open(source_path, "rb") as src:
            with open(target_path, "wb") as dst:
                dst.write(src.read())

    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
    ) -> None:
        """
        Write an in-memory object (bytes or file-like) to the target location.
        """
        target_path = self._get_target_path(target_filename)

        # Ensure parent directory exists
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # Write content based on type
        if isinstance(obj, str):
            with open(target_path, "w", encoding="utf-8") as f:
                f.write(obj)
        elif isinstance(obj, (bytes, bytearray)):
            with open(target_path, "wb") as f:
                f.write(obj)
        else:
            # Assume it's a file-like object
            with open(target_path, "wb") as f:
                f.write(obj.read())

    def _get_target_path(self, target_filename: str) -> Path:
        """
        Determine the full target path based on the configured path.
        - If path is a directory, append target_filename
        - If path is a file, use it directly (ignore target_filename)
        """
        path = self._target.path

        # Check if path is intended to be a directory
        if path.is_dir() or path.suffix == "" or str(path).endswith("/"):
            # Treat as directory - append target_filename
            return path / target_filename
        else:
            # Treat as file - use the path directly
            return path
