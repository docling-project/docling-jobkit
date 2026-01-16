from pathlib import Path
from typing import Iterator, TypedDict

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.source_processor import BaseSourceProcessor
from docling_jobkit.datamodel.task_sources import TaskLocalPathSource


def _should_ignore_file(file_path: Path) -> bool:
    """
    Check if a file should be ignored based on common patterns for
    hidden files, temporary files, and system metadata files.

    Returns True if the file should be ignored, False otherwise.
    """
    name = file_path.name

    # Hidden files (starting with .)
    if name.startswith("."):
        return True

    # Vim temporary files
    if name.endswith(("~", ".swp", ".swo")):
        return True

    # Emacs temporary files
    if name.startswith("#") and name.endswith("#"):
        return True

    # Microsoft Office temporary files
    if name.startswith("~$"):
        return True

    # Windows thumbnail cache
    if name.lower() == "thumbs.db":
        return True

    # Desktop.ini (Windows)
    if name.lower() == "desktop.ini":
        return True

    return False


class LocalPathFileIdentifier(TypedDict):
    path: Path
    size: int
    last_modified: float


class LocalPathSourceProcessor(BaseSourceProcessor[LocalPathFileIdentifier]):
    def __init__(self, source: TaskLocalPathSource):
        super().__init__()
        self._source = source

    def _initialize(self):
        """Validate that the path exists."""
        if not self._source.path.exists():
            raise FileNotFoundError(f"Path does not exist: {self._source.path}")

    def _finalize(self):
        """No cleanup needed for local filesystem."""

    def _list_document_ids(self) -> Iterator[LocalPathFileIdentifier]:
        """
        List all files based on the source configuration.
        - If path is a file, yield that single file
        - If path is a directory, discover files based on pattern and recursive settings
        """
        path = self._source.path

        if path.is_file():
            # Single file case
            stat = path.stat()
            yield LocalPathFileIdentifier(
                path=path,
                size=stat.st_size,
                last_modified=stat.st_mtime,
            )
        elif path.is_dir():
            # Directory case - use glob or rglob based on recursive setting
            if self._source.recursive:
                # Recursive traversal
                files = path.rglob(self._source.pattern)
            else:
                # Non-recursive traversal
                files = path.glob(self._source.pattern)

            for file_path in files:
                # Only yield actual files, not directories
                # Skip hidden files, temporary files, and system metadata
                if file_path.is_file() and not _should_ignore_file(file_path):
                    stat = file_path.stat()
                    yield LocalPathFileIdentifier(
                        path=file_path,
                        size=stat.st_size,
                        last_modified=stat.st_mtime,
                    )
        else:
            raise ValueError(f"Path is neither a file nor a directory: {path}")

    def _count_documents(self) -> int:
        """Count total number of documents."""
        return sum(1 for _ in self._list_document_ids())

    def _fetch_document_by_id(
        self, identifier: LocalPathFileIdentifier
    ) -> DocumentStream:
        """Fetch a document by opening the file from the local filesystem."""
        file_path = identifier["path"]

        # Open file in binary mode and return as DocumentStream
        with open(file_path, "rb") as f:
            content = f.read()

        from io import BytesIO

        buffer = BytesIO(content)

        return DocumentStream(name=str(file_path), stream=buffer)

    def _fetch_documents(self) -> Iterator[DocumentStream]:
        """Iterate through all documents."""
        for identifier in self._list_document_ids():
            yield self._fetch_document_by_id(identifier)
