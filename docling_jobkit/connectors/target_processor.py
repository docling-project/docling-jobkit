import json
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Literal, Optional

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
    def check_dependencies(cls) -> None:
        """Probe that all optional runtime dependencies for this connector are installed.

        Override in connectors that require an optional extra (e.g. ``opensearch``,
        ``azure``, ``gdrive``).  Raise :exc:`ImportError` if a required package is
        missing — ``defaults.py`` will catch this and skip registration, logging an
        INFO message that tells the operator which extra to install.

        The default implementation is a no-op (connector has no optional deps).
        """

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

    # ------------------------------------------------------------------
    # Streaming chunk protocol
    # ------------------------------------------------------------------

    @classmethod
    def requires_chunks(cls) -> bool:
        """Return True when this processor needs per-chunk records.

        ResultsProcessor will activate the chunker and drive the streaming
        chunk protocol (begin_chunks / consume_chunk / end_chunks) on this
        processor for every successfully converted document, regardless of
        whether ``"chunks"`` appears in ``to_formats``.

        File-storage targets should leave this as False and instead rely on
        ``"chunks"`` being listed in ``to_formats`` to receive chunk output.
        """
        return False

    def begin_chunks(
        self,
        filename: str,
        temp_dir: Path,
        chunk_target_key: Optional[str] = None,
        document_hash: Optional[str] = None,
    ) -> None:
        """Called once before the first chunk of a document is streamed.

        Default: open a temp ``{stem}.chunks.jsonl`` file for writing.
        DB processors that override ``requires_chunks()`` can use this to
        initialise per-document state instead.

        ``chunk_target_key``, when provided, is the storage path that
        ``end_chunks`` will pass to ``upload_file()`` as *target_filename*.
        When omitted the bare filename (``{stem}.chunks.jsonl``) is used,
        which is only correct for database processors that override
        ``end_chunks`` and never call ``upload_file()`` themselves.

        ``document_hash`` is the SHA-256 hex digest of the raw input file bytes.
        Processors that need stable, content-addressed chunk IDs should store
        this and combine it with ``chunk_index`` to derive the ID.
        """
        stem = Path(filename).stem
        self._chunk_jsonl_path: Optional[Path] = temp_dir / f"{stem}.chunks.jsonl"
        self._chunk_jsonl_file = self._chunk_jsonl_path.open("w", encoding="utf-8")
        self._chunk_target_key: Optional[str] = (
            chunk_target_key
            if chunk_target_key is not None
            else self._chunk_jsonl_path.name
        )

    def consume_chunk(self, chunk: "ChunkedDocumentResultItem") -> None:
        """Called once per chunk as it is produced by the chunker.

        Default: append one JSON line to the temp file opened in
        ``begin_chunks()``.  DB processors override to call ``upsert_row()``
        directly so each chunk is written to the store without buffering.

        This method must NEVER re-chunk the document — the chunk list is
        produced exactly once by ``ResultsProcessor`` and shared across all
        participating processors.
        """
        self._chunk_jsonl_file.write(
            json.dumps(chunk.model_dump(mode="json"), ensure_ascii=False) + "\n"
        )

    def end_chunks(self) -> None:
        """Called after the last chunk of a document has been consumed.

        Default: close the temp file and upload it via ``upload_file()``.
        DB processors override to close any open resources without uploading.
        """
        self._chunk_jsonl_file.close()
        self.upload_file(
            filename=self._chunk_jsonl_path,  # type: ignore[arg-type]
            target_filename=self._chunk_target_key,  # type: ignore[arg-type]
            content_type="application/jsonl",
        )
