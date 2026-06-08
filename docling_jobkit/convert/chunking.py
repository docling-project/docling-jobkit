import hashlib
import json
import logging
import os
import shutil
import threading
import time
import warnings
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

import httpx
from pydantic import BaseModel, Field

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.document import ConversionResult
from docling.datamodel.service.callbacks import (
    ProcessedDocsItem,
    ProgressDocumentCompleted,
    ProgressSetNumDocs,
    ProgressUpdateProcessed,
)
from docling.datamodel.service.chunking import (
    BaseChunkerOptions,
    HierarchicalChunkerOptions,
    HybridChunkerOptions,
)
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.targets import InBodyTarget, PutTarget
from docling_core.transforms.chunker import BaseChunker
from docling_core.transforms.chunker.hierarchical_chunker import (
    ChunkingDocSerializer,
    ChunkingSerializerProvider,
    DocChunk,
    HierarchicalChunker,
)
from docling_core.transforms.chunker.hybrid_chunker import HybridChunker
from docling_core.transforms.chunker.tokenizer.huggingface import (
    HuggingFaceTokenizer,
)
from docling_core.transforms.serializer.markdown import (
    MarkdownTableSerializer,
)
from docling_core.types.doc.document import DoclingDocument, ImageRefMode

from docling_jobkit.convert.results import (
    _build_document_completed_item,
    _export_document_as_content,
)
from docling_jobkit.datamodel.exportable_document import ExportableDocument
from docling_jobkit.datamodel.result import (
    ChunkedDocumentResult,
    ChunkedDocumentResultItem,
    DoclingTaskResult,
    DocumentResultItem,
    ExportDocumentResponse,
    RemoteTargetResult,
    ResultType,
    ZipArchiveResult,
)
from docling_jobkit.datamodel.task import Task
from docling_jobkit.public_errors import render_public_error_list

if TYPE_CHECKING:
    from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker

_log = logging.getLogger(__name__)


class MarkdownChunkingSerializerProvider(ChunkingSerializerProvider):
    """Custom serializer provider that can be configured to use markdown serializers based on chunking options."""

    def __init__(
        self,
        *,
        use_markdown_tables: bool = False,
        use_markdown_images: bool = False,
        image_placeholder: Optional[str] = None,
    ):
        self._use_markdown_tables = use_markdown_tables
        self._use_markdown_images = use_markdown_images
        self._image_placeholder = image_placeholder

    def get_serializer(self, doc: DoclingDocument):
        serializers: dict[str, Any] = {}
        if self._use_markdown_tables:
            serializers["table_serializer"] = MarkdownTableSerializer()
        markdownParams = ChunkingDocSerializer.model_fields["params"].default
        if self._use_markdown_images:
            markdownParams = markdownParams.model_copy(
                update={"image_placeholder": self._image_placeholder}
            )

        return ChunkingDocSerializer(doc=doc, params=markdownParams, **serializers)


class DocumentChunkerConfig(BaseModel):
    """Configuration for DocumentChunker."""

    cache_size: int = Field(
        default=10,
        gt=0,
        le=100,
        description="Maximum number of chunker instances to cache",
    )


class DocumentChunkerManager:
    """Handles document chunking for RAG workflows using chunkers from docling-core."""

    def __init__(
        self,
        config: Optional[DocumentChunkerConfig] = None,
    ):
        self.config = config or DocumentChunkerConfig()
        self._cache_lock = threading.Lock()
        self._options_map: dict[bytes, BaseChunkerOptions] = {}
        self._get_chunker_from_cache = self._create_chunker_cache()

    def _create_chunker_cache(self):
        """Create LRU cache for chunker instances."""

        @lru_cache(maxsize=self.config.cache_size)
        def _get_chunker_from_cache(cache_key: bytes) -> BaseChunker:
            try:
                options = self._options_map[cache_key]

                # Create serializer provider based on markdown table option
                serializer_provider = MarkdownChunkingSerializerProvider(
                    use_markdown_tables=options.use_markdown_tables,
                    use_markdown_images=options.use_markdown_images,
                    image_placeholder=options.image_placeholder,
                )

                if isinstance(options, HybridChunkerOptions):
                    # Create tokenizer
                    tokenizer_name = options.tokenizer
                    tokenizer_obj = HuggingFaceTokenizer.from_pretrained(
                        model_name=tokenizer_name,
                        max_tokens=options.max_tokens,
                    )

                    chunker: BaseChunker = HybridChunker(
                        tokenizer=tokenizer_obj,
                        merge_peers=options.merge_peers,
                        serializer_provider=serializer_provider,
                    )
                elif isinstance(options, HierarchicalChunkerOptions):
                    chunker = HierarchicalChunker(
                        serializer_provider=serializer_provider
                    )
                else:
                    raise RuntimeError(f"Unknown chunker {options.chunker}.")

                return chunker

            except ImportError as e:
                _log.error(f"Missing dependencies for document chunking: {e}")
                raise ImportError(
                    "Document chunking requires docling-core with chunking dependencies. "
                    "Install with: pip install 'docling-core[chunking]'"
                ) from e
            except (ValueError, TypeError, AttributeError) as e:
                _log.error(f"Invalid chunking configuration: {e}")
                raise ValueError(f"Invalid chunking options: {e}") from e
            except (OSError, ConnectionError) as e:
                _log.error(f"Resource or connection error during chunker creation: {e}")
                raise RuntimeError(
                    f"Failed to initialize chunker resources: {e}"
                ) from e

        return _get_chunker_from_cache

    def _get_chunker(
        self,
        options: BaseChunkerOptions,
    ) -> BaseChunker:
        """Get or create a cached BaseChunker instance."""
        cache_key = self._generate_cache_key(options)

        with self._cache_lock:
            self._options_map[cache_key] = options
            return self._get_chunker_from_cache(cache_key)

    def _generate_cache_key(
        self,
        options: BaseChunkerOptions,
    ) -> bytes:
        """Generate a deterministic cache key from chunking options."""
        # BasechunkerOptions will have the image_placeholder options that way we only need the basechunker options to generate the cache key.
        chunking_data = options.model_dump_json(serialize_as_any=True)

        return hashlib.sha1(chunking_data.encode(), usedforsecurity=False).digest()

    def clear_cache(self):
        """Clear the chunker cache."""
        with self._cache_lock:
            self._get_chunker_from_cache.cache_clear()

    def chunk_document(
        self,
        document: DoclingDocument,
        filename: str,
        options: BaseChunkerOptions,
    ) -> Iterable[ChunkedDocumentResultItem]:
        """Chunk a document using chunker from docling-core."""

        chunker = self._get_chunker(options)

        chunks = list(chunker.chunk(document))

        # Convert chunks to response format
        chunk_items: list[ChunkedDocumentResultItem] = []
        for i, chunk in enumerate(chunks):
            page_numbers: List[int] = []
            metadata: Dict[str, Any] = {}

            doc_chunk = DocChunk.model_validate(chunk)

            # Extract page numbers and doc_items refs
            page_numbers = []
            doc_items = []
            for item in doc_chunk.meta.doc_items:
                doc_items.append(item.self_ref)
                for prov in item.prov:
                    page_numbers.append(prov.page_no)

            # Remove duplicates and sort
            page_numbers = sorted(set(page_numbers))

            # Store additional metadata
            if doc_chunk.meta.origin:
                metadata["origin"] = doc_chunk.meta.origin

            metadata["has_image"] = any(
                item.self_ref.startswith("#/pictures/")
                for item in doc_chunk.meta.doc_items
            )

            # Get the text
            text = chunker.contextualize(doc_chunk)

            # Compute the number of tokens
            num_tokens: int | None = None
            if isinstance(chunker, HybridChunker):
                num_tokens = chunker.tokenizer.count_tokens(text)

            # Create chunk item
            chunk_item = ChunkedDocumentResultItem(
                filename=filename,
                chunk_index=i,
                text=text,
                raw_text=doc_chunk.text if options.include_raw_text else None,
                num_tokens=num_tokens,
                headings=doc_chunk.meta.headings,
                captions=doc_chunk.meta.captions,
                doc_items=doc_items,
                page_numbers=page_numbers,
                metadata=metadata,
            )
            chunk_items.append(chunk_item)

        return chunk_items


def _export_document_for_chunking(
    exportable_document: ExportableDocument,
    output_dir: Path,
    image_mode: ImageRefMode,
) -> ExportDocumentResponse:
    """Extract document content for chunking and ensure artifacts are on disk.

    If ``image_mode`` is ``REFERENCED``, a temporary JSON export is performed
    so that the ``artifacts/`` directory is created as a side-effect; the
    temporary file is then removed because the converted content is already
    embedded inside ``chunked_result.json``.
    """
    document = ExportDocumentResponse(filename=exportable_document.file.name)

    if (
        exportable_document.status
        in (
            ConversionStatus.SUCCESS,
            ConversionStatus.PARTIAL_SUCCESS,
        )
        and exportable_document.document is not None
    ):
        artifacts_dir = output_dir / "artifacts"
        new_doc = exportable_document.document._make_copy_with_refmode(
            artifacts_dir,
            image_mode,
            page_no=None,
            reference_path=output_dir,
        )
        document.json_content = new_doc

        if image_mode == ImageRefMode.REFERENCED:
            temp_fname = output_dir / f"{exportable_document.file.stem}.json"
            exportable_document.document.save_as_json(
                filename=temp_fname,
                image_mode=image_mode,
                artifacts_dir=artifacts_dir,
            )
            temp_fname.unlink(missing_ok=True)

    return document


def _export_chunking_result(
    result: ChunkedDocumentResult,
    output_dir: Path,
) -> None:
    """Write the consolidated chunking result as ``chunked_result.json``."""
    fname = output_dir / "chunked_result.json"
    _log.info(f"writing chunk output to {fname}")
    with fname.open("w", encoding="utf-8") as f:
        json.dump(
            result.model_dump(mode="json"),
            f,
            ensure_ascii=False,
            indent=2,
        )


def process_chunkable_results(
    task: Task,
    exportable_documents: Iterable[ExportableDocument],
    work_dir: Path,
    chunker_manager: Optional[DocumentChunkerManager] = None,
    callback_invoker: Optional["CallbackInvoker"] = None,
    debug_error_details: bool = False,
    expected_doc_count: Optional[int] = None,
    start_time: Optional[float] = None,
) -> DoclingTaskResult:
    # Let's start by processing the documents
    start_time = start_time if start_time is not None else time.monotonic()
    chunking_options = task.chunking_options or HybridChunkerOptions()
    conversion_options = task.convert_options or ConvertDocumentsOptions()

    # 1. Send ProgressSetNumDocs at start
    total_docs = (
        expected_doc_count if expected_doc_count is not None else len(task.sources)
    )
    if callback_invoker and task.callbacks and total_docs:
        callback_invoker.invoke_callbacks_async(
            callbacks=task.callbacks,
            task_id=task.task_id,
            progress=ProgressSetNumDocs(num_docs=total_docs),
        )

    # We have some results, let's prepare the response
    task_result: ResultType
    chunks: list[ChunkedDocumentResultItem] = []
    documents: list[DocumentResultItem] = []
    num_succeeded = 0
    num_failed = 0
    docs: list[ProcessedDocsItem] = []

    # TODO: DocumentChunkerManager should be initialized outside for really working as a cache
    chunker_manager = chunker_manager or DocumentChunkerManager()

    output_dir: Optional[Path] = None
    if not isinstance(task.target, InBodyTarget):
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

    for idx, exportable_document in enumerate(exportable_documents):
        _log.debug(
            "Document %s | status=%s | errors=%s",
            exportable_document.file.name,
            exportable_document.status,
            exportable_document.errors,
        )
        errors = exportable_document.errors
        filename = exportable_document.file.name
        if (
            exportable_document.status == ConversionStatus.SUCCESS
            and exportable_document.document is not None
        ):
            try:
                chunks.extend(
                    chunker_manager.chunk_document(
                        document=exportable_document.document,
                        filename=filename,
                        options=chunking_options,
                    )
                )
                num_succeeded += 1
            except Exception as e:
                _log.exception(
                    f"Document chunking failed for {exportable_document.file}: {e}",
                    stack_info=True,
                )
                num_failed += 1
                # TODO: for propagating errors we have first to allow other component_type in the Docling class.
                # errors = [
                #     *errors,
                #     ErrorItem(
                #         component_type="chunking",
                #         module_name=type(e).__name__,
                #         error_message=str(e),
                #     ),
                # ]

        else:
            _log.warning(f"Document {exportable_document.file} failed to convert.")
            num_failed += 1

        summary_error = render_public_error_list(
            errors,
            debug_enabled=debug_error_details,
        )
        docs.append(
            ProcessedDocsItem(
                source=str(exportable_document.file),
                status=exportable_document.status,
                error=summary_error
                or (
                    "Unknown error"
                    if exportable_document.status != ConversionStatus.SUCCESS
                    else None
                ),
            )
        )

        # 2. Send per-document callback (non-blocking)
        if callback_invoker and task.callbacks:
            document_info = _build_document_completed_item(
                exportable_document,
                error=render_public_error_list(
                    errors,
                    debug_enabled=debug_error_details,
                ),
            )

            callback_invoker.invoke_callbacks_async(
                callbacks=task.callbacks,
                task_id=task.task_id,
                progress=ProgressDocumentCompleted(
                    document=document_info,
                    total_processed=idx + 1,
                    total_docs=total_docs,
                ),
            )

        if task.chunking_export_options.include_converted_doc:
            if (
                isinstance(task.target, InBodyTarget)
                and conversion_options.image_export_mode == ImageRefMode.REFERENCED
            ):
                raise RuntimeError("InBodyTarget cannot use REFERENCED image mode.")

            if isinstance(task.target, InBodyTarget):
                doc_content = _export_document_as_content(
                    exportable_document,
                    export_json=True,
                    export_doctags=False,
                    export_html=False,
                    export_md=False,
                    export_txt=False,
                    image_mode=conversion_options.image_export_mode,
                    md_page_break_placeholder=conversion_options.md_page_break_placeholder,
                )
            elif output_dir is not None:
                doc_content = _export_document_for_chunking(
                    exportable_document,
                    output_dir=output_dir,
                    image_mode=conversion_options.image_export_mode,
                )
        else:
            doc_content = ExportDocumentResponse(filename=filename)

        doc_result = DocumentResultItem(
            document=doc_content,
            status=exportable_document.status,
            timings=exportable_document.timings,
            errors=errors,
        )

        documents.append(doc_result)
    num_total = num_succeeded + num_failed
    # Task-level wall clock elapsed time across the whole request.
    processing_time = time.monotonic() - start_time
    _log.info(
        f"Processed {num_total} docs generating {len(chunks)} chunks in {processing_time:.2f} seconds."
    )

    # 3. Send ProgressUpdateProcessed at end with final summary
    if callback_invoker and task.callbacks:
        callback_invoker.invoke_callbacks_async(
            callbacks=task.callbacks,
            task_id=task.task_id,
            progress=ProgressUpdateProcessed(
                num_processed=len(docs),
                num_succeeded=num_succeeded,
                num_partially_succeeded=0,
                num_failed=num_failed,
                docs=docs,
            ),
        )

    # Export results based on target type and options
    # Booleans to know what to export
    if isinstance(task.target, InBodyTarget):
        task_result = ChunkedDocumentResult(
            chunks=chunks,
            documents=documents,
            processing_time=processing_time,
            chunking_info=chunking_options.model_dump(mode="json"),
        )

    # Multiple documents were processed, or we are forced returning as a file
    elif output_dir is not None:
        # Export the consolidated chunking result (including artifacts)
        chunked_result = ChunkedDocumentResult(
            chunks=chunks,
            documents=documents,
            processing_time=processing_time,
            chunking_info=chunking_options.model_dump(mode="json"),
        )
        _export_chunking_result(
            result=chunked_result,
            output_dir=output_dir,
        )
        files = os.listdir(output_dir)
        if len(files) == 0:
            raise RuntimeError("No documents were exported.")
        file_path = work_dir / "converted_docs.zip"
        shutil.make_archive(
            base_name=str(file_path.with_suffix("")),
            format="zip",
            root_dir=output_dir,
        )
        if isinstance(task.target, PutTarget):
            try:
                with file_path.open("rb") as file_data:
                    r = httpx.put(str(task.target.url), files={"file": file_data})
                    r.raise_for_status()
                task_result = RemoteTargetResult()
            except Exception as exc:
                _log.error("An error occour while uploading zip to s3", exc_info=exc)
                raise RuntimeError(
                    "An error occour while uploading zip to the target url."
                )
        else:
            task_result = ZipArchiveResult(content=file_path.read_bytes())

    return DoclingTaskResult(
        result=task_result,
        processing_time=processing_time,
        num_succeeded=num_succeeded,
        num_partially_succeeded=0,
        num_failed=num_failed,
        num_converted=num_total,
    )


def process_chunk_results(
    task: Task,
    conv_results: Iterable[ConversionResult],
    work_dir: Path,
    chunker_manager: Optional[DocumentChunkerManager] = None,
    callback_invoker: Optional["CallbackInvoker"] = None,
) -> DoclingTaskResult:
    warnings.warn(
        "process_chunk_results() is deprecated; use process_chunkable_results()",
        DeprecationWarning,
        stacklevel=2,
    )
    return process_chunkable_results(
        task=task,
        exportable_documents=(
            ExportableDocument.from_conversion_result(conv_res)
            for conv_res in conv_results
        ),
        work_dir=work_dir,
        chunker_manager=chunker_manager,
        callback_invoker=callback_invoker,
    )
