import hashlib
import json
import logging
import os
import shutil
import threading
import time
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

import httpx
from pydantic import BaseModel, Field

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.document import ConversionResult
from docling.datamodel.service.callbacks import (
    DocumentCompletedItem,
    FailedDocsItem,
    ProgressDocumentCompleted,
    ProgressSetNumDocs,
    ProgressUpdateProcessed,
    SucceededDocsItem,
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
    _export_document_as_content,
)
from docling_jobkit.datamodel.result import (
    ChunkedDocumentResult,
    ChunkedDocumentResultItem,
    DoclingTaskResult,
    ExportDocumentResponse,
    ExportResult,
    RemoteTargetResult,
    ResultType,
    ZipArchiveResult,
)
from docling_jobkit.datamodel.task import Task

if TYPE_CHECKING:
    from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker

_log = logging.getLogger(__name__)

DEFAULT_IMAGE_PLACEHOLDER = "![Image]"
# TODO: Once md_image_placeholder is exposed in ConvertDocumentsOptions
# (docling main library), replace this constant with the user-provided
# runtime value passed through the options object.


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
        params = None
        default = ChunkingDocSerializer.model_fields["params"].default
        if self._use_markdown_images:
            params = default.model_copy(
                update={
                    "image_placeholder": self._image_placeholder
                    or DEFAULT_IMAGE_PLACEHOLDER
                }
            )
        return ChunkingDocSerializer(doc=doc, params=params, **serializers)


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
        self._options_map: dict[
            bytes, tuple[BaseChunkerOptions, ConvertDocumentsOptions]
        ] = {}
        self._get_chunker_from_cache = self._create_chunker_cache()

    def _create_chunker_cache(self):
        """Create LRU cache for chunker instances."""

        @lru_cache(maxsize=self.config.cache_size)
        def _get_chunker_from_cache(cache_key: bytes) -> BaseChunker:
            try:
                options, conversion_options = self._options_map[cache_key]

                use_markdown_tables = options.use_markdown_tables
                use_markdown_images = False
                if conversion_options.image_export_mode != ImageRefMode.PLACEHOLDER:
                    use_markdown_images = True
                _log.debug(
                    f"Using serializer options - Markdown Tables: {use_markdown_tables}, Markdown Images: {use_markdown_images}"
                )
                # Create serializer provider based on markdown table option
                serializer_provider = MarkdownChunkingSerializerProvider(
                    use_markdown_tables=use_markdown_tables,
                    use_markdown_images=use_markdown_images,
                    # TODO: Pass the image placeholder from the chunking options once it's exposed in the main docling library. For now, we use a default constant.
                    image_placeholder=DEFAULT_IMAGE_PLACEHOLDER,
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
        conversion_options: ConvertDocumentsOptions,
    ) -> BaseChunker:
        """Get or create a cached BaseChunker instance."""
        cache_key = self._generate_cache_key(options, conversion_options)

        with self._cache_lock:
            self._options_map[cache_key] = (options, conversion_options)
            return self._get_chunker_from_cache(cache_key)

    def _generate_cache_key(
        self,
        options: BaseChunkerOptions,
        conversion_options: ConvertDocumentsOptions,
    ) -> bytes:
        """Generate a deterministic cache key from chunking options."""
        key_data = json.dumps(
            {
                "chunker": json.loads(options.model_dump_json(serialize_as_any=True)),
                "img_mode": conversion_options.image_export_mode.value,
                "img_placeholder": DEFAULT_IMAGE_PLACEHOLDER,  # This should be replaced with the actual placeholder from options once it's exposed in the main library
            },
            sort_keys=True,
        )
        return hashlib.sha1(key_data.encode(), usedforsecurity=False).digest()

    def clear_cache(self):
        """Clear the chunker cache."""
        with self._cache_lock:
            self._get_chunker_from_cache.cache_clear()

    def chunk_document(
        self,
        document: DoclingDocument,
        filename: str,
        options: BaseChunkerOptions,
        conversion_options: ConvertDocumentsOptions,
    ) -> Iterable[ChunkedDocumentResultItem]:
        """Chunk a document using chunker from docling-core."""

        chunker = self._get_chunker(options, conversion_options)

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
    conv_res: ConversionResult,
    output_dir: Path,
    image_mode: ImageRefMode,
) -> ExportDocumentResponse:
    """Extract document content for chunking and ensure artifacts are on disk.

    If ``image_mode`` is ``REFERENCED``, a temporary JSON export is performed
    so that the ``artifacts/`` directory is created as a side-effect; the
    temporary file is then removed because the converted content is already
    embedded inside ``chunked_result.json``.
    """
    document = ExportDocumentResponse(filename=conv_res.input.file.name)

    if conv_res.status == ConversionStatus.SUCCESS:
        artifacts_dir = output_dir / "artifacts"
        new_doc = conv_res.document._make_copy_with_refmode(
            artifacts_dir,
            image_mode,
            page_no=None,
            reference_path=output_dir,
        )
        document.json_content = new_doc

        if image_mode == ImageRefMode.REFERENCED:
            temp_fname = output_dir / f"{conv_res.input.file.stem}.json"
            conv_res.document.save_as_json(
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


def process_chunk_results(
    task: Task,
    conv_results: Iterable[ConversionResult],
    work_dir: Path,
    chunker_manager: Optional[DocumentChunkerManager] = None,
    callback_invoker: Optional["CallbackInvoker"] = None,
) -> DoclingTaskResult:
    # Let's start by processing the documents
    start_time = time.monotonic()
    chunking_options = task.chunking_options or HybridChunkerOptions()
    conversion_options = task.convert_options or ConvertDocumentsOptions()

    # 1. Send ProgressSetNumDocs at start
    total_docs = len(task.sources)
    if callback_invoker and task.callbacks and total_docs:
        callback_invoker.invoke_callbacks_async(
            callbacks=task.callbacks,
            task_id=task.task_id,
            progress=ProgressSetNumDocs(num_docs=total_docs),
        )

    # We have some results, let's prepare the response
    task_result: ResultType
    chunks: list[ChunkedDocumentResultItem] = []
    conv_results_list = []
    documents: list[ExportResult] = []
    num_succeeded = 0
    num_failed = 0
    docs_succeeded: list[SucceededDocsItem] = []
    docs_failed: list[FailedDocsItem] = []

    # TODO: DocumentChunkerManager should be initialized outside for really working as a cache
    chunker_manager = chunker_manager or DocumentChunkerManager()

    output_dir: Optional[Path] = None
    if not isinstance(task.target, InBodyTarget):
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

    for idx, conv_res in enumerate(conv_results):
        # Add detailed logging for each document conversion result , in case errors in conversion or chunking occur we can easily identify the bug.
        _log.debug(
            f"Document {conv_res.input.file.name} | status={conv_res.status} | errors={conv_res.errors}"
        )
        errors = conv_res.errors
        # Document has JUST been converted (lazy evaluation triggered here)
        conv_results_list.append(conv_res)
        filename = conv_res.input.file.name
        if conv_res.status == ConversionStatus.SUCCESS:
            try:
                chunks.extend(
                    chunker_manager.chunk_document(
                        document=conv_res.document,
                        filename=filename,
                        options=chunking_options,
                        conversion_options=conversion_options,
                    )
                )
                num_succeeded += 1
            except Exception as e:
                _log.exception(
                    f"Document chunking failed for {conv_res.input.file}: {e}",
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
            _log.warning(f"Document {conv_res.input.file} failed to convert.")
            num_failed += 1

        # Track for final summary
        if conv_res.status == ConversionStatus.SUCCESS:
            docs_succeeded.append(SucceededDocsItem(source=str(conv_res.input.file)))
        else:
            docs_failed.append(
                FailedDocsItem(
                    source=str(conv_res.input.file),
                    error=str(errors) if errors else "Unknown error",
                )
            )

        # 2. Send per-document callback (non-blocking)
        if callback_invoker and task.callbacks:
            document_info = DocumentCompletedItem(
                source=str(conv_res.input.file),
                status=conv_res.status,
                num_pages=(len(conv_res.document.pages) if conv_res.document else None),
                processing_time=(
                    sum(sum(item.times) for item in conv_res.timings.values())
                    if conv_res.timings
                    else None
                ),
                doc_hash=conv_res.input.document_hash,
                error=str(errors) if errors else None,
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
                    conv_res,
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
                    conv_res,
                    output_dir=output_dir,
                    image_mode=conversion_options.image_export_mode,
                )
        else:
            doc_content = ExportDocumentResponse(filename=filename)

        doc_result = ExportResult(
            content=doc_content,
            status=conv_res.status,
            timings=conv_res.timings,
            errors=errors,
        )

        documents.append(doc_result)

    conv_results = conv_results_list
    num_total = num_succeeded + num_failed
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
                num_processed=len(docs_succeeded) + len(docs_failed),
                num_succeeded=len(docs_succeeded),
                num_failed=len(docs_failed),
                docs_succeeded=docs_succeeded,
                docs_failed=docs_failed,
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
        num_failed=num_failed,
        num_converted=num_total,
    )
