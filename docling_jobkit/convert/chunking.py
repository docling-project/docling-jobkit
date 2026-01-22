import hashlib
import logging
import threading
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from pydantic import BaseModel, Field

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.document import ConversionResult
from docling_core.transforms.chunker import BaseChunker
from docling_core.transforms.chunker.hierarchical_chunker import (
    ChunkingSerializerProvider,
    DocChunk,
    HierarchicalChunker,
)
from docling_core.transforms.chunker.hybrid_chunker import HybridChunker
from docling_core.transforms.chunker.tokenizer.huggingface import (
    HuggingFaceTokenizer,
)
from docling_core.types.doc.document import DoclingDocument, ImageRefMode

from docling_jobkit.convert.results import _export_document_as_content
from docling_jobkit.datamodel.chunking import (
    BaseChunkerOptions,
    HierarchicalChunkerOptions,
    HybridChunkerOptions,
)
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.result import (
    ChunkedDocumentResult,
    ChunkedDocumentResultItem,
    DoclingTaskResult,
    ExportDocumentResponse,
    ExportResult,
)
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_targets import InBodyTarget

_log = logging.getLogger(__name__)


class MarkdownTableSerializerProvider(ChunkingSerializerProvider):
    """Serializer provider that uses markdown table format for table serialization."""

    def get_serializer(self, doc):
        """Get a serializer that uses markdown table format."""
        from docling_core.transforms.chunker.hierarchical_chunker import (
            ChunkingDocSerializer,
        )
        from docling_core.transforms.serializer.markdown import MarkdownTableSerializer

        return ChunkingDocSerializer(
            doc=doc,
            table_serializer=MarkdownTableSerializer(),
        )


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

    def __init__(self, config: Optional[DocumentChunkerConfig] = None):
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
                if options.use_markdown_tables:
                    serializer_provider: ChunkingSerializerProvider = (
                        MarkdownTableSerializerProvider()
                    )
                else:
                    serializer_provider = ChunkingSerializerProvider()

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

    def _get_chunker(self, options: BaseChunkerOptions) -> BaseChunker:
        """Get or create a cached BaseChunker instance."""
        # Create a cache key based on chunking options using the same pattern as the repo
        cache_key = self._generate_cache_key(options)

        with self._cache_lock:
            self._options_map[cache_key] = options
            return self._get_chunker_from_cache(cache_key)

    def _generate_cache_key(self, options: BaseChunkerOptions) -> bytes:
        """Generate a deterministic cache key from chunking options."""
        serialized_data = options.model_dump_json(serialize_as_any=True)
        options_hash = hashlib.sha1(
            serialized_data.encode(), usedforsecurity=False
        ).digest()
        return options_hash

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


def process_chunk_results(
    task: Task,
    conv_results: Iterable[ConversionResult],
    work_dir: Path,
    chunker_manager: Optional[DocumentChunkerManager] = None,
) -> DoclingTaskResult:
    # Let's start by processing the documents
    start_time = time.monotonic()
    chunking_options = task.chunking_options or HybridChunkerOptions()
    conversion_options = task.convert_options or ConvertDocumentsOptions()

    # We have some results, let's prepare the response
    task_result: ChunkedDocumentResult
    chunks: list[ChunkedDocumentResultItem] = []
    documents: list[ExportResult] = []
    num_succeeded = 0
    num_failed = 0

    # TODO: DocumentChunkerManager should be initialized outside for really working as a cache
    chunker_manager = chunker_manager or DocumentChunkerManager()
    for conv_res in conv_results:
        errors = conv_res.errors
        filename = conv_res.input.file.name
        if conv_res.status == ConversionStatus.SUCCESS:
            try:
                chunks.extend(
                    chunker_manager.chunk_document(
                        document=conv_res.document,
                        filename=filename,
                        options=chunking_options,
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

        if (
            isinstance(task.target, InBodyTarget)
            and task.chunking_export_options.include_converted_doc
        ):
            if conversion_options.image_export_mode == ImageRefMode.REFERENCED:
                raise RuntimeError("InBodyTarget cannot use REFERENCED image mode.")

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
        else:
            doc_content = ExportDocumentResponse(filename=filename)

        doc_result = ExportResult(
            content=doc_content,
            status=conv_res.status,
            timings=conv_res.timings,
            errors=errors,
        )

        documents.append(doc_result)

    num_total = num_succeeded + num_failed
    processing_time = time.monotonic() - start_time
    _log.info(
        f"Processed {num_total} docs generating {len(chunks)} chunks in {processing_time:.2f} seconds."
    )

    if isinstance(task.target, InBodyTarget):
        task_result = ChunkedDocumentResult(
            chunks=chunks,
            documents=documents,
            processing_time=processing_time,
            chunking_info=chunking_options.model_dump(mode="json"),
        )

    # Multiple documents were processed, or we are forced returning as a file
    else:
        raise NotImplementedError("Saving chunks to a file is not yet supported.")

    return DoclingTaskResult(
        result=task_result,
        processing_time=processing_time,
        num_succeeded=num_succeeded,
        num_failed=num_failed,
        num_converted=num_total,
    )
