import hashlib
import json
import logging
import threading
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from pydantic import BaseModel, Field

from docling.datamodel.base_models import ConversionStatus, ErrorItem
from docling.datamodel.document import ConversionResult
from docling_core.types.doc.document import DoclingDocument

from docling_jobkit.datamodel.chunking import (
    ChunkedDocumentConvertDetail,
    ChunkedDocumentResponse,
    ChunkedDocumentResponseItem,
    ChunkingOptions,
)
from docling_jobkit.datamodel.result import DoclingTaskResult
from docling_jobkit.datamodel.task_targets import InBodyTarget, TaskTarget

_log = logging.getLogger(__name__)


class MarkdownTableSerializerProvider:
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
    default_tokenizer: str = Field(
        default="sentence-transformers/all-MiniLM-L6-v2",
        description="Default tokenizer to use when none is specified",
    )


class DocumentChunkerManager:
    """Handles document chunking for RAG workflows using HybridChunker from docling-core."""

    def __init__(self, config: Optional[DocumentChunkerConfig] = None):
        self.config = config or DocumentChunkerConfig()
        self._cache_lock = threading.Lock()
        self._get_chunker_from_cache = self._create_chunker_cache()

    def _create_chunker_cache(self):
        """Create LRU cache for chunker instances."""

        @lru_cache(maxsize=self.config.cache_size)
        def _get_chunker_from_cache(cache_key: str) -> Any:
            try:
                from docling_core.transforms.chunker.hierarchical_chunker import (
                    ChunkingSerializerProvider,
                )
                from docling_core.transforms.chunker.hybrid_chunker import HybridChunker
                from docling_core.transforms.chunker.tokenizer.huggingface import (
                    HuggingFaceTokenizer,
                )

                # Parse cache key back to options
                parts = cache_key.split("_")
                tokenizer = parts[0] if parts[0] != "None" else None
                max_tokens = int(parts[1])
                merge_peers = parts[2] == "True"
                use_markdown_tables = parts[3] == "True"

                # Create tokenizer
                tokenizer_name = tokenizer or self.config.default_tokenizer
                tokenizer_obj = HuggingFaceTokenizer.from_pretrained(
                    model_name=tokenizer_name,
                    max_tokens=max_tokens,
                )

                # Create serializer provider based on markdown table option
                if use_markdown_tables:
                    serializer_provider: Any = MarkdownTableSerializerProvider()
                else:
                    serializer_provider = ChunkingSerializerProvider()

                chunker = HybridChunker(
                    tokenizer=tokenizer_obj,
                    merge_peers=merge_peers,
                    serializer_provider=serializer_provider,
                )

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

    def _get_chunker(self, options: ChunkingOptions) -> Any:
        """Get or create a cached HybridChunker instance."""
        # Create a cache key based on chunking options using the same pattern as the repo
        cache_key = self._generate_cache_key(options)

        with self._cache_lock:
            return self._get_chunker_from_cache(cache_key)

    def _generate_cache_key(self, options: ChunkingOptions) -> str:
        """Generate a deterministic cache key from chunking options."""
        key_data = {
            "tokenizer": options.tokenizer,
            "max_tokens": options.max_tokens,
            "merge_peers": options.merge_peers,
            "use_markdown_tables": options.use_markdown_tables,
        }
        # Use the same hashing pattern as other cache keys in the repo
        serialized_data = json.dumps(key_data, sort_keys=True)
        options_hash = hashlib.sha1(
            serialized_data.encode(), usedforsecurity=False
        ).hexdigest()
        return options_hash

    def clear_cache(self):
        """Clear the chunker cache."""
        with self._cache_lock:
            self._get_chunker_from_cache.cache_clear()

    def chunking_options(self, options: ChunkingOptions) -> dict:
        return {
            "tokenizer": options.tokenizer or self.config.default_tokenizer,
            "max_tokens": options.max_tokens,
            "merge_peers": options.merge_peers,
            "use_markdown_tables": options.use_markdown_tables,
        }

    def chunk_document(
        self,
        document: DoclingDocument,
        filename: str,
        options: ChunkingOptions,
    ) -> Iterable[ChunkedDocumentResponseItem]:
        """Chunk a document using HybridChunker from docling-core."""

        chunker = self._get_chunker(options)

        chunks = list(chunker.chunk(document))

        # Convert chunks to response format
        chunk_items: list[ChunkedDocumentResponseItem] = []
        for i, chunk in enumerate(chunks):
            headings: List[str] = []
            page_numbers: List[int] = []
            metadata: Dict[str, Any] = {}

            if hasattr(chunk, "meta") and chunk.meta:
                # Extract headings
                if hasattr(chunk.meta, "headings") and chunk.meta.headings:
                    headings = [
                        h.text for h in chunk.meta.headings if hasattr(h, "text")
                    ]

                # Extract page numbers from doc items
                if hasattr(chunk.meta, "doc_items") and chunk.meta.doc_items:
                    page_numbers = []
                    for item in chunk.meta.doc_items:
                        if hasattr(item, "prov") and item.prov:
                            for prov in item.prov:
                                if (
                                    hasattr(prov, "page_no")
                                    and prov.page_no is not None
                                ):
                                    page_numbers.append(prov.page_no)

                    # Remove duplicates and sort
                    page_numbers = sorted(set(page_numbers))

                # Store additional metadata
                if hasattr(chunk.meta, "origin"):
                    metadata["origin"] = (
                        str(chunk.meta.origin) if chunk.meta.origin else None
                    )

            # Create chunk item
            chunk_item = ChunkedDocumentResponseItem(
                filename=filename,
                chunk_index=i,
                text=chunk.text,
                raw_text=chunk.text if options.include_raw_text else None,
                num_tokens=0,  # TODO
                headings=headings,
                page_numbers=page_numbers,
                metadata=metadata,
            )
            chunk_items.append(chunk_item)

        return chunk_items


def process_chunk_results(
    chunking_options: ChunkingOptions | None,
    target: TaskTarget,
    conv_results: Iterable[ConversionResult],
    work_dir: Path,
) -> DoclingTaskResult:
    # Let's start by processing the documents
    start_time = time.monotonic()
    chunking_options = chunking_options or ChunkingOptions()

    # We have some results, let's prepare the response
    task_result: ChunkedDocumentResponse
    chunks: list[ChunkedDocumentResponseItem] = []
    convert_details: list[ChunkedDocumentConvertDetail] = []
    num_succeeded = 0
    num_failed = 0

    chunker_manager = DocumentChunkerManager()
    for conv_res in conv_results:
        errors = conv_res.errors
        if conv_res.status == ConversionStatus.SUCCESS:
            try:
                chunks.extend(
                    chunker_manager.chunk_document(
                        document=conv_res.document,
                        filename=conv_res.input.file.name,
                        options=chunking_options,
                    )
                )
                num_succeeded += 1
            except Exception as e:
                _log.error(f"Document chunking failed for {conv_res.input.file}: {e}")
                num_failed += 1
                errors = [
                    *errors,
                    ErrorItem(
                        component_type="chunking",
                        module_name=type(e).__name__,
                        error_message=str(e),
                    ),
                ]

        else:
            _log.warning(f"Document {conv_res.input.file} failed to convert.")
            num_failed += 1

        convert_details.append(
            ChunkedDocumentConvertDetail(
                status=conv_res.status, errors=errors, timings=conv_res.timings
            )
        )

    num_total = num_succeeded + num_failed
    processing_time = time.monotonic() - start_time
    _log.info(
        f"Processed {num_total} docs generating {len(chunks)} chunks in {processing_time:.2f} seconds."
    )

    if isinstance(target, InBodyTarget):
        task_result = ChunkedDocumentResponse(
            chunks=chunks,
            convert_details=convert_details,
            processing_time=processing_time,
            chunking_info=chunker_manager.chunking_options(options=chunking_options),
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
