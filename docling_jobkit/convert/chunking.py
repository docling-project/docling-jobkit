import hashlib
import json
import logging
import threading
from functools import lru_cache
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from docling.datamodel.base_models import ConversionStatus, ErrorItem
from docling.datamodel.document import ConversionResult
from docling.utils.profiling import ProfilingItem
from docling_core.types.doc.document import DoclingDocument

from docling_jobkit.datamodel.chunking import (
    ChunkedDocumentResponse,
    ChunkedDocumentResponseItem,
    ChunkingOptions,
)

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


class DocumentChunker:
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

    def chunk_document(
        self,
        document: DoclingDocument,
        filename: str,
        options: ChunkingOptions,
        timings: Optional[Dict[str, ProfilingItem]] = None,
    ) -> ChunkedDocumentResponse:
        """Chunk a document using HybridChunker from docling-core."""
        import time

        start_time = time.time()

        try:
            chunker = self._get_chunker(options)

            chunks = list(chunker.chunk(document))

            # Convert chunks to response format
            chunk_items = []
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
                    headings=headings,
                    page_numbers=page_numbers,
                    metadata=metadata,
                )
                chunk_items.append(chunk_item)

            processing_time = time.time() - start_time

            # Create chunking info
            chunking_info = {
                "tokenizer": options.tokenizer or self.config.default_tokenizer,
                "max_tokens": options.max_tokens,
                "total_chunks": len(chunk_items),
                "merge_peers": options.merge_peers,
                "use_markdown_tables": options.use_markdown_tables,
            }

            return ChunkedDocumentResponse(
                chunks=chunk_items,
                status=ConversionStatus.SUCCESS,
                errors=[],
                processing_time=processing_time,
                timings=timings or {},
                chunking_info=chunking_info,
            )

        except Exception as e:
            _log.error(f"Document chunking failed for {filename}: {e}")
            processing_time = time.time() - start_time

            return ChunkedDocumentResponse(
                chunks=[],
                status=ConversionStatus.FAILURE,
                errors=[
                    ErrorItem(
                        component_type="chunking",
                        module_name=type(e).__name__,
                        error_message=str(e),
                    )
                ],
                processing_time=processing_time,
                timings=timings or {},
                chunking_info=None,
            )

    def chunk_conversion_result(
        self,
        conv_res: ConversionResult,
        options: ChunkingOptions,
    ) -> ChunkedDocumentResponse:
        """Chunk a conversion result."""
        if conv_res.status != ConversionStatus.SUCCESS:
            return ChunkedDocumentResponse(
                chunks=[],
                status=conv_res.status,
                errors=conv_res.errors,
                processing_time=0.0,
                timings=conv_res.timings,
                chunking_info=None,
            )

        return self.chunk_document(
            document=conv_res.document,
            filename=conv_res.input.file.name,
            options=options,
            timings=conv_res.timings,
        )
