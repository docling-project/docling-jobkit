from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field

from docling.datamodel.base_models import ConversionStatus, ErrorItem
from docling.utils.profiling import ProfilingItem


class ChunkingOptions(BaseModel):
    """Configuration options for document chunking using HybridChunker."""

    max_tokens: Annotated[
        int,
        Field(
            description="Maximum number of tokens per chunk.",
            gt=0,
            le=32768,  # Reasonable upper limit
        ),
    ] = 512

    tokenizer: Annotated[
        Optional[str],
        Field(
            description="HuggingFace model name for custom tokenization. If not specified, uses 'Qwen/Qwen3-Embedding-0.6B' as default.",
            examples=[
                "Qwen/Qwen3-Embedding-0.6B",
                "sentence-transformers/all-MiniLM-L6-v2",
            ],
        ),
    ] = None

    use_markdown_tables: Annotated[
        bool,
        Field(
            description="Use markdown table format instead of triplets for table serialization.",
        ),
    ] = False

    merge_peers: Annotated[
        bool,
        Field(
            description="Merge undersized successive chunks with same headings.",
        ),
    ] = True

    include_raw_text: Annotated[
        bool,
        Field(
            description="Include both chunk_text and contextualized_text in response. If False, only contextualized_text is included.",
        ),
    ] = True


class ChunkedDocumentResponseItem(BaseModel):
    filename: str
    chunk_index: int
    contextualized_text: str
    chunk_text: str | None = None
    headings: list[str] | None = None
    page_numbers: list[int] | None = None
    metadata: dict | None = None


class ChunkedDocumentResponse(BaseModel):
    kind: Literal["ChunkedDocumentResponse"] = "ChunkedDocumentResponse"
    chunks: list[ChunkedDocumentResponseItem]
    status: ConversionStatus
    errors: list[ErrorItem] = []
    processing_time: float
    timings: dict[str, ProfilingItem] = {}
    chunking_info: Optional[dict] = None
