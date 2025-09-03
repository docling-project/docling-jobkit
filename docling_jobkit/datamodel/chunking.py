import enum
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field

from docling.datamodel.base_models import ConversionStatus, ErrorItem
from docling.utils.profiling import ProfilingItem


class ChunkerType(str, enum.Enum):
    """Choice of the chunkers available in Docling."""

    HIERARCHICAL = "hierarchical"
    HYBRID = "hybrid"


class ChunkingOptions(BaseModel):
    """Configuration options for document chunking using Docling chunkers."""

    chunker: ChunkerType = ChunkerType.HYBRID

    max_tokens: Annotated[
        int,
        Field(
            description="Maximum number of tokens per chunk. Default is 512.",
            gt=0,
            le=32768,  # Reasonable upper limit
        ),
    ] = 512

    tokenizer: Annotated[
        Optional[str],
        Field(
            description="HuggingFace model name for custom tokenization. If not specified, uses 'sentence-transformers/all-MiniLM-L6-v2' as default.",
            examples=[
                "Qwen/Qwen3-Embedding-0.6B",
                "sentence-transformers/all-MiniLM-L6-v2",
            ],
        ),
    ] = None

    merge_peers: Annotated[
        bool,
        Field(
            description="Merge undersized successive chunks with same headings.",
        ),
    ] = True

    use_markdown_tables: Annotated[
        bool,
        Field(
            description="Use markdown table format instead of triplets for table serialization.",
        ),
    ] = False

    include_raw_text: Annotated[
        bool,
        Field(
            description="Include both raw_text and text (contextualized) in response. If False, only text is included.",
        ),
    ] = True


class ChunkedDocumentResponseItem(BaseModel):
    """A single chunk of a document with its metadata and content."""

    filename: str
    chunk_index: int
    text: str = Field(
        description="The chunk text with structural context (headers, formatting)"
    )
    raw_text: str | None = Field(
        default=None,
        description="Raw chunk text without additional formatting or context",
    )
    num_tokens: int | None = Field(
        description="Number of tokens in the text, if the chunker is aware of tokens"
    )
    headings: list[str] | None = Field(
        default=None, description="List of headings for this chunk"
    )
    page_numbers: list[int] | None = Field(
        default=None, description="Page numbers where this chunk content appears"
    )
    metadata: dict | None = Field(
        default=None, description="Additional metadata associated with this chunk"
    )


class ChunkedDocumentConvertDetail(BaseModel):
    status: ConversionStatus
    errors: list[ErrorItem] = []
    timings: dict[str, ProfilingItem] = {}


class ChunkedDocumentResponse(BaseModel):
    kind: Literal["ChunkedDocumentResponse"] = "ChunkedDocumentResponse"
    chunks: list[ChunkedDocumentResponseItem]
    convert_details: list[ChunkedDocumentConvertDetail]
    chunking_info: Optional[dict] = None
