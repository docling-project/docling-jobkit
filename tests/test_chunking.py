from unittest.mock import Mock

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.document import ConversionResult

from docling_jobkit.convert.chunking import DocumentChunker
from docling_jobkit.datamodel.chunking import ChunkedDocumentResponse, ChunkingOptions


class TestDocumentChunker:
    """Test cases for DocumentChunker functionality."""

    def test_chunker_initialization(self):
        """Test that DocumentChunker can be initialized."""
        chunker = DocumentChunker()
        assert chunker is not None
        assert chunker.config.cache_size == 2  # Default cache size
        assert chunker._get_chunker_from_cache is not None

    def test_chunking_options_defaults(self):
        """Test ChunkingOptions with default values."""
        options = ChunkingOptions()
        assert options.max_tokens == 512
        assert options.tokenizer is None
        assert options.use_markdown_tables is False
        assert options.merge_peers is True
        assert options.include_raw_text is True

    def test_chunking_options_custom_values(self):
        """Test ChunkingOptions with custom values."""
        options = ChunkingOptions(
            max_tokens=1024,
            tokenizer="custom/tokenizer",
            use_markdown_tables=True,
            merge_peers=False,
            include_raw_text=False,
        )
        assert options.max_tokens == 1024
        assert options.tokenizer == "custom/tokenizer"
        assert options.use_markdown_tables is True
        assert options.merge_peers is False
        assert options.include_raw_text is False

    def test_chunk_conversion_result_failure(self):
        """Test chunking with failed conversion result."""
        chunker = DocumentChunker()

        # Create failed conversion result with minimal required fields
        failed_result = Mock(spec=ConversionResult)
        failed_result.status = ConversionStatus.FAILURE
        failed_result.errors = []
        failed_result.timings = {}

        options = ChunkingOptions()
        result = chunker.chunk_conversion_result(failed_result, options)

        assert isinstance(result, ChunkedDocumentResponse)
        assert result.status == ConversionStatus.FAILURE
        assert len(result.chunks) == 0
        assert result.chunking_info is None


class TestChunkedDocumentResponse:
    """Test cases for ChunkedDocumentResponse model."""

    def test_chunked_response_creation(self):
        """Test creating a ChunkedDocumentResponse."""
        response = ChunkedDocumentResponse(
            chunks=[],
            status=ConversionStatus.SUCCESS,
            errors=[],
            processing_time=1.5,
            timings={},
            chunking_info={"total_chunks": 0},
        )

        assert response.status == ConversionStatus.SUCCESS
        assert response.processing_time == 1.5
        assert response.chunking_info == {"total_chunks": 0}

    def test_chunked_response_with_chunks(self):
        """Test ChunkedDocumentResponse with actual chunks."""
        from docling_jobkit.datamodel.chunking import ChunkedDocumentResponseItem

        chunk = ChunkedDocumentResponseItem(
            filename="test.pdf",
            chunk_index=0,
            contextualized_text="Test content",
            headings=["Heading 1"],
            page_numbers=[1],
        )

        response = ChunkedDocumentResponse(
            chunks=[chunk],
            status=ConversionStatus.SUCCESS,
            errors=[],
            processing_time=0.5,
            timings={},
            chunking_info={"total_chunks": 1},
        )

        assert len(response.chunks) == 1
        assert response.chunks[0].filename == "test.pdf"
        assert response.chunks[0].contextualized_text == "Test content"
