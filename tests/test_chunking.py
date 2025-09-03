from unittest.mock import Mock

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.document import ConversionResult

from docling_jobkit.convert.chunking import DocumentChunkerManager
from docling_jobkit.datamodel.chunking import ChunkedDocumentResponse, ChunkingOptions


class TestDocumentChunker:
    """Test cases for DocumentChunker functionality."""

    def test_chunker_initialization(self):
        """Test that DocumentChunker can be initialized."""
        chunker = DocumentChunkerManager()
        assert chunker is not None
        assert chunker.config.cache_size == 10  # Default cache size
        assert (
            chunker.config.default_tokenizer == "sentence-transformers/all-MiniLM-L6-v2"
        )
        assert chunker._get_chunker_from_cache is not None

    def test_chunker_custom_config(self):
        """Test DocumentChunker with custom configuration."""
        from docling_jobkit.convert.chunking import DocumentChunkerConfig

        config = DocumentChunkerConfig(
            cache_size=5, default_tokenizer="custom/tokenizer"
        )
        chunker = DocumentChunkerManager(config=config)
        assert chunker.config.cache_size == 5
        assert chunker.config.default_tokenizer == "custom/tokenizer"

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
        chunker = DocumentChunkerManager()

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
            text="Test content",
            num_tokens=4,
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
        assert response.chunks[0].text == "Test content"

    def test_cache_key_generation(self):
        """Test that cache key generation is deterministic and uses SHA1."""
        chunker = DocumentChunkerManager()

        options1 = ChunkingOptions(
            max_tokens=512,
            tokenizer="test-tokenizer",
            merge_peers=True,
            use_markdown_tables=False,
        )
        options2 = ChunkingOptions(
            max_tokens=512,
            tokenizer="test-tokenizer",
            merge_peers=True,
            use_markdown_tables=False,
        )
        options3 = ChunkingOptions(
            max_tokens=1024,  # Different value
            tokenizer="test-tokenizer",
            merge_peers=True,
            use_markdown_tables=False,
        )

        key1 = chunker._generate_cache_key(options1)
        key2 = chunker._generate_cache_key(options2)
        key3 = chunker._generate_cache_key(options3)

        # Same options should generate same key
        assert key1 == key2
        # Different options should generate different key
        assert key1 != key3
        # Should be a hex string (SHA1 produces 40 character hex string)
        assert len(key1) == 40
        assert all(c in "0123456789abcdef" for c in key1)
