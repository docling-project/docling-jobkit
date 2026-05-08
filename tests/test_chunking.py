import json
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.targets import ZipTarget

from docling_jobkit.convert.chunking import (
    DocumentChunkerManager,
    _export_chunking_result,
    process_chunkable_results,
)
from docling_jobkit.datamodel.chunking import (
    HierarchicalChunkerOptions,
    HybridChunkerOptions,
)
from docling_jobkit.datamodel.exportable_document import ExportableDocument
from docling_jobkit.datamodel.result import (
    ChunkedDocumentResult,
    ChunkedDocumentResultItem,
    ExportDocumentResponse,
    ExportResult,
    ZipArchiveResult,
)
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskType
from docling_jobkit.datamodel.task_targets import InBodyTarget


class TestDocumentChunker:
    """Test cases for DocumentChunker functionality."""

    def test_chunker_initialization(self):
        """Test that DocumentChunker can be initialized."""
        chunker = DocumentChunkerManager()
        assert chunker is not None
        assert chunker.config.cache_size == 10  # Default cache size
        assert chunker._get_chunker_from_cache is not None

    def test_chunker_custom_config(self):
        """Test DocumentChunker with custom configuration."""
        from docling_jobkit.convert.chunking import DocumentChunkerConfig

        config = DocumentChunkerConfig(cache_size=5)
        chunker = DocumentChunkerManager(config=config)
        assert chunker.config.cache_size == 5

    def test_chunking_options_defaults(self):
        """Test HybridChunkerOptions with default values."""
        options = HybridChunkerOptions()
        assert options.max_tokens is None
        assert options.tokenizer == "sentence-transformers/all-MiniLM-L6-v2"
        assert options.use_markdown_tables is False
        assert options.merge_peers is True
        assert options.include_raw_text is False

    def test_chunking_options_custom_values(self):
        """Test HybridChunkerOptions with custom values."""
        options = HybridChunkerOptions(
            max_tokens=1024,
            tokenizer="custom/tokenizer",
            use_markdown_tables=True,
            merge_peers=False,
            include_raw_text=True,
        )
        assert options.max_tokens == 1024
        assert options.tokenizer == "custom/tokenizer"
        assert options.use_markdown_tables is True
        assert options.merge_peers is False
        assert options.include_raw_text is True

    def test_chunk_conversion_result_failure(self):
        """Test chunking with failed conversion result."""
        failed_result = ExportableDocument(
            file=Path("file.pdf"),
            status=ConversionStatus.FAILURE,
            errors=[],
            timings={},
        )

        workdir = tempfile.mkdtemp()

        task = Task(
            task_id="abc",
            task_type=TaskType.CHUNK,
            chunking_options=HybridChunkerOptions(),
            target=InBodyTarget(),
        )
        task_result = process_chunkable_results(
            task=task,
            exportable_documents=[failed_result],
            work_dir=workdir,
        )
        result = task_result.result

        assert isinstance(result, ChunkedDocumentResult)
        assert result.documents[0].status == ConversionStatus.FAILURE
        assert len(result.chunks) == 0


class TestChunkedDocumentResponse:
    """Test cases for ChunkedDocumentResponse model."""

    def test_chunked_response_creation(self):
        """Test creating a ChunkedDocumentResponse."""
        response = ChunkedDocumentResult(
            chunks=[],
            documents=[
                ExportResult(
                    content=ExportDocumentResponse(filename="file.pdf"),
                    status=ConversionStatus.SUCCESS,
                )
            ],
            chunking_info={"total_chunks": 0},
        )

        assert response.documents[0].status == ConversionStatus.SUCCESS
        assert response.chunking_info == {"total_chunks": 0}

    def test_chunked_response_with_chunks(self):
        """Test ChunkedDocumentResponse with actual chunks."""
        from docling_jobkit.datamodel.result import ChunkedDocumentResultItem

        chunk = ChunkedDocumentResultItem(
            filename="test.pdf",
            chunk_index=0,
            text="Test content",
            num_tokens=4,
            headings=["Heading 1"],
            doc_items=["#/tests/1"],
            page_numbers=[1],
        )

        response = ChunkedDocumentResult(
            chunks=[chunk],
            documents=[
                ExportResult(
                    content=ExportDocumentResponse(filename="file.pdf"),
                    status=ConversionStatus.SUCCESS,
                )
            ],
            chunking_info={"total_chunks": 1},
        )

        assert len(response.chunks) == 1
        assert response.chunks[0].filename == "test.pdf"
        assert response.chunks[0].text == "Test content"

    def test_cache_key_generation(self):
        """Test that cache key generation is deterministic and uses SHA1."""
        chunker = DocumentChunkerManager()

        options1 = HybridChunkerOptions(
            max_tokens=512,
            tokenizer="test-tokenizer",
            merge_peers=True,
            use_markdown_tables=False,
        )
        options2 = HybridChunkerOptions(
            max_tokens=512,
            tokenizer="test-tokenizer",
            merge_peers=True,
            use_markdown_tables=False,
        )
        options3 = HybridChunkerOptions(
            max_tokens=1024,  # Different value
            tokenizer="test-tokenizer",
            merge_peers=True,
            use_markdown_tables=False,
        )
        options4 = HierarchicalChunkerOptions()

        key1 = chunker._generate_cache_key(options1)
        key2 = chunker._generate_cache_key(options2)
        key3 = chunker._generate_cache_key(options3)
        key4 = chunker._generate_cache_key(options4)

        # Same options should generate same key
        assert key1 == key2
        # Different options should generate different key
        assert key1 != key3
        assert key1 != key4
        assert key3 != key4

    def test_export_chunking_result(self):
        """Test that the full chunked result is exported as a single JSON file."""
        output_dir = Path(tempfile.mkdtemp())
        chunks = [
            ChunkedDocumentResultItem(
                filename="doc1.pdf",
                chunk_index=0,
                text="first chunk",
                doc_items=["#/1"],
                page_numbers=[1],
            ),
            ChunkedDocumentResultItem(
                filename="doc1.pdf",
                chunk_index=1,
                text="second chunk",
                doc_items=["#/2"],
                page_numbers=[1, 2],
            ),
        ]
        documents = [
            ExportResult(
                content=ExportDocumentResponse(filename="doc1.pdf"),
                status=ConversionStatus.SUCCESS,
            ),
        ]
        result = ChunkedDocumentResult(
            chunks=chunks,
            documents=documents,
            processing_time=1.23,
            chunking_info={"chunker": "hybrid"},
        )
        _export_chunking_result(
            result=result,
            output_dir=output_dir,
        )

        result_file = output_dir / "chunked_result.json"
        assert result_file.exists()

        with result_file.open("r", encoding="utf-8") as f:
            data = json.load(f)

        assert len(data["chunks"]) == 2
        assert data["chunks"][0]["text"] == "first chunk"
        assert data["chunks"][1]["text"] == "second chunk"
        assert len(data["documents"]) == 1
        assert data["documents"][0]["content"]["filename"] == "doc1.pdf"
        assert data["chunking_info"] == {"chunker": "hybrid"}

    def test_chunk_conversion_result_zip_target(self):
        """Test chunking with zip target exports chunks into the archive."""
        exportable_document = ExportableDocument.model_construct(
            file=Path("test.pdf"),
            document_hash=None,
            status=ConversionStatus.SUCCESS,
            errors=[],
            timings={},
            document=Mock(),
            page_range=None,
            slice_index=None,
        )
        exportable_document.document.pages = [Mock()]

        chunk_manager = Mock(spec=DocumentChunkerManager)
        chunk_manager.chunk_document.return_value = [
            ChunkedDocumentResultItem(
                filename="test.pdf",
                chunk_index=0,
                text="chunk 0",
                doc_items=["#/test/1"],
                page_numbers=[1],
            ),
            ChunkedDocumentResultItem(
                filename="test.pdf",
                chunk_index=1,
                text="chunk 1",
                doc_items=["#/test/2"],
                page_numbers=[1],
            ),
        ]

        workdir = tempfile.mkdtemp()

        task = Task(
            task_id="abc",
            task_type=TaskType.CHUNK,
            chunking_options=HybridChunkerOptions(),
            target=ZipTarget(),
            convert_options=ConvertDocumentsOptions(),
        )

        task_result = process_chunkable_results(
            task=task,
            exportable_documents=[exportable_document],
            work_dir=Path(workdir),
            chunker_manager=chunk_manager,
        )

        assert isinstance(task_result.result, ZipArchiveResult)

        zip_bytes = task_result.result.content
        with zipfile.ZipFile(BytesIO(zip_bytes), "r") as zf:
            names = zf.namelist()
            assert "chunked_result.json" in names
            result_data = json.loads(zf.read("chunked_result.json"))
            assert len(result_data["chunks"]) == 2
            assert result_data["chunks"][0]["text"] == "chunk 0"
            assert result_data["chunks"][1]["text"] == "chunk 1"
            assert len(result_data["documents"]) == 1
            assert result_data["documents"][0]["content"]["filename"] == "test.pdf"
