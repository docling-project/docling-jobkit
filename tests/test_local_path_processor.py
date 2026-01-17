import tempfile
from pathlib import Path

import pytest

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.local_path_source_processor import (
    LocalPathSourceProcessor,
)
from docling_jobkit.connectors.local_path_target_processor import (
    LocalPathTargetProcessor,
)
from docling_jobkit.datamodel.task_sources import TaskLocalPathSource
from docling_jobkit.datamodel.task_targets import LocalPathTarget

# -------------------------------------------------------------------
# Pytest fixtures
# -------------------------------------------------------------------


@pytest.fixture
def temp_test_dir():
    """Create a temporary directory with test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        test_dir = Path(tmpdir)

        # Create test directory structure
        (test_dir / "subdir1").mkdir()
        (test_dir / "subdir2").mkdir()
        (test_dir / "subdir1" / "nested").mkdir()

        # Create test files
        (test_dir / "file1.pdf").write_bytes(b"%PDF-1.4 test content 1")
        (test_dir / "file2.pdf").write_bytes(b"%PDF-1.4 test content 2")
        (test_dir / "file3.txt").write_bytes(b"text file content")
        (test_dir / "subdir1" / "file4.pdf").write_bytes(b"%PDF-1.4 test content 4")
        (test_dir / "subdir1" / "nested" / "file5.pdf").write_bytes(
            b"%PDF-1.4 test content 5"
        )
        (test_dir / "subdir2" / "file6.docx").write_bytes(b"docx content")

        yield test_dir


@pytest.fixture
def temp_output_dir():
    """Create a temporary output directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# -------------------------------------------------------------------
# LocalPathSourceProcessor tests
# -------------------------------------------------------------------


def test_local_path_single_file(temp_test_dir):
    """Test processing a single file."""
    source = TaskLocalPathSource(
        kind="local_path",
        path=temp_test_dir / "file1.pdf",
    )

    with LocalPathSourceProcessor(source) as processor:
        # Count documents
        count = processor._count_documents()
        assert count == 1, "Expected exactly one file"

        # List document IDs
        doc_ids = list(processor._list_document_ids())
        assert len(doc_ids) == 1
        assert doc_ids[0]["path"] == temp_test_dir / "file1.pdf"

        # Fetch document
        doc = processor._fetch_document_by_id(doc_ids[0])
        assert isinstance(doc, DocumentStream)
        content = doc.stream.read()
        assert content == b"%PDF-1.4 test content 1"


def test_local_path_directory_recursive(temp_test_dir):
    """Test processing a directory recursively with pattern."""
    source = TaskLocalPathSource(
        kind="local_path",
        path=temp_test_dir,
        pattern="*.pdf",
        recursive=True,
    )

    with LocalPathSourceProcessor(source) as processor:
        # Count documents
        count = processor._count_documents()
        assert count == 4, "Expected 4 PDF files (recursive)"

        # List document IDs
        doc_ids = list(processor._list_document_ids())
        assert len(doc_ids) == 4

        # Verify all are PDF files
        for doc_id in doc_ids:
            assert doc_id["path"].suffix == ".pdf"

        # Iterate documents
        docs = list(processor.iterate_documents())
        assert len(docs) == 4

        for doc in docs:
            assert isinstance(doc, DocumentStream)
            content = doc.stream.read()
            assert content.startswith(b"%PDF-1.4")


def test_local_path_directory_non_recursive(temp_test_dir):
    """Test processing a directory non-recursively."""
    source = TaskLocalPathSource(
        kind="local_path",
        path=temp_test_dir,
        pattern="*.pdf",
        recursive=False,
    )

    with LocalPathSourceProcessor(source) as processor:
        # Count documents
        count = processor._count_documents()
        assert count == 2, "Expected 2 PDF files in root directory only"

        # List document IDs
        doc_ids = list(processor._list_document_ids())
        assert len(doc_ids) == 2

        # Verify they are from root directory
        for doc_id in doc_ids:
            assert doc_id["path"].parent == temp_test_dir


def test_local_path_all_files(temp_test_dir):
    """Test processing all files with default pattern."""
    source = TaskLocalPathSource(
        kind="local_path",
        path=temp_test_dir,
        pattern="*",
        recursive=True,
    )

    with LocalPathSourceProcessor(source) as processor:
        count = processor._count_documents()
        assert count == 6, "Expected 6 files total (all types)"


def test_local_path_nonexistent_file():
    """Test that nonexistent path raises error."""
    source = TaskLocalPathSource(
        kind="local_path",
        path=Path("/nonexistent/path/file.pdf"),
    )

    with pytest.raises(FileNotFoundError):
        with LocalPathSourceProcessor(source) as _:
            pass


# -------------------------------------------------------------------
# LocalPathTargetProcessor tests
# -------------------------------------------------------------------


def test_local_path_target_directory(temp_output_dir):
    """Test writing to a directory target."""
    target = LocalPathTarget(
        kind="local_path",
        path=temp_output_dir / "output",
    )

    with LocalPathTargetProcessor(target) as processor:
        # Upload a file
        test_content = b"test output content"
        processor.upload_object(
            obj=test_content,
            target_filename="result.txt",
            content_type="text/plain",
        )

        # Verify file was created
        output_file = temp_output_dir / "output" / "result.txt"
        assert output_file.exists()
        assert output_file.read_bytes() == test_content


def test_local_path_target_file(temp_output_dir):
    """Test writing to a specific file target."""
    target = LocalPathTarget(
        kind="local_path",
        path=temp_output_dir / "specific_output.json",
    )

    with LocalPathTargetProcessor(target) as processor:
        # Upload content (target_filename is ignored for file targets)
        test_content = '{"result": "success"}'
        processor.upload_object(
            obj=test_content,
            target_filename="ignored.json",
            content_type="application/json",
        )

        # Verify file was created at the specified path
        output_file = temp_output_dir / "specific_output.json"
        assert output_file.exists()
        assert output_file.read_text() == test_content


def test_local_path_target_nested_directory(temp_output_dir):
    """Test that nested directories are created automatically."""
    target = LocalPathTarget(
        kind="local_path",
        path=temp_output_dir / "level1" / "level2" / "level3",
    )

    with LocalPathTargetProcessor(target) as processor:
        processor.upload_object(
            obj=b"nested content",
            target_filename="nested.txt",
            content_type="text/plain",
        )

        # Verify nested directories were created
        output_file = temp_output_dir / "level1" / "level2" / "level3" / "nested.txt"
        assert output_file.exists()
        assert output_file.read_bytes() == b"nested content"


def test_local_path_target_upload_file(temp_test_dir, temp_output_dir):
    """Test uploading from a source file."""
    target = LocalPathTarget(
        kind="local_path",
        path=temp_output_dir,
    )

    source_file = temp_test_dir / "file1.pdf"

    with LocalPathTargetProcessor(target) as processor:
        processor.upload_file(
            filename=source_file,
            target_filename="copied.pdf",
            content_type="application/pdf",
        )

        # Verify file was copied
        output_file = temp_output_dir / "copied.pdf"
        assert output_file.exists()
        assert output_file.read_bytes() == source_file.read_bytes()


def test_local_path_target_bytes_and_string(temp_output_dir):
    """Test uploading both bytes and string content."""
    target = LocalPathTarget(
        kind="local_path",
        path=temp_output_dir,
    )

    with LocalPathTargetProcessor(target) as processor:
        # Upload bytes
        processor.upload_object(
            obj=b"binary content",
            target_filename="binary.bin",
            content_type="application/octet-stream",
        )

        # Upload string
        processor.upload_object(
            obj="text content",
            target_filename="text.txt",
            content_type="text/plain",
        )

        # Verify both files
        assert (temp_output_dir / "binary.bin").read_bytes() == b"binary content"
        assert (temp_output_dir / "text.txt").read_text() == "text content"
