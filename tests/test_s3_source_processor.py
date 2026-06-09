import socket
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.s3_source_processor import (
    S3FileIdentifier,
    S3SourceProcessor,
)
from docling_jobkit.datamodel.s3_coords import S3Coordinates

# -------------------------------------------------------------------
# Helper function to check MinIO availability
# -------------------------------------------------------------------


def is_minio_available(host: str = "127.0.0.1", port: int = 9000) -> bool:
    """Check if MinIO is running and accessible."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


# -------------------------------------------------------------------
# Pytest fixtures
# -------------------------------------------------------------------


@pytest.fixture
def minio_coords() -> S3Coordinates:
    """Fixture providing S3Coordinates for local MinIO test instance."""
    return S3Coordinates(
        endpoint="127.0.0.1:9000",
        verify_ssl=False,
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="test",
        key_prefix="",
    )


# -------------------------------------------------------------------
# S3SourceProcessor integration tests with real MinIO
# -------------------------------------------------------------------


@pytest.mark.skipif(
    not is_minio_available(),
    reason="MinIO is not running at 127.0.0.1:9000",
)
def test_s3_connection_and_list_files(minio_coords):
    """Test S3 connection and file listing from MinIO."""
    with S3SourceProcessor(minio_coords) as processor:
        # Get document count
        count = processor._count_documents()
        assert count > 0, "Expected at least one file in MinIO test bucket"

        # List all document IDs
        doc_ids = list(processor._list_document_ids())
        assert len(doc_ids) == count

        # Verify S3FileIdentifier structure
        for doc_id in doc_ids:
            assert isinstance(doc_id.key, str)
            assert isinstance(doc_id.size, int)
            assert doc_id.last_modified is None or isinstance(
                doc_id.last_modified, datetime
            )

        # Verify we have PDF files
        pdf_files = [doc_id for doc_id in doc_ids if doc_id.key.endswith(".pdf")]
        assert len(pdf_files) > 0, "Expected PDF files in test bucket"

        print(f"\nFound {count} files in MinIO test bucket:")
        for doc_id in doc_ids:
            print(f"  - {doc_id.key} ({doc_id.size} bytes)")


def test_s3_document_ref_preserves_canonical_source_uri(minio_coords):
    processor = S3SourceProcessor(minio_coords)
    ref = processor._make_document_ref(
        S3FileIdentifier(
            key="incoming/doc.pdf",
            size=123,
            last_modified=datetime(2026, 6, 2, 10, 0, 0, tzinfo=timezone.utc),
        ),
        source_index=5,
    )

    assert ref.source_index == 5
    assert ref.source_uri == "s3://test/incoming/doc.pdf"
    assert ref.filename == "incoming/doc.pdf"


def test_s3_list_document_ids_respects_max_num_elements(minio_coords):
    capped_coords = minio_coords.model_copy(update={"max_num_elements": 3})
    processor = S3SourceProcessor(capped_coords)
    processor._client = MagicMock()
    paginator = processor._client.get_paginator.return_value
    paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "incoming/a.pdf", "Size": 1},
                {"Key": "incoming/b.pdf", "Size": 2},
            ]
        },
        {
            "Contents": [
                {"Key": "incoming/c.pdf", "Size": 3},
                {"Key": "incoming/d.pdf", "Size": 4},
            ]
        },
    ]

    doc_ids = list(processor._list_document_ids())

    assert [doc_id.key for doc_id in doc_ids] == [
        "incoming/a.pdf",
        "incoming/b.pdf",
        "incoming/c.pdf",
    ]


def test_s3_count_documents_clips_to_max_num_elements(minio_coords):
    capped_coords = minio_coords.model_copy(update={"max_num_elements": 3})
    processor = S3SourceProcessor(capped_coords)
    processor._client = MagicMock()
    paginator = processor._client.get_paginator.return_value
    paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "incoming/a.pdf", "Size": 1},
                {"Key": "incoming/b.pdf", "Size": 2},
            ]
        },
        {
            "Contents": [
                {"Key": "incoming/c.pdf", "Size": 3},
                {"Key": "incoming/d.pdf", "Size": 4},
            ]
        },
    ]

    assert processor._count_documents() == 3


def test_s3_iterate_documents_respects_max_num_elements(minio_coords):
    capped_coords = minio_coords.model_copy(update={"max_num_elements": 2})
    processor = S3SourceProcessor(capped_coords)
    processor._initialized = True
    processor._client = MagicMock()
    paginator = processor._client.get_paginator.return_value
    paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "incoming/a.pdf", "Size": 1},
                {"Key": "incoming/b.pdf", "Size": 2},
                {"Key": "incoming/c.pdf", "Size": 3},
            ]
        }
    ]
    processor._fetch_document_by_id = MagicMock(
        side_effect=lambda identifier: DocumentStream(
            name=identifier.key,
            stream=BytesIO(b"content"),
        )
    )

    docs = list(processor.iterate_documents())

    assert [doc.name for doc in docs] == ["incoming/a.pdf", "incoming/b.pdf"]
    assert processor._fetch_document_by_id.call_count == 2


def test_s3_fetch_document_logs_and_reraises_client_error(minio_coords, caplog):
    """A download failure is logged with the offending key and propagated.

    Propagation is intentional: the Ray S3 fan-out path turns a per-document
    fetch failure into a single failed-document result without failing the
    whole task (see DoclingProcessorCoordinatorDeployment._iter_source_chunks_for_s3_fanout).
    """
    processor = S3SourceProcessor(minio_coords)
    error = ClientError(
        error_response={"Error": {"Code": "NoSuchKey", "Message": "Not Found"}},
        operation_name="DownloadFileobj",
    )
    processor._client = MagicMock()
    processor._client.download_fileobj.side_effect = error

    identifier = S3FileIdentifier(
        key="incoming/missing.pdf", size=0, last_modified=None
    )

    with caplog.at_level("WARNING"):
        with pytest.raises(ClientError):
            processor._fetch_document_by_id(identifier)

    assert any(
        "incoming/missing.pdf" in record.message
        and minio_coords.bucket in record.message
        for record in caplog.records
    )


@pytest.mark.skipif(
    not is_minio_available(),
    reason="MinIO is not running at 127.0.0.1:9000",
)
def test_s3_fetch_specific_document(minio_coords):
    """Test fetching a specific document from S3 using S3FileIdentifier."""
    with S3SourceProcessor(minio_coords) as processor:
        # Get first document ID
        doc_ids = list(processor._list_document_ids())
        assert len(doc_ids) > 0, "Expected at least one document"

        first_id = doc_ids[0]

        # Fetch the document using S3FileIdentifier
        doc = processor._fetch_document_by_id(first_id)

        # Verify document structure
        assert isinstance(doc, DocumentStream)
        assert doc.name == first_id.key
        assert doc.stream is not None

        # Verify content can be read
        content = doc.stream.read()
        assert len(content) > 0, "Document content is empty"
        assert len(content) == first_id.size, "Content size doesn't match S3 metadata"

        # For PDF files, verify header
        if first_id.key.endswith(".pdf"):
            assert content[:4] == b"%PDF", "Invalid PDF header"

        print(f"\nFetched document: {doc.name} ({len(content)} bytes)")


@pytest.mark.skipif(
    not is_minio_available(),
    reason="MinIO is not running at 127.0.0.1:9000",
)
def test_s3_iterate_documents(minio_coords):
    """Test iterating through all S3 documents using the high-level interface."""
    with S3SourceProcessor(minio_coords) as processor:
        # Get expected count
        expected_count = processor._count_documents()
        assert expected_count > 0, "Expected files in MinIO test bucket"

        # Iterate through all documents
        docs = list(processor.iterate_documents())

        # Verify count matches
        assert len(docs) == expected_count, (
            f"Expected {expected_count} documents, got {len(docs)}"
        )

        # Verify all documents are valid
        for doc in docs:
            assert isinstance(doc, DocumentStream)
            assert doc.name is not None
            assert doc.stream is not None

            # Verify content is readable
            content = doc.stream.read(4)
            assert len(content) > 0

            # PDF files should have correct header
            if doc.name.endswith(".pdf"):
                assert content == b"%PDF", f"Invalid PDF header for {doc.name}"

        print(f"\nIterated through {len(docs)} documents successfully")
