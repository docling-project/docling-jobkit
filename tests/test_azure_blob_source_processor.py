"""Unit tests for Azure Blob Storage source processor."""

from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock

import pytest
from azure.core.exceptions import ResourceNotFoundError

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.azure_blob_source_processor import (
    AzureBlobFileIdentifier,
    AzureBlobSourceProcessor,
)
from docling_jobkit.convert.materialization import SourceLimitExceededError
from docling_jobkit.datamodel.azure_blob_coords import AzureBlobCoordinates


@pytest.fixture
def azure_coords() -> AzureBlobCoordinates:
    return AzureBlobCoordinates(
        account_name="testaccount",
        container="testcontainer",
        connection_string="DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net",
        blob_prefix="",
    )


def test_azure_blob_document_ref_preserves_canonical_uri(azure_coords):
    processor = AzureBlobSourceProcessor(azure_coords)
    ref = processor._make_document_ref(
        AzureBlobFileIdentifier(
            name="incoming/doc.pdf",
            size=123,
            last_modified=datetime(2026, 6, 2, 10, 0, 0, tzinfo=timezone.utc),
        ),
        source_index=5,
    )

    assert ref.source_index == 5
    assert ref.source_uri == "azure://testaccount/testcontainer/incoming/doc.pdf"
    assert ref.filename == "incoming/doc.pdf"


def test_azure_blob_list_respects_max_num_elements(azure_coords):
    capped_coords = azure_coords.model_copy(update={"max_num_elements": 3})
    processor = AzureBlobSourceProcessor(capped_coords)

    mock_blob1 = MagicMock()
    mock_blob1.name = "incoming/a.pdf"
    mock_blob1.size = 100
    mock_blob1.last_modified = None

    mock_blob2 = MagicMock()
    mock_blob2.name = "incoming/b.pdf"
    mock_blob2.size = 200
    mock_blob2.last_modified = None

    mock_blob3 = MagicMock()
    mock_blob3.name = "incoming/c.pdf"
    mock_blob3.size = 300
    mock_blob3.last_modified = None

    mock_blob4 = MagicMock()
    mock_blob4.name = "incoming/d.pdf"
    mock_blob4.size = 400
    mock_blob4.last_modified = None

    processor._container_client = MagicMock()
    processor._container_client.list_blobs.return_value = [
        mock_blob1,
        mock_blob2,
        mock_blob3,
        mock_blob4,
    ]

    doc_ids = list(processor._list_document_ids())

    assert len(doc_ids) == 3
    assert [doc_id.name for doc_id in doc_ids] == [
        "incoming/a.pdf",
        "incoming/b.pdf",
        "incoming/c.pdf",
    ]


def test_azure_blob_count_clips_to_max_num_elements(azure_coords):
    capped_coords = azure_coords.model_copy(update={"max_num_elements": 3})
    processor = AzureBlobSourceProcessor(capped_coords)

    mock_blobs = []
    for i in range(5):
        mock_blob = MagicMock()
        mock_blob.name = f"file{i}.pdf"
        mock_blob.size = 100
        mock_blobs.append(mock_blob)

    processor._container_client = MagicMock()
    processor._container_client.list_blobs.return_value = mock_blobs

    assert processor._count_documents() == 3


def test_azure_blob_iterate_documents_respects_max_num_elements(azure_coords):
    capped_coords = azure_coords.model_copy(update={"max_num_elements": 2})
    processor = AzureBlobSourceProcessor(capped_coords)
    processor._initialized = True

    mock_blobs = []
    for i in range(3):
        mock_blob = MagicMock()
        mock_blob.name = f"file{i}.pdf"
        mock_blob.size = 100
        mock_blob.last_modified = None
        mock_blobs.append(mock_blob)

    processor._container_client = MagicMock()
    processor._container_client.list_blobs.return_value = mock_blobs
    processor._fetch_document_by_id = MagicMock(
        side_effect=lambda identifier, *, max_file_size=None: DocumentStream(
            name=identifier.name,
            stream=BytesIO(b"content"),
        )
    )

    docs = list(processor.iterate_documents())

    assert len(docs) == 2
    assert [doc.name for doc in docs] == ["file0.pdf", "file1.pdf"]
    assert processor._fetch_document_by_id.call_count == 2


def test_azure_blob_fetch_rejects_oversized_before_download(azure_coords):
    processor = AzureBlobSourceProcessor(azure_coords)
    processor._container_client = MagicMock()

    identifier = AzureBlobFileIdentifier(
        name="incoming/too-large.pdf",
        size=9,
        last_modified=None,
    )

    with pytest.raises(SourceLimitExceededError, match="max_file_size=8"):
        processor._fetch_document_by_id(identifier, max_file_size=8)

    processor._container_client.get_blob_client.assert_not_called()


def test_azure_blob_fetch_logs_and_reraises_error(azure_coords, caplog):
    processor = AzureBlobSourceProcessor(azure_coords)

    mock_blob_client = MagicMock()
    mock_download_stream = MagicMock()
    mock_download_stream.readinto.side_effect = ResourceNotFoundError("Blob not found")
    mock_blob_client.download_blob.return_value = mock_download_stream

    processor._container_client = MagicMock()
    processor._container_client.get_blob_client.return_value = mock_blob_client

    identifier = AzureBlobFileIdentifier(
        name="incoming/missing.pdf",
        size=100,
        last_modified=None,
    )

    with caplog.at_level("WARNING"):
        with pytest.raises(ResourceNotFoundError):
            processor._fetch_document_by_id(identifier)

    assert any(
        "incoming/missing.pdf" in record.message
        and azure_coords.container in record.message
        for record in caplog.records
    )


def test_azure_blob_list_with_prefix(azure_coords):
    coords_with_prefix = azure_coords.model_copy(update={"blob_prefix": "pdfs/"})
    processor = AzureBlobSourceProcessor(coords_with_prefix)

    processor._container_client = MagicMock()
    processor._container_client.list_blobs.return_value = []

    list(processor._list_document_ids())

    processor._container_client.list_blobs.assert_called_once_with(
        name_starts_with="pdfs/"
    )


def test_azure_blob_list_without_prefix(azure_coords):
    processor = AzureBlobSourceProcessor(azure_coords)

    processor._container_client = MagicMock()
    processor._container_client.list_blobs.return_value = []

    list(processor._list_document_ids())

    processor._container_client.list_blobs.assert_called_once_with(
        name_starts_with=None
    )
