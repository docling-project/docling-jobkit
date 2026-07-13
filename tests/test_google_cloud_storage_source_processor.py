from datetime import datetime, timezone
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import NotFound

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.google_cloud_storage_helper import (
    GoogleCloudStorageFileIdentifier,
)
from docling_jobkit.connectors.google_cloud_storage_source_processor import (
    GoogleCloudStorageSourceProcessor,
)
from docling_jobkit.convert.materialization import SourceLimitExceededError
from docling_jobkit.datamodel.google_cloud_storage_coords import (
    GoogleCloudStorageCoordinates,
)


@pytest.fixture
def coords() -> GoogleCloudStorageCoordinates:
    return GoogleCloudStorageCoordinates(bucket="test", key_prefix="source3/")


def _blob(name: str, size: int, updated: datetime | None = None) -> SimpleNamespace:
    """Stand-in for a google.cloud.storage.Blob (only the fields we read)."""
    return SimpleNamespace(name=name, size=size, updated=updated)


# ----------------- Source lineage -----------------


def test_make_document_ref_builds_gs_source_uri(coords):
    processor = GoogleCloudStorageSourceProcessor(coords)
    ref = processor._make_document_ref(
        GoogleCloudStorageFileIdentifier(
            name="source3/doc.pdf",
            size=123,
            last_modified=datetime(2026, 6, 2, 10, 0, 0, tzinfo=timezone.utc),
        ),
        source_index=5,
    )

    assert ref.source_index == 5
    assert ref.source_uri == "gs://test/source3/doc.pdf"
    assert ref.filename == "source3/doc.pdf"


# ----------------- Listing / traversal -----------------


def test_list_document_ids_skips_folder_markers(coords):
    processor = GoogleCloudStorageSourceProcessor(coords)
    processor._client = MagicMock()
    processor._client.list_blobs.return_value = [
        _blob("source3/", 0),  # folder-placeholder object
        _blob("source3/a.pdf", 10),
        _blob("source3/b.pdf", 20),
    ]

    doc_ids = list(processor._list_document_ids())

    assert [d.name for d in doc_ids] == ["source3/a.pdf", "source3/b.pdf"]


def test_list_document_ids_caps_real_documents_not_markers(coords):
    # Regression: max_num_elements must count real documents, not the folder
    # marker that sorts first. Previously the cap was pushed to list_blobs'
    # max_results, so max_num_elements=1 returned only the "source3/" marker.
    capped = coords.model_copy(update={"max_num_elements": 1})
    processor = GoogleCloudStorageSourceProcessor(capped)
    processor._client = MagicMock()
    processor._client.list_blobs.return_value = [
        _blob("source3/", 0),
        _blob("source3/a.pdf", 10),
        _blob("source3/b.pdf", 20),
    ]

    doc_ids = list(processor._list_document_ids())

    assert [d.name for d in doc_ids] == ["source3/a.pdf"]


# ----------------- Document fetch -----------------


def test_fetch_document_rejects_size_before_download(coords):
    processor = GoogleCloudStorageSourceProcessor(coords)
    processor._client = MagicMock()
    identifier = GoogleCloudStorageFileIdentifier(name="source3/big.pdf", size=9)

    with pytest.raises(SourceLimitExceededError, match="max_file_size=8"):
        processor._fetch_document_by_id(identifier, max_file_size=8)

    processor._client.bucket.return_value.blob.return_value.download_to_file.assert_not_called()


def test_fetch_document_logs_and_reraises_api_error(coords, caplog):
    processor = GoogleCloudStorageSourceProcessor(coords)
    processor._client = MagicMock()
    processor._client.bucket.return_value.blob.return_value.download_to_file.side_effect = NotFound(
        "missing"
    )
    identifier = GoogleCloudStorageFileIdentifier(name="source3/missing.pdf", size=0)

    with caplog.at_level("WARNING"):
        with pytest.raises(NotFound):
            processor._fetch_document_by_id(identifier)

    assert any(
        "source3/missing.pdf" in record.message and coords.bucket in record.message
        for record in caplog.records
    )


def test_fetch_document_returns_stream_with_name(coords):
    processor = GoogleCloudStorageSourceProcessor(coords)
    processor._client = MagicMock()
    identifier = GoogleCloudStorageFileIdentifier(name="source3/a.pdf", size=10)

    doc = processor._fetch_document_by_id(identifier)

    assert isinstance(doc, DocumentStream)
    assert doc.name == "source3/a.pdf"
    processor._client.bucket.assert_called_once_with("test")
    processor._client.bucket.return_value.blob.assert_called_once_with("source3/a.pdf")


def test_iterate_documents_respects_max_num_elements(coords):
    capped = coords.model_copy(update={"max_num_elements": 2})
    processor = GoogleCloudStorageSourceProcessor(capped)
    processor._initialized = True
    processor._client = MagicMock()
    processor._client.list_blobs.return_value = [
        _blob("source3/a.pdf", 1),
        _blob("source3/b.pdf", 2),
        _blob("source3/c.pdf", 3),
    ]
    processor._fetch_document_by_id = MagicMock(
        side_effect=lambda identifier, *, max_file_size=None: DocumentStream(
            name=identifier.name, stream=BytesIO(b"x")
        )
    )

    docs = list(processor.iterate_documents())

    assert [d.name for d in docs] == ["source3/a.pdf", "source3/b.pdf"]
    assert processor._fetch_document_by_id.call_count == 2
