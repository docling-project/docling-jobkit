import logging
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest
import requests

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.errors import SourceConnectorUnavailableError
from docling_jobkit.connectors.filenet.models import FileNetCoordinates
from docling_jobkit.connectors.filenet.source_processor import (
    FileNetFileIdentifier,
    FileNetSourceProcessor,
)
from docling_jobkit.convert.materialization import SourceLimitExceededError

# Note: Same with GCP + Azure, we need to build actual integration tests at some point. S3 uses Minio


@pytest.fixture
def filenet_coords() -> FileNetCoordinates:
    return FileNetCoordinates(
        base_url="https://filenet.example.com/content-services-graphql",
        username="testuser",
        api_key="testkey",
        repository_id="test-repo",
    )


def test_initialize_does_not_log_connected_on_probe_failure(caplog) -> None:
    processor = FileNetSourceProcessor(MagicMock())
    with patch("docling_jobkit.connectors.filenet.helper.time.sleep"):
        with patch(
            "requests.post", side_effect=requests.ConnectionError("DNS failure")
        ):
            with pytest.raises(SourceConnectorUnavailableError):
                processor._initialize()

    assert "Connected to FileNet" not in caplog.text


def test_initialize_logs_connected_after_successful_probe(caplog) -> None:
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "data": {"documents": {"pageInfo": {"totalCount": 0}}}
    }
    processor = FileNetSourceProcessor(MagicMock())

    with caplog.at_level(logging.INFO):
        with patch("requests.post", return_value=mock_response):
            processor._initialize()

    assert "Connected to FileNet" in caplog.text


def test_filenet_list_respects_max_num_elements(filenet_coords):
    capped_coords = filenet_coords.model_copy(
        update={"folder_id": "{FOLDER-123}", "max_num_elements": 2}
    )
    processor = FileNetSourceProcessor(capped_coords)
    processor._auth_header = "test-auth"
    processor._graphql_url = "https://test.com/graphql"

    mock_folder_docs = [
        {
            "id": "{DOC-1}",
            "name": "doc1.pdf",
            "contentSize": 1000,
            "contentElements": [{"downloadUrl": "/content?id={DOC-1}"}],
        },
        {
            "id": "{DOC-2}",
            "name": "doc2.pdf",
            "contentSize": 2000,
            "contentElements": [{"downloadUrl": "/content?id={DOC-2}"}],
        },
        {
            "id": "{DOC-3}",
            "name": "doc3.pdf",
            "contentSize": 3000,
            "contentElements": [{"downloadUrl": "/content?id={DOC-3}"}],
        },
    ]

    with patch(
        "docling_jobkit.connectors.filenet.source_processor.list_folder_documents",
        return_value=iter(mock_folder_docs),
    ):
        doc_ids = list(processor._list_document_ids())

    assert len(doc_ids) == 2
    assert [doc.name for doc in doc_ids] == ["doc1.pdf", "doc2.pdf"]


def test_filenet_document_ids_is_optional_single_document_override(filenet_coords):
    assert filenet_coords.document_ids == []

    # Multiple document_ids are now supported (no longer raises ValidationError)
    coords_multi = filenet_coords.model_copy(
        update={"document_ids": ["{DOC-1}", "{DOC-2}"]}
    )
    assert coords_multi.document_ids == ["{DOC-1}", "{DOC-2}"]

    # Single document_id with folder_id
    coords = filenet_coords.model_copy(
        update={"folder_id": "{FOLDER-123}", "document_ids": ["{DOC-1}"]}
    )
    processor = FileNetSourceProcessor(coords)
    processor._auth_header = "test-auth"

    mock_doc = {
        "id": "{DOC-1}",
        "name": "doc1.pdf",
        "contentSize": 1000,
        "contentElements": [
            {
                "downloadUrl": "/content?id={DOC-1}",
            }
        ],
    }

    with (
        patch(
            "docling_jobkit.connectors.filenet.source_processor.list_docs_by_id",
            return_value=iter([mock_doc]),
        ) as list_docs,
        patch(
            "docling_jobkit.connectors.filenet.source_processor.list_folder_documents"
        ) as list_folder,
    ):
        docs = list(processor._list_document_ids())

    assert [doc.id for doc in docs] == ["{DOC-1}"]
    assert processor._count_documents() == 1
    list_docs.assert_called_once_with(coords, "test-auth", ["{DOC-1}"])
    list_folder.assert_not_called()


def test_filenet_count_clips_to_max_num_elements(filenet_coords):
    capped_coords = filenet_coords.model_copy(
        update={"folder_id": "{FOLDER-123}", "max_num_elements": 3}
    )
    processor = FileNetSourceProcessor(capped_coords)
    processor._auth_header = "test-auth"

    mock_folder_docs = [{"id": f"{{DOC-{i}}}", "name": f"doc{i}.pdf"} for i in range(5)]

    with patch(
        "docling_jobkit.connectors.filenet.source_processor.list_folder_documents",
        return_value=iter(mock_folder_docs),
    ):
        count = processor._count_documents()

    assert count == 3


def test_filenet_iterate_documents_respects_max_num_elements(filenet_coords):
    capped_coords = filenet_coords.model_copy(
        update={"folder_id": "{FOLDER-123}", "max_num_elements": 2}
    )
    processor = FileNetSourceProcessor(capped_coords)
    processor._initialized = True
    processor._auth_header = "test-auth"
    processor._graphql_url = "https://test.com/graphql"

    mock_folder_docs = [
        {
            "id": "{DOC-1}",
            "name": "doc1.pdf",
            "contentSize": 1000,
            "contentElements": [{"downloadUrl": "/content?id={DOC-1}"}],
        },
        {
            "id": "{DOC-2}",
            "name": "doc2.pdf",
            "contentSize": 2000,
            "contentElements": [{"downloadUrl": "/content?id={DOC-2}"}],
        },
        {
            "id": "{DOC-3}",
            "name": "doc3.pdf",
            "contentSize": 3000,
            "contentElements": [{"downloadUrl": "/content?id={DOC-3}"}],
        },
    ]

    with patch(
        "docling_jobkit.connectors.filenet.source_processor.list_folder_documents",
        return_value=iter(mock_folder_docs),
    ):
        processor._fetch_document_by_id = MagicMock(
            side_effect=lambda identifier, *, max_file_size=None: DocumentStream(
                name=identifier.name,
                stream=BytesIO(b"content"),
            )
        )

        docs = list(processor.iterate_documents())

    assert len(docs) == 2
    assert [doc.name for doc in docs] == ["doc1.pdf", "doc2.pdf"]
    assert processor._fetch_document_by_id.call_count == 2


def test_filenet_make_document_ref_uses_name_not_repr(filenet_coords):
    processor = FileNetSourceProcessor(filenet_coords)

    identifier = FileNetFileIdentifier(
        id="{DOC-1}",
        name="report.pdf",
        size=1000,
        mime_type="application/pdf",
        download_url="/content?id={DOC-1}&token=secret",
    )

    ref = processor._make_document_ref(identifier, source_index=0)

    # Filename now includes document ID for uniqueness
    assert ref.filename == "report-DOC-1.pdf"
    assert ref.source_uri == "filenet://test-repo/{DOC-1}"
    # Verify no sensitive information is leaked
    assert "download_url" not in ref.filename
    assert "secret" not in ref.filename
    assert "token" not in ref.filename


def test_filenet_fetch_rejects_oversized_before_download(filenet_coords):
    processor = FileNetSourceProcessor(filenet_coords)

    identifier = FileNetFileIdentifier(
        id="{DOC-LARGE}",
        name="large.pdf",
        size=10000,
        download_url="/content?id={DOC-LARGE}",
    )

    with pytest.raises(SourceLimitExceededError, match="max_file_size=8000"):
        processor._fetch_document_by_id(identifier, max_file_size=8000)


def test_filenet_fetch_logs_and_reraises_error(filenet_coords, caplog):
    processor = FileNetSourceProcessor(filenet_coords)

    identifier = FileNetFileIdentifier(
        id="{DOC-MISSING}",
        name="missing.pdf",
        size=1000,
        download_url="/content?id={DOC-MISSING}",
    )

    with patch("docling_jobkit.connectors.filenet.helper.time.sleep"):
        with patch(
            "requests.get",
            side_effect=requests.HTTPError("404 Not Found"),
        ):
            with caplog.at_level(logging.WARNING):
                with pytest.raises(SourceConnectorUnavailableError):
                    processor._fetch_document_by_id(identifier)


def test_filenet_unique_filenames_for_duplicate_names(filenet_coords):
    """Test that documents with the same name get unique filenames using doc ID."""
    processor = FileNetSourceProcessor(filenet_coords)
    processor._auth_header = "test-auth"
    processor._graphql_url = "https://test.com/graphql"
    processor._coords = filenet_coords

    # Two documents with the same name but different IDs
    identifier1 = FileNetFileIdentifier(
        id="{DOC-ABC-123}",
        name="report.pdf",
        size=1000,
        mime_type="application/pdf",
        download_url="/content?id={DOC-ABC-123}",
    )
    identifier2 = FileNetFileIdentifier(
        id="{DOC-XYZ-789}",
        name="report.pdf",
        size=2000,
        mime_type="application/pdf",
        download_url="/content?id={DOC-XYZ-789}",
    )

    # Test _make_document_ref creates unique filenames
    ref1 = processor._make_document_ref(identifier1, source_index=0)
    ref2 = processor._make_document_ref(identifier2, source_index=1)

    assert ref1.filename == "report-DOC-ABC-123.pdf"
    assert ref2.filename == "report-DOC-XYZ-789.pdf"
    assert ref1.filename != ref2.filename

    # Test _fetch_document_by_id uses the same unique filename
    mock_buffer1 = BytesIO(b"test content 1")
    mock_buffer2 = BytesIO(b"test content 2")
    with patch(
        "docling_jobkit.connectors.filenet.source_processor.download_document",
        side_effect=[mock_buffer1, mock_buffer2],
    ):
        stream1 = processor._fetch_document_by_id(identifier1)
        stream2 = processor._fetch_document_by_id(identifier2)

    assert stream1.name == "report-DOC-ABC-123.pdf"
    assert stream2.name == "report-DOC-XYZ-789.pdf"
    assert stream1.name == ref1.filename
    assert stream2.name == ref2.filename


def test_filenet_unique_filenames_without_extension(filenet_coords):
    """Test unique filename generation for files without extensions."""
    processor = FileNetSourceProcessor(filenet_coords)
    processor._coords = filenet_coords

    identifier = FileNetFileIdentifier(
        id="{DOC-NO-EXT}",
        name="README",
        size=500,
        download_url="/content?id={DOC-NO-EXT}",
    )

    ref = processor._make_document_ref(identifier, source_index=0)
    assert ref.filename == "README-DOC-NO-EXT"

    mock_buffer = BytesIO(b"readme content")
    with patch(
        "docling_jobkit.connectors.filenet.source_processor.download_document",
        return_value=mock_buffer,
    ):
        stream = processor._fetch_document_by_id(identifier)

    assert stream.name == "README-DOC-NO-EXT"
    assert stream.name == ref.filename
