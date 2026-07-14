import logging
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest
import requests

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.filenet_source_processor import (
    FileNetFileIdentifier,
    FileNetSourceProcessor,
)
from docling_jobkit.convert.materialization import SourceLimitExceededError
from docling_jobkit.datamodel.filenet_coords import FileNetCoordinates

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
    with patch("docling_jobkit.connectors.filenet_helper.time.sleep"):
        with patch(
            "requests.post", side_effect=requests.ConnectionError("DNS failure")
        ):
            with pytest.raises(requests.ConnectionError):
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
        {"id": "{DOC-1}", "name": "doc1.pdf"},
        {"id": "{DOC-2}", "name": "doc2.pdf"},
        {"id": "{DOC-3}", "name": "doc3.pdf"},
    ]

    mock_metadata_responses = [
        {
            "id": "{DOC-1}",
            "name": "doc1.pdf",
            "contentSize": 1000,
            "downloadUrl": "/content?id={DOC-1}",
        },
        {
            "id": "{DOC-2}",
            "name": "doc2.pdf",
            "contentSize": 2000,
            "downloadUrl": "/content?id={DOC-2}",
        },
    ]

    with patch(
        "docling_jobkit.connectors.filenet_source_processor.list_folder_documents",
        return_value=iter(mock_folder_docs),
    ):
        with patch(
            "docling_jobkit.connectors.filenet_source_processor.get_document_metadata",
            side_effect=mock_metadata_responses,
        ):
            doc_ids = list(processor._list_document_ids())

    assert len(doc_ids) == 2
    assert [doc.name for doc in doc_ids] == ["doc1.pdf", "doc2.pdf"]


def test_filenet_count_clips_to_max_num_elements(filenet_coords):
    capped_coords = filenet_coords.model_copy(
        update={"folder_id": "{FOLDER-123}", "max_num_elements": 3}
    )
    processor = FileNetSourceProcessor(capped_coords)
    processor._auth_header = "test-auth"

    mock_folder_docs = [{"id": f"{{DOC-{i}}}", "name": f"doc{i}.pdf"} for i in range(5)]

    with patch(
        "docling_jobkit.connectors.filenet_source_processor.list_folder_documents",
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
        {"id": "{DOC-1}", "name": "doc1.pdf"},
        {"id": "{DOC-2}", "name": "doc2.pdf"},
        {"id": "{DOC-3}", "name": "doc3.pdf"},
    ]

    mock_metadata_responses = [
        {
            "id": "{DOC-1}",
            "name": "doc1.pdf",
            "contentSize": 1000,
            "downloadUrl": "/content?id={DOC-1}",
        },
        {
            "id": "{DOC-2}",
            "name": "doc2.pdf",
            "contentSize": 2000,
            "downloadUrl": "/content?id={DOC-2}",
        },
    ]

    with patch(
        "docling_jobkit.connectors.filenet_source_processor.list_folder_documents",
        return_value=iter(mock_folder_docs),
    ):
        with patch(
            "docling_jobkit.connectors.filenet_source_processor.get_document_metadata",
            side_effect=mock_metadata_responses,
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

    with patch("docling_jobkit.connectors.filenet_helper.time.sleep"):
        with patch(
            "requests.get",
            side_effect=requests.HTTPError("404 Not Found"),
        ):
            with caplog.at_level(logging.WARNING):
                with pytest.raises(requests.HTTPError):
                    processor._fetch_document_by_id(identifier)
