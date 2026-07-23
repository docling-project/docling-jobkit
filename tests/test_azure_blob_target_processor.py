from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from azure.core.exceptions import ClientAuthenticationError

from docling.datamodel.service.sources import AzureBlobCoordinates

from docling_jobkit.connectors.azure_blob.target_processor import (
    AzureBlobTargetProcessor,
)
from docling_jobkit.connectors.errors import ConnectorAuthenticationError


@pytest.fixture
def azure_coords() -> AzureBlobCoordinates:
    return AzureBlobCoordinates(
        account_name="testaccount",
        container="testcontainer",
        connection_string="DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net",
        blob_prefix="",
    )


def test_azure_blob_build_artifact_uri_without_prefix(azure_coords):
    processor = AzureBlobTargetProcessor(azure_coords)

    uri = processor.build_artifact_uri("output/document.json")

    assert uri == "azure://testaccount/testcontainer/output/document.json"


def test_azure_blob_build_artifact_uri_with_prefix(azure_coords):
    coords_with_prefix = azure_coords.model_copy(update={"blob_prefix": "converted/"})
    processor = AzureBlobTargetProcessor(coords_with_prefix)

    uri = processor.build_artifact_uri("output/document.json")

    assert uri == "azure://testaccount/testcontainer/converted/output/document.json"


def test_azure_blob_upload_file(azure_coords):
    processor = AzureBlobTargetProcessor(azure_coords)

    mock_blob_client = MagicMock()
    processor._container_client = MagicMock()
    processor._container_client.get_blob_client.return_value = mock_blob_client

    with patch(
        "docling_jobkit.connectors.azure_blob.upload_support.upload_azure_blob_file"
    ) as mock_upload:
        processor.upload_file(
            filename=Path("/tmp/test.json"),
            target_filename="output/test.json",
            content_type="application/json",
        )

        processor._container_client.get_blob_client.assert_called_once_with(
            "output/test.json"
        )
        mock_upload.assert_called_once_with(
            mock_blob_client,
            filename=Path("/tmp/test.json"),
            content_type="application/json",
        )


def test_azure_blob_upload_file_with_prefix(azure_coords):
    coords_with_prefix = azure_coords.model_copy(update={"blob_prefix": "converted/"})
    processor = AzureBlobTargetProcessor(coords_with_prefix)

    mock_blob_client = MagicMock()
    processor._container_client = MagicMock()
    processor._container_client.get_blob_client.return_value = mock_blob_client

    with patch(
        "docling_jobkit.connectors.azure_blob.upload_support.upload_azure_blob_file"
    ):
        processor.upload_file(
            filename=Path("/tmp/test.json"),
            target_filename="output/test.json",
            content_type="application/json",
        )

        processor._container_client.get_blob_client.assert_called_once_with(
            "converted/output/test.json"
        )


def test_azure_blob_upload_object(azure_coords):
    processor = AzureBlobTargetProcessor(azure_coords)

    mock_blob_client = MagicMock()
    processor._container_client = MagicMock()
    processor._container_client.get_blob_client.return_value = mock_blob_client

    with patch(
        "docling_jobkit.connectors.azure_blob.upload_support.upload_azure_blob_object"
    ) as mock_upload:
        processor.upload_object(
            obj=b"test content",
            target_filename="output/test.json",
            content_type="application/json",
        )

        processor._container_client.get_blob_client.assert_called_once_with(
            "output/test.json"
        )
        mock_upload.assert_called_once_with(
            mock_blob_client,
            obj=b"test content",
            content_type="application/json",
        )


def test_azure_blob_upload_object_with_prefix(azure_coords):
    coords_with_prefix = azure_coords.model_copy(update={"blob_prefix": "converted/"})
    processor = AzureBlobTargetProcessor(coords_with_prefix)

    mock_blob_client = MagicMock()
    processor._container_client = MagicMock()
    processor._container_client.get_blob_client.return_value = mock_blob_client

    with patch(
        "docling_jobkit.connectors.azure_blob.upload_support.upload_azure_blob_object"
    ):
        processor.upload_object(
            obj="test string",
            target_filename="output/test.txt",
            content_type="text/plain",
        )

        processor._container_client.get_blob_client.assert_called_once_with(
            "converted/output/test.txt"
        )


def test_azure_blob_target_authentication_error_is_client_actionable(azure_coords):
    processor = AzureBlobTargetProcessor(azure_coords)
    processor._container_client = MagicMock()

    with (
        patch(
            "docling_jobkit.connectors.azure_blob.upload_support."
            "upload_azure_blob_object",
            side_effect=ClientAuthenticationError("invalid credentials"),
        ),
        pytest.raises(
            ConnectorAuthenticationError,
            match="Azure Blob Storage authentication",
        ),
    ):
        processor.upload_object(b"data", "out.json", "application/json")
