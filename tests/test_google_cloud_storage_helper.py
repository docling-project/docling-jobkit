from unittest.mock import MagicMock, patch

from google.cloud import storage

from docling_jobkit.connectors.google_cloud_storage_helper import get_client
from docling_jobkit.datamodel.google_cloud_storage_coords import (
    GoogleCloudStorageCoordinates,
)


def test_get_client_uses_service_account_key_when_provided() -> None:
    sa_key = MagicMock()
    sa_key.model_dump.return_value = {
        "project_id": "my-project",
        "private_key": "fake-key",
    }

    coords = GoogleCloudStorageCoordinates.model_construct(
        bucket="my-bucket",
        project="my-project",
        service_account_key=sa_key,
    )

    with patch.object(storage.Client, "from_service_account_info") as mock_from_info:
        client = get_client(coords)

    mock_from_info.assert_called_once_with(
        {"project_id": "my-project", "private_key": "fake-key"}, project="my-project"
    )
    assert client is mock_from_info.return_value


def test_get_client_falls_back_to_adc_without_key() -> None:
    coords = GoogleCloudStorageCoordinates.model_construct(
        bucket="my-bucket", project="my-project"
    )

    with patch.object(storage, "Client") as mock_client:
        client = get_client(coords)

    mock_client.assert_called_once_with(project="my-project")
    assert client is mock_client.return_value
