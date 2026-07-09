from unittest.mock import patch

from google.cloud import storage

from docling_jobkit.connectors.google_cloud_storage_helper import get_client
from docling_jobkit.datamodel.google_cloud_storage_coords import (
    GoogleCloudStorageCoordinates,
)


def test_get_client_uses_service_account_key_when_provided() -> None:
    coords = GoogleCloudStorageCoordinates(
        bucket="my-bucket",
        project="my-project",
        service_account_key_path="/tmp/sa-key.json",
    )

    with patch.object(storage.Client, "from_service_account_json") as mock_from_json:
        client = get_client(coords)

    mock_from_json.assert_called_once_with("/tmp/sa-key.json", project="my-project")
    assert client is mock_from_json.return_value


def test_get_client_falls_back_to_adc_without_key() -> None:
    # No key path → Application Default Credentials / Workload Identity.
    coords = GoogleCloudStorageCoordinates(bucket="my-bucket", project="my-project")

    with patch.object(storage, "Client") as mock_client:
        client = get_client(coords)

    mock_client.assert_called_once_with(project="my-project")
    assert client is mock_client.return_value
