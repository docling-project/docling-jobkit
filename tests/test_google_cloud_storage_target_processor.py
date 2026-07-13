from io import BytesIO
from unittest.mock import MagicMock

import pytest

from docling.datamodel.service.sources import (
    GoogleCloudStorageCoordinates,
)

from docling_jobkit.connectors.google_cloud_storage_target_processor import (
    GoogleCloudStorageTargetProcessor,
)


@pytest.fixture
def coords() -> GoogleCloudStorageCoordinates:
    return GoogleCloudStorageCoordinates(bucket="test", key_prefix="target/")


# ----------------- Key / URI construction -----------------


def test_build_full_key_prepends_prefix(coords):
    processor = GoogleCloudStorageTargetProcessor(coords)
    assert processor._build_full_key("out.json") == "target/out.json"


def test_build_full_key_without_prefix():
    processor = GoogleCloudStorageTargetProcessor(
        GoogleCloudStorageCoordinates(bucket="test", key_prefix="")
    )
    assert processor._build_full_key("out.json") == "out.json"


def test_build_artifact_uri(coords):
    processor = GoogleCloudStorageTargetProcessor(coords)
    assert processor.build_artifact_uri("out.json") == "gs://test/target/out.json"


# ----------------- Uploads -----------------


def test_upload_file_uses_full_key_and_content_type(coords):
    processor = GoogleCloudStorageTargetProcessor(coords)
    processor._client = MagicMock()
    blob = processor._client.bucket.return_value.blob.return_value

    processor.upload_file("/tmp/out.json", "out.json", "application/json")

    processor._client.bucket.assert_called_once_with("test")
    processor._client.bucket.return_value.blob.assert_called_once_with(
        "target/out.json"
    )
    blob.upload_from_filename.assert_called_once_with(
        "/tmp/out.json", content_type="application/json"
    )


def test_upload_object_bytes_uses_upload_from_string(coords):
    processor = GoogleCloudStorageTargetProcessor(coords)
    processor._client = MagicMock()
    blob = processor._client.bucket.return_value.blob.return_value

    processor.upload_object(b"data", "out.json", "application/json")

    blob.upload_from_string.assert_called_once_with(
        b"data", content_type="application/json"
    )
    blob.upload_from_file.assert_not_called()


def test_upload_object_str_uses_upload_from_string(coords):
    processor = GoogleCloudStorageTargetProcessor(coords)
    processor._client = MagicMock()
    blob = processor._client.bucket.return_value.blob.return_value

    processor.upload_object("data", "out.md", "text/markdown")

    blob.upload_from_string.assert_called_once_with(
        "data", content_type="text/markdown"
    )
    blob.upload_from_file.assert_not_called()


def test_upload_object_filelike_uses_upload_from_file(coords):
    processor = GoogleCloudStorageTargetProcessor(coords)
    processor._client = MagicMock()
    blob = processor._client.bucket.return_value.blob.return_value
    stream = BytesIO(b"data")

    processor.upload_object(stream, "out.json", "application/json")

    blob.upload_from_file.assert_called_once_with(
        stream, content_type="application/json"
    )
    blob.upload_from_string.assert_not_called()
