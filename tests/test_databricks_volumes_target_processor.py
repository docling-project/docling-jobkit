from io import BytesIO
from unittest.mock import MagicMock, call, patch

import pytest
import requests

from docling_jobkit.connectors.databricks_volumes.models import (
    DatabricksVolumesCoordinates,
)
from docling_jobkit.connectors.databricks_volumes.target_processor import (
    DatabricksVolumesTargetProcessor,
)
from docling_jobkit.connectors.errors import ConnectorAuthenticationError


@pytest.fixture
def dbvol_coords() -> DatabricksVolumesCoordinates:
    return DatabricksVolumesCoordinates(
        workspace_host="dbc-xxxxxxx.cloud.databricks.com",
        token="tok",
        volume_path="/Volumes/main/default/output",
    )


def _make_http_exc(status_code: int) -> requests.HTTPError:
    response = MagicMock()
    response.status_code = status_code
    return requests.HTTPError(response=response)


def test_build_artifact_uri(dbvol_coords):
    processor = DatabricksVolumesTargetProcessor(dbvol_coords)

    uri = processor.build_artifact_uri("output/doc.json")

    assert uri == (
        "databricks_volumes://dbc-xxxxxxx.cloud.databricks.com"
        "/Volumes/main/default/output/output/doc.json"
    )


def test_initialize_ensures_directory(dbvol_coords):
    processor = DatabricksVolumesTargetProcessor(dbvol_coords)

    with patch(
        "docling_jobkit.connectors.databricks_volumes.target_processor.ensure_directory"
    ) as mock_ensure:
        processor._initialize()

    mock_ensure.assert_called_once_with(
        "dbc-xxxxxxx.cloud.databricks.com", "tok", "/Volumes/main/default/output"
    )


def test_upload_creates_the_nested_parent_directory(dbvol_coords):
    """target_filename is always nested (json/, md/, pdf/, ...), and the Files
    API upload endpoint does not create missing parents."""
    processor = DatabricksVolumesTargetProcessor(dbvol_coords)

    with (
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.ensure_directory"
        ) as mock_ensure,
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.upload_document"
        ),
    ):
        processor._initialize()
        processor.upload_object(b"{}", "json/doc.json", "application/json")

    assert mock_ensure.call_args_list[-1] == call(
        "dbc-xxxxxxx.cloud.databricks.com", "tok", "/Volumes/main/default/output/json"
    )


def test_parent_directories_are_created_once_per_prefix(dbvol_coords, tmp_path):
    """mkdir is per distinct output prefix, not per document."""
    processor = DatabricksVolumesTargetProcessor(dbvol_coords)
    local = tmp_path / "doc.html"
    local.write_bytes(b"<html></html>")

    with (
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.ensure_directory"
        ) as mock_ensure,
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.upload_document"
        ),
    ):
        processor._initialize()
        processor.upload_object(b"a", "json/a.json", "application/json")
        processor.upload_object(b"b", "json/b.json", "application/json")
        processor.upload_object("md", "md/a.md", "text/markdown")
        processor.upload_file(local, "html/a.html", "text/html")
        processor.upload_object(b"top", "summary.txt", "text/plain")

    ensured = [c.args[2] for c in mock_ensure.call_args_list]
    assert ensured == [
        "/Volumes/main/default/output",
        "/Volumes/main/default/output/json",
        "/Volumes/main/default/output/md",
        "/Volumes/main/default/output/html",
    ]


def test_finalize_clears_the_directory_cache(dbvol_coords):
    """A reopened processor must not assume directories from a previous cycle
    still exist."""
    processor = DatabricksVolumesTargetProcessor(dbvol_coords)

    with (
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.ensure_directory"
        ) as mock_ensure,
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.upload_document"
        ),
    ):
        processor._initialize()
        processor.upload_object(b"a", "json/a.json", "application/json")
        processor._finalize()
        processor._initialize()
        processor.upload_object(b"a", "json/a.json", "application/json")

    ensured = [c.args[2] for c in mock_ensure.call_args_list]
    assert ensured == [
        "/Volumes/main/default/output",
        "/Volumes/main/default/output/json",
        "/Volumes/main/default/output",
        "/Volumes/main/default/output/json",
    ]


def test_upload_file_reads_from_disk_and_uploads(dbvol_coords, tmp_path):
    processor = DatabricksVolumesTargetProcessor(dbvol_coords)
    file_path = tmp_path / "doc.json"
    file_path.write_bytes(b"file contents")

    captured: dict = {}

    def _capture(host, token, path, data, *, content_type):
        captured["host"] = host
        captured["token"] = token
        captured["path"] = path
        captured["data"] = data.read()
        captured["content_type"] = content_type

    with (
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.upload_document",
            side_effect=_capture,
        ),
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.ensure_directory"
        ),
    ):
        processor.upload_file(file_path, "output/doc.json", "application/json")

    assert captured == {
        "host": "dbc-xxxxxxx.cloud.databricks.com",
        "token": "tok",
        "path": "/Volumes/main/default/output/output/doc.json",
        "data": b"file contents",
        "content_type": "application/json",
    }


def test_upload_object_bytes(dbvol_coords):
    processor = DatabricksVolumesTargetProcessor(dbvol_coords)

    with (
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.upload_document"
        ) as mock_upload,
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.ensure_directory"
        ),
    ):
        processor.upload_object(b"raw bytes", "output/doc.json", "application/json")

    mock_upload.assert_called_once_with(
        "dbc-xxxxxxx.cloud.databricks.com",
        "tok",
        "/Volumes/main/default/output/output/doc.json",
        b"raw bytes",
        content_type="application/json",
    )


def test_upload_object_str_is_utf8_encoded(dbvol_coords):
    processor = DatabricksVolumesTargetProcessor(dbvol_coords)

    with (
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.upload_document"
        ) as mock_upload,
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.ensure_directory"
        ),
    ):
        processor.upload_object("héllo", "output/doc.txt", "text/plain")

    mock_upload.assert_called_once_with(
        "dbc-xxxxxxx.cloud.databricks.com",
        "tok",
        "/Volumes/main/default/output/output/doc.txt",
        "héllo".encode("utf-8"),
        content_type="text/plain",
    )


def test_upload_object_file_like_passthrough(dbvol_coords):
    processor = DatabricksVolumesTargetProcessor(dbvol_coords)
    buf = BytesIO(b"stream contents")

    with (
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.upload_document"
        ) as mock_upload,
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.ensure_directory"
        ),
    ):
        processor.upload_object(buf, "output/doc.bin", "application/octet-stream")

    mock_upload.assert_called_once_with(
        "dbc-xxxxxxx.cloud.databricks.com",
        "tok",
        "/Volumes/main/default/output/output/doc.bin",
        buf,
        content_type="application/octet-stream",
    )


def test_target_authentication_error_is_client_actionable(dbvol_coords):
    processor = DatabricksVolumesTargetProcessor(dbvol_coords)

    with (
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.upload_document",
            side_effect=_make_http_exc(403),
        ),
        patch(
            "docling_jobkit.connectors.databricks_volumes.target_processor.ensure_directory"
        ),
        pytest.raises(
            ConnectorAuthenticationError,
            match="Databricks Volumes authentication",
        ),
    ):
        processor.upload_object(b"data", "out.json", "application/json")
