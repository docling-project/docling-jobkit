from unittest.mock import MagicMock, patch

import pytest
from office365.runtime.client_request_exception import ClientRequestException
from pydantic import SecretStr, ValidationError
from requests import Response

from docling_jobkit.connectors.errors import ConnectorAuthenticationError
from docling_jobkit.connectors.sharepoint.target_processor import (
    SharePointTargetProcessor,
)
from docling_jobkit.datamodel.sharepoint_coords import (
    SharePointTargetCoordinates,
    TaskSharePointTarget,
)

_HELPER = "docling_jobkit.connectors.sharepoint.helper"


def _graph_error(status: int) -> ClientRequestException:
    """Build a ClientRequestException without its response-inspecting __init__."""
    exc = ClientRequestException.__new__(ClientRequestException)
    response = Response()
    response.status_code = status
    exc.response = response
    return exc


@pytest.fixture
def coords() -> SharePointTargetCoordinates:
    return SharePointTargetCoordinates(
        tenant="t",
        client_id="c",
        client_secret=SecretStr("s"),
        site_url="https://contoso.sharepoint.com/sites/Marketing",
        folder_path="out",
    )


def _proc(coords: SharePointTargetCoordinates) -> SharePointTargetProcessor:
    processor = SharePointTargetProcessor(coords)
    processor._drive = MagicMock()
    return processor


def test_get_config_types():
    assert SharePointTargetProcessor.get_config_types() == (TaskSharePointTarget,)


def test_target_coords_reject_no_target():
    with pytest.raises(ValidationError, match="Exactly one"):
        SharePointTargetCoordinates(
            tenant="t", client_id="c", client_secret=SecretStr("s")
        )


def test_target_coords_omit_read_only_fields():
    fields = SharePointTargetCoordinates.model_fields
    assert "file_ids" not in fields
    assert "max_num_elements" not in fields


def test_initialize_resolves_drive(coords):
    processor = SharePointTargetProcessor(coords)
    with (
        patch(f"{_HELPER}.get_client", return_value="the-client") as get_client,
        patch(f"{_HELPER}.resolve_drive", return_value="the-drive") as resolve_drive,
        patch(f"{_HELPER}.check_connection") as check_connection,
    ):
        processor._initialize()

    get_client.assert_called_once_with(coords)
    resolve_drive.assert_called_once_with("the-client", coords)
    check_connection.assert_called_once_with("the-client", "the-drive")
    assert processor._drive == "the-drive"


def test_upload_file_delegates_with_folder_path(coords):
    processor = _proc(coords)
    with patch(f"{_HELPER}.upload_file") as upload_file:
        processor.upload_file("local.pdf", "pdf/doc.pdf", "application/pdf")

    upload_file.assert_called_once_with(
        processor._drive, "local.pdf", "out", "pdf/doc.pdf"
    )


def test_upload_object_delegates_with_folder_path(coords):
    processor = _proc(coords)
    with patch(f"{_HELPER}.upload_object") as upload_object:
        processor.upload_object(b"data", "md/doc.md", "text/markdown")

    upload_object.assert_called_once_with(processor._drive, b"data", "out", "md/doc.md")


@pytest.mark.parametrize("status", [401, 403], ids=["401", "403"])
def test_upload_maps_auth_error(coords, status):
    processor = _proc(coords)
    with patch(f"{_HELPER}.upload_file", side_effect=_graph_error(status)):
        with pytest.raises(ConnectorAuthenticationError):
            processor.upload_file("local.pdf", "pdf/doc.pdf", "application/pdf")
