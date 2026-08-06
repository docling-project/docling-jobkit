from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("office365")

from docling_jobkit.connectors.errors import (
    ConnectorAuthenticationError,
    TargetConnectorConfigError,
)
from docling_jobkit.connectors.sharepoint.target_processor import (
    SharePointTargetProcessor,
)

_HELPER = "docling_jobkit.connectors.sharepoint.helper"


def _proc(coords) -> SharePointTargetProcessor:
    processor = SharePointTargetProcessor(coords)
    processor._drive = MagicMock()
    return processor


def test_initialize_resolves_drive(sp_target_coords):
    processor = SharePointTargetProcessor(sp_target_coords)
    with (
        patch(f"{_HELPER}.get_client", return_value="the-client") as get_client,
        patch(f"{_HELPER}.resolve_drive", return_value="the-drive") as resolve_drive,
        patch(f"{_HELPER}.check_connection") as check_connection,
    ):
        processor._initialize()

    get_client.assert_called_once_with(sp_target_coords)
    resolve_drive.assert_called_once_with("the-client", sp_target_coords)
    check_connection.assert_called_once_with("the-client", "the-drive")
    assert processor._drive == "the-drive"


def test_initialize_reports_unresolvable_drive_as_target_config_error(sp_target_coords):
    """The shared helper raises a neutral error; the target must not surface it as a
    *source* connector failure."""
    from docling_jobkit.connectors.sharepoint.helper import SharePointDriveNotFoundError

    processor = SharePointTargetProcessor(sp_target_coords)
    with (
        patch(f"{_HELPER}.get_client"),
        patch(
            f"{_HELPER}.resolve_drive",
            side_effect=SharePointDriveNotFoundError("library 'Missing' not found"),
        ),
    ):
        with pytest.raises(TargetConnectorConfigError, match="Missing"):
            processor._initialize()


@pytest.mark.parametrize(
    "method, helper_name, payload",
    [
        ("upload_file", "upload_file", "local.pdf"),
        ("upload_object", "upload_object", b"data"),
    ],
    ids=["file", "object"],
)
def test_upload_resolves_destination_from_folder_path(
    sp_target_coords, method, helper_name, payload
):
    processor = _proc(sp_target_coords)
    folder = MagicMock()

    with (
        patch(f"{_HELPER}.get_or_create_folder") as get_or_create,
        patch(f"{_HELPER}.folder_handle", return_value=folder) as handle,
        patch(f"{_HELPER}.{helper_name}") as upload,
    ):
        getattr(processor, method)(payload, "json/doc.json", "application/json")

    # coords.folder_path ("out") is prefixed onto the artifact's relative key
    get_or_create.assert_called_once_with(processor._drive, "out/json")
    handle.assert_called_once_with(processor._drive, "out/json")
    upload.assert_called_once_with(folder, payload, "doc.json")


def test_folder_creation_is_walked_once_per_destination(sp_target_coords):
    """get_or_create_folder costs a round-trip per path segment and runs on every
    artifact — including once per page image — so it must not repeat per upload."""
    processor = _proc(sp_target_coords)

    with (
        patch(f"{_HELPER}.get_or_create_folder") as get_or_create,
        patch(f"{_HELPER}.folder_handle") as handle,
        patch(f"{_HELPER}.upload_object"),
    ):
        processor.upload_object(b"a", "images/one.png", "image/png")
        processor.upload_object(b"b", "images/two.png", "image/png")
        processor.upload_object(b"c", "json/doc.json", "application/json")

    assert [call.args[1] for call in get_or_create.call_args_list] == [
        "out/images",
        "out/json",
    ]
    # the handle itself is rebuilt per upload — it costs no request, and sharing one
    # would retain a children entry per uploaded artifact for the whole batch
    assert handle.call_count == 3


def test_finalize_forgets_which_folders_were_ensured(sp_target_coords):
    processor = _proc(sp_target_coords)
    with (
        patch(f"{_HELPER}.get_or_create_folder"),
        patch(f"{_HELPER}.folder_handle"),
        patch(f"{_HELPER}.upload_object"),
    ):
        processor.upload_object(b"a", "json/doc.json", "application/json")

    assert processor._ensured_folders == {"out/json"}
    processor._finalize()
    assert processor._ensured_folders == set()


def test_upload_maps_auth_error(sp_target_coords, graph_error):
    processor = _proc(sp_target_coords)
    with (
        patch(f"{_HELPER}.get_or_create_folder"),
        patch(f"{_HELPER}.folder_handle"),
        patch(f"{_HELPER}.upload_file", side_effect=graph_error(403)),
    ):
        with pytest.raises(ConnectorAuthenticationError):
            processor.upload_file("local.pdf", "pdf/doc.pdf", "application/pdf")
