from io import BytesIO
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("box_sdk_gen")

from box_sdk_gen import FileFull, FolderMini
from pydantic import SecretStr

from docling_jobkit.connectors.box.models import BoxTarget
from docling_jobkit.connectors.box.target_processor import BoxTargetProcessor
from docling_jobkit.connectors.errors import ConnectorAuthenticationError

_MOD = "docling_jobkit.connectors.box.target_processor"


@pytest.fixture
def box_target_coords() -> BoxTarget:
    return BoxTarget(
        client_id="client-id",
        client_secret=SecretStr("secret"),
        enterprise_id="enterprise-id",
        folder_id="root-folder",
    )


def test_subfolder_is_created_once_and_reused_across_uploads(box_target_coords):
    """resolve_target_folder's cache must survive across upload calls within one
    open/close cycle, or every document re-walks and re-creates the same
    subfolder tree instead of reusing it."""
    processor = BoxTargetProcessor(box_target_coords)
    client = MagicMock()
    client.folders.get_folder_items.return_value = SimpleNamespace(
        entries=[], next_marker=None
    )
    client.folders.create_folder.return_value = FolderMini(
        id="json-folder", name="json"
    )
    processor._client = client

    with patch(f"{_MOD}.upload_document") as mock_upload:
        processor.upload_object(b"a", "json/a.json", "application/json")
        processor.upload_object(b"b", "json/b.json", "application/json")

    assert client.folders.create_folder.call_count == 1
    folder_ids = [c.args[1] for c in mock_upload.call_args_list]
    assert folder_ids == ["json-folder", "json-folder"]


def test_large_object_upload_routes_through_the_chunked_session(
    box_target_coords, monkeypatch
):
    """End-to-end through the processor (not just upload_document in isolation):
    a large object must actually reach the chunked-session path, proving the
    wiring from upload_object through resolve_target_folder/upload_document
    holds together."""
    from docling_jobkit.connectors.box import helper as box_helper

    monkeypatch.setattr(box_helper, "_CHUNKED_THRESHOLD", 10)
    processor = BoxTargetProcessor(box_target_coords)
    client = MagicMock()
    client.chunked_uploads.create_file_upload_session.return_value = SimpleNamespace(
        id="sess-1", part_size=4
    )
    client.chunked_uploads.upload_file_part.side_effect = (
        lambda session_id, body, **kw: SimpleNamespace(
            part=SimpleNamespace(data=body.read())
        )
    )
    client.chunked_uploads.create_file_upload_session_commit.return_value = (
        SimpleNamespace(entries=[FileFull(id="new-file-id", name="big.pdf", size=10)])
    )
    processor._client = client

    processor.upload_object(b"0123456789", "big.pdf", "application/pdf")

    client.chunked_uploads.create_file_upload_session.assert_called_once_with(
        "root-folder", 10, "big.pdf"
    )
    _, parts = client.chunked_uploads.create_file_upload_session_commit.call_args.args
    assert b"".join(p.data for p in parts) == b"0123456789"


def test_auth_failure_is_translated_to_a_client_actionable_error(
    box_target_coords, box_api_error
):
    processor = BoxTargetProcessor(box_target_coords)
    processor._client = MagicMock()

    with (
        patch(f"{_MOD}.resolve_target_folder", return_value=("folder-1", "doc.json")),
        patch(f"{_MOD}.upload_document", side_effect=box_api_error(403)),
        pytest.raises(ConnectorAuthenticationError, match="Box authentication"),
    ):
        processor.upload_object(b"data", "doc.json", "application/json")


def test_seekable_object_is_uploaded_without_being_copied(box_target_coords):
    """_as_seekable_stream must pass an already-seekable stream through as-is
    rather than reading it fully into a new buffer, to keep memory flat for
    large in-memory objects."""
    processor = BoxTargetProcessor(box_target_coords)
    processor._client = MagicMock()
    buf = BytesIO(b"stream contents")

    with (
        patch(f"{_MOD}.resolve_target_folder", return_value=("folder-1", "doc.bin")),
        patch(f"{_MOD}.upload_document") as mock_upload,
    ):
        processor.upload_object(buf, "bin/doc.bin", "application/octet-stream")

    assert mock_upload.call_args.args[3] is buf
