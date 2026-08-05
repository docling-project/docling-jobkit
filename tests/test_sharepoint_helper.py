import os
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from office365.runtime.client_request_exception import ClientRequestException
from pydantic import SecretStr
from requests import ConnectionError as RequestsConnectionError, Response, Timeout

from docling_jobkit.connectors.errors import SourceConnectorPolicyError
from docling_jobkit.connectors.sharepoint import helper
from docling_jobkit.datamodel.sharepoint_coords import SharePointConnection


@pytest.fixture
def sp_coords() -> SharePointConnection:
    """SharePoint target (site_url)."""
    return SharePointConnection(
        tenant="tenant-guid",
        client_id="client-id",
        client_secret=SecretStr("secret"),
        site_url="https://contoso.sharepoint.com/sites/Marketing",
    )


@pytest.fixture
def od_coords() -> SharePointConnection:
    """OneDrive target (onedrive_user)."""
    return SharePointConnection(
        tenant="tenant-guid",
        client_id="client-id",
        client_secret=SecretStr("secret"),
        onedrive_user="alice@contoso.com",
    )


def _drive_item(id: str, name: str, size: int, is_folder: bool = False) -> MagicMock:
    """Stand-in for an office365 DriveItem (only the fields _to_file_meta reads)."""
    item = MagicMock()
    item.id = id
    item.name = name
    item.is_folder = is_folder
    item.last_modified_datetime = None
    item.get_property.side_effect = lambda key, default=None: (
        size if key == "size" else default
    )
    return item


def _graph_error(status: int) -> ClientRequestException:
    """Build a ClientRequestException without its response-inspecting __init__."""
    exc = ClientRequestException.__new__(ClientRequestException)
    response = Response()
    response.status_code = status
    exc.response = response
    return exc


def test_get_client_builds_confidential_client(sp_coords):
    with patch("office365.graph_client.GraphClient") as graph_client:
        client = helper.get_client(sp_coords)

    graph_client.assert_called_once_with(tenant="tenant-guid")
    graph_client.return_value.with_client_secret.assert_called_once_with(
        "client-id", "secret"
    )
    assert client is graph_client.return_value.with_client_secret.return_value


def test_resolve_drive_uses_default_library(sp_coords):
    client = MagicMock()
    site = (
        client.sites.get_by_url.return_value.get.return_value.execute_query.return_value
    )

    drive = helper.resolve_drive(client, sp_coords)

    assert drive is site.drive
    client.sites.get_by_url.assert_called_once_with(sp_coords.site_url)


def test_resolve_drive_selects_named_library(sp_coords):
    coords = sp_coords.model_copy(update={"document_library": "Contracts"})
    client = MagicMock()
    site = (
        client.sites.get_by_url.return_value.get.return_value.execute_query.return_value
    )
    documents = SimpleNamespace(name="Documents")
    contracts = SimpleNamespace(name="Contracts")
    site.drives.get_all.return_value.execute_query.return_value = [documents, contracts]

    drive = helper.resolve_drive(client, coords)

    assert drive is contracts


def test_resolve_drive_named_library_not_found_raises(sp_coords):
    coords = sp_coords.model_copy(update={"document_library": "Missing"})
    client = MagicMock()
    site = (
        client.sites.get_by_url.return_value.get.return_value.execute_query.return_value
    )
    site.drives.get_all.return_value.execute_query.return_value = [
        SimpleNamespace(name="Documents")
    ]

    with pytest.raises(SourceConnectorPolicyError, match="Missing"):
        helper.resolve_drive(client, coords)


def test_resolve_drive_onedrive_uses_user_drive(od_coords):
    client = MagicMock()

    drive = helper.resolve_drive(client, od_coords)

    assert drive is client.users.get_by_principal_name.return_value.drive
    client.users.get_by_principal_name.assert_called_once_with("alice@contoso.com")


def test_list_folder_items_yields_file_meta_from_root():
    client = MagicMock()
    drive = MagicMock()
    drive.root.get_files.return_value = [
        _drive_item("1", "a.pdf", 10),
        _drive_item("2", "b.pdf", 20),
    ]

    metas = list(helper.list_folder_items(client, drive, None))

    assert [m["id"] for m in metas] == ["1", "2"]
    assert metas[1]["size"] == 20
    drive.root.get_files.assert_called_once_with(
        recursive=True, page_size=helper._DEFAULT_PAGE_SIZE
    )
    client.execute_query.assert_called_once()


def test_list_folder_items_navigates_to_folder_path():
    client = MagicMock()
    drive = MagicMock()
    drive.root.get_by_path.return_value.get_files.return_value = [
        _drive_item("1", "a.pdf", 10)
    ]

    metas = list(helper.list_folder_items(client, drive, "/Reports/2026"))

    drive.root.get_by_path.assert_called_once_with("/Reports/2026")
    assert [m["id"] for m in metas] == ["1"]


def test_list_items_by_id_skips_folders():
    client = MagicMock()
    drive = MagicMock()
    file_item = _drive_item("1", "a.pdf", 10)
    folder_item = _drive_item("2", "subfolder", 0, is_folder=True)
    drive.items.__getitem__.side_effect = lambda key: {
        "1": file_item,
        "2": folder_item,
    }[key]

    metas = list(helper.list_items_by_id(client, drive, ["1", "2"]))

    assert [m["id"] for m in metas] == ["1"]


@pytest.mark.parametrize(
    "status, expected",
    [(401, True), (403, True), (404, False), (500, False)],
    ids=["401", "403", "404", "500"],
)
def test_is_authentication_error(status, expected):
    assert helper.is_sharepoint_authentication_error(_graph_error(status)) is expected


@pytest.mark.parametrize(
    "status, expected",
    [(500, True), (503, True), (401, False), (404, False)],
    ids=["500", "503", "401", "404"],
)
def test_is_unavailable_error_by_status(status, expected):
    assert helper.is_sharepoint_unavailable_error(_graph_error(status)) is expected


def test_is_unavailable_error_covers_connection_and_timeout():
    assert helper.is_sharepoint_unavailable_error(RequestsConnectionError()) is True
    assert helper.is_sharepoint_unavailable_error(Timeout()) is True


def test_predicates_ignore_unrelated_exceptions():
    assert helper.is_sharepoint_authentication_error(ValueError()) is False
    assert helper.is_sharepoint_unavailable_error(ValueError()) is False


# upload helpers tests


@pytest.mark.parametrize(
    "base_folder, target_filename, expected",
    [
        (None, "doc.json", (None, "doc.json")),
        ("out", "doc.json", ("out", "doc.json")),
        ("out", "json/doc.json", ("out/json", "doc.json")),
        ("/a/", "p/q/f.png", ("a/p/q", "f.png")),
        (None, "pages/1/img.png", ("pages/1", "img.png")),
    ],
)
def test_resolve_destination(base_folder, target_filename, expected):
    assert helper.resolve_destination(base_folder, target_filename) == expected


def test_get_or_create_folder_returns_root_for_empty_path():
    drive = MagicMock()
    assert helper.get_or_create_folder(drive, None) is drive.root
    drive.root.get_by_path.assert_not_called()


def test_get_or_create_folder_returns_existing():
    drive = MagicMock()
    existing = MagicMock(name="existing")
    drive.root.get_by_path.return_value.get.return_value.execute_query.return_value = (
        existing
    )

    result = helper.get_or_create_folder(drive, "out")

    assert result is existing
    drive.root.get_by_path.assert_called_once_with("out")
    drive.root.create_folder.assert_not_called()


def test_get_or_create_folder_creates_missing_and_returns_created():
    drive = MagicMock()
    child = drive.root.get_by_path.return_value
    child.get.return_value.execute_query.side_effect = _graph_error(404)
    created = MagicMock(name="created")
    drive.root.create_folder.return_value = created

    result = helper.get_or_create_folder(drive, "out")

    drive.root.create_folder.assert_called_once()
    created.execute_query_retry.assert_called_once()
    # the created folder — not its parent — must be returned
    assert result is created


def test_get_or_create_folder_reraises_non_404():
    drive = MagicMock()
    drive.root.get_by_path.return_value.get.return_value.execute_query.side_effect = (
        _graph_error(403)
    )
    with pytest.raises(ClientRequestException):
        helper.get_or_create_folder(drive, "out")


def test_on_retry_failure_swallows_then_reraises_on_final_attempt():
    exc = _graph_error(403)
    # non-final attempt only logs, so the retry loop keeps going
    helper._on_retry_failure(1, exc)
    # final attempt re-raises so a persistent failure is not silently swallowed
    with pytest.raises(ClientRequestException):
        helper._on_retry_failure(helper._UPLOAD_MAX_RETRY, exc)


def test_upload_file_simple_uploads_bytes(tmp_path):
    drive = MagicMock()
    folder = MagicMock()
    src = tmp_path / "src.json"
    src.write_bytes(b"hello")

    with patch.object(helper, "get_or_create_folder", return_value=folder) as goc:
        helper.upload_file(drive, src, "out", "json/doc.json")

    goc.assert_called_once_with(drive, "out/json")
    folder.upload.assert_called_once_with("doc.json", b"hello")
    folder.upload.return_value.execute_query_retry.assert_called_once()
    folder.resumable_upload.assert_not_called()


def test_upload_file_large_same_name_uses_resumable(tmp_path, monkeypatch):
    monkeypatch.setattr(helper, "_SIMPLE_UPLOAD_MAX_BYTES", 2)
    drive = MagicMock()
    folder = MagicMock()
    src = tmp_path / "doc.json"
    src.write_bytes(b"1234567")

    with patch.object(helper, "get_or_create_folder", return_value=folder):
        helper.upload_file(drive, src, None, "doc.json")

    folder.upload.assert_not_called()
    folder.resumable_upload.assert_called_once()
    (path_arg,), _ = folder.resumable_upload.call_args
    assert os.path.basename(path_arg) == "doc.json"


def test_upload_file_large_differing_name_copies_to_target_leaf(tmp_path, monkeypatch):
    monkeypatch.setattr(helper, "_SIMPLE_UPLOAD_MAX_BYTES", 2)
    drive = MagicMock()
    folder = MagicMock()
    src = tmp_path / "tmp_random.json"  # basename != target leaf
    src.write_bytes(b"1234567")

    captured = {}

    def _capture(path, chunk_size):
        captured["basename"] = os.path.basename(path)
        captured["content"] = open(path, "rb").read()
        return MagicMock()

    folder.resumable_upload.side_effect = _capture

    with patch.object(helper, "get_or_create_folder", return_value=folder):
        helper.upload_file(drive, src, "out", "json/doc.json")

    assert captured["basename"] == "doc.json"  # uploaded under the target leaf
    assert captured["content"] == b"1234567"  # copied bytes intact


@pytest.mark.parametrize(
    "obj, expected",
    [("hi", b"hi"), (b"hi", b"hi"), (bytearray(b"hi"), b"hi")],
    ids=["str", "bytes", "bytearray"],
)
def test_upload_object_normalizes_to_bytes(obj, expected):
    drive = MagicMock()
    folder = MagicMock()

    with patch.object(helper, "get_or_create_folder", return_value=folder) as goc:
        helper.upload_object(drive, obj, None, "doc.txt")

    goc.assert_called_once_with(drive, None)
    folder.upload.assert_called_once_with("doc.txt", expected)


def test_upload_object_reads_file_like():
    drive = MagicMock()
    folder = MagicMock()

    with patch.object(helper, "get_or_create_folder", return_value=folder):
        helper.upload_object(drive, BytesIO(b"data"), "out", "bin/x.bin")

    folder.upload.assert_called_once_with("x.bin", b"data")


def test_upload_object_large_uses_resumable(monkeypatch):
    monkeypatch.setattr(helper, "_SIMPLE_UPLOAD_MAX_BYTES", 2)
    drive = MagicMock()
    folder = MagicMock()
    captured = {}

    def _capture(path, chunk_size):
        captured["basename"] = os.path.basename(path)
        captured["content"] = open(path, "rb").read()
        return MagicMock()

    folder.resumable_upload.side_effect = _capture

    with patch.object(helper, "get_or_create_folder", return_value=folder):
        helper.upload_object(drive, b"1234567", "out", "json/big.json")

    folder.upload.assert_not_called()
    assert captured["basename"] == "big.json"
    assert captured["content"] == b"1234567"
