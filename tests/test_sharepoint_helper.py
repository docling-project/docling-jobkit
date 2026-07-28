from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from office365.runtime.client_request_exception import ClientRequestException
from pydantic import SecretStr
from requests import ConnectionError as RequestsConnectionError, Response, Timeout

from docling_jobkit.connectors.errors import SourceConnectorPolicyError
from docling_jobkit.connectors.sharepoint import helper
from docling_jobkit.datamodel.sharepoint_coords import SharePointCoordinates


@pytest.fixture
def sp_coords() -> SharePointCoordinates:
    """SharePoint target (site_url)."""
    return SharePointCoordinates(
        tenant="tenant-guid",
        client_id="client-id",
        client_secret=SecretStr("secret"),
        site_url="https://contoso.sharepoint.com/sites/Marketing",
    )


@pytest.fixture
def od_coords() -> SharePointCoordinates:
    """OneDrive target (onedrive_user)."""
    return SharePointCoordinates(
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
