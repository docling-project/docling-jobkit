from __future__ import annotations

import logging
from io import BytesIO
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from office365.graph_client import GraphClient
    from office365.onedrive.drives.drive import Drive

from docling_jobkit.datamodel.sharepoint_coords import SharePointCoordinates

_log = logging.getLogger(__name__)
_DEFAULT_PAGE_SIZE = 200  # NOTE: should we allow the user to toggle this?


def is_sharepoint_authentication_error(exc: BaseException) -> bool:
    from office365.runtime.client_request_exception import ClientRequestException

    return (
        isinstance(exc, ClientRequestException)
        and exc.response is not None
        and exc.response.status_code in (401, 403)
    )


def is_sharepoint_unavailable_error(exc: BaseException) -> bool:
    from office365.runtime.client_request_exception import ClientRequestException
    from requests import ConnectionError, Timeout

    if isinstance(exc, (ConnectionError, Timeout)):
        return True
    return (
        isinstance(exc, ClientRequestException)
        and exc.response is not None
        and exc.response.status_code >= 500
    )


def get_client(coords: SharePointCoordinates) -> GraphClient:
    """build the sharepoint connection client with the client creds"""
    from office365.graph_client import GraphClient

    return GraphClient(tenant=coords.tenant).with_client_secret(
        coords.client_id, coords.client_secret.get_secret_value()
    )


def resolve_drive(client: GraphClient, coords: SharePointCoordinates) -> Drive:
    """Resolve the coordinates to a single Graph drive.

    site_url -> a SharePoint document library (either default or user supplied)
    onedrive_user -> user's OneDrive for Business
    """
    from docling_jobkit.connectors.errors import SourceConnectorPolicyError

    # OneDrive for Business: /users/{upn}/drive — there is no /me under app-only auth.
    if not coords.site_url:
        return client.users.get_by_principal_name(coords.onedrive_user).drive

    site = (
        client.sites.get_by_url(coords.site_url).get().execute_query()
    )  # maybe retry + exp backoff?
    if not coords.document_library:
        return site.drive

    drives = site.drives.get_all(page_size=_DEFAULT_PAGE_SIZE).execute_query()
    for drive in drives:
        if drive.name == coords.document_library:
            return drive

    raise SourceConnectorPolicyError(
        f"Document library '{coords.document_library}' not found on site "
        f"'{coords.site_url}'.",
        source_kind="sharepoint",
    )


def check_connection(client: GraphClient, drive: Drive) -> None:
    """Validate creds and target by loading the resolved drive"""
    drive.get().execute_query()


def _to_file_meta(item: Any) -> dict[str, Any]:
    return {
        "id": item.id,
        "name": item.name,
        "size": int(item.get_property("size", 0) or 0),
        "last_modified": item.last_modified_datetime,
    }


def list_folder_items(
    client: GraphClient, drive: Drive, folder_path: str | None
) -> Iterator[dict[str, Any]]:
    """Yield file metadata for every file under folder_path (recursively)"""
    root = drive.root
    folder = root.get_by_path(folder_path) if folder_path else root
    files = folder.get_files(recursive=True, page_size=_DEFAULT_PAGE_SIZE)

    client.execute_query()
    for item in files:
        yield _to_file_meta(item)


def list_items_by_id(
    client: GraphClient, drive: Drive, file_ids: list[str]
) -> Iterator[dict[str, Any]]:
    """Yield file metadata for explicit item ids"""
    items = [drive.items[file_id] for file_id in file_ids]
    for item in items:
        item.get()

    client.execute_query()
    for item in items:
        if item.is_folder:
            _log.warning("Item %s is a folder, skipping", item.id)
            continue

        yield _to_file_meta(item)


def download_item(client: GraphClient, drive: Drive, item_id: str) -> BytesIO:
    buffer = BytesIO()
    drive.items[item_id].download(buffer)

    client.execute_query()
    buffer.seek(0)

    return buffer
