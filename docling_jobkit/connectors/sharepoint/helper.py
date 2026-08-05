from __future__ import annotations

import logging
import os
import shutil
import tempfile
from io import BytesIO
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, BinaryIO, Iterator

if TYPE_CHECKING:
    from office365.graph_client import GraphClient
    from office365.onedrive.driveitems.driveItem import DriveItem
    from office365.onedrive.drives.drive import Drive

from docling_jobkit.datamodel.sharepoint_coords import SharePointConnection

_log = logging.getLogger(__name__)
_DEFAULT_PAGE_SIZE = 200  # NOTE: should we allow the user to toggle this?

# for upload
_SIMPLE_UPLOAD_MAX_BYTES = 4 * 1024 * 1024  # simple upload for office365 caps at 4 MB
_RESUMABLE_CHUNK_BYTES = 4 * 1024 * 1024  # each chunk should also be 4 MB each


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


def get_client(coords: SharePointConnection) -> GraphClient:
    """build the sharepoint connection client with the client creds"""
    from office365.graph_client import GraphClient

    return GraphClient(tenant=coords.tenant).with_client_secret(
        coords.client_id, coords.client_secret.get_secret_value()
    )


def resolve_drive(client: GraphClient, coords: SharePointConnection) -> Drive:
    """Resolve the coordinates to a single Graph drive.

    site_url -> a SharePoint document library (either default or user supplied)
    onedrive_user -> user's OneDrive for Business
    """
    from docling_jobkit.connectors.errors import SourceConnectorPolicyError

    # OneDrive for Business: /users/{upn}/drive — there is no /me under app-only auth.
    if not coords.site_url:
        assert coords.onedrive_user is not None  # validated by SharePointConnection
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


def resolve_destination(
    base_folder: str | None, target_filename: str
) -> tuple[str | None, str]:
    """Split target_filename into into (destination_folder, leaf), prefixing base_folder

    e.g. base_folder='out', target_filename='json/doc.json' -> ('out/json', 'doc.json').
    """
    target = PurePosixPath(target_filename)
    parts: list[str] = []

    if base_folder:
        parts.append(base_folder.strip("/"))
    if str(target.parent) != ".":
        parts.append(str(target.parent))

    return "/".join(p for p in parts if p) or None, target.name


def get_or_create_folder(drive: Drive, folder_path: str | None) -> DriveItem:
    """Return the destination folder for uploads, creating any missing path segments."""
    from office365.onedrive.driveitems.conflict_behavior import ConflictBehavior
    from office365.runtime.client_request_exception import ClientRequestException

    folder = drive.root
    if not folder_path:
        return folder

    for part in PurePosixPath(folder_path.strip("/")).parts:
        # return the child folder name under parent and create if absent
        child = folder.get_by_path(part)
        try:
            folder = child.get().execute_query()
        except ClientRequestException as exc:
            if exc.response is not None and exc.response.status_code == 404:
                folder = folder.create_folder(
                    part,
                    conflict_behavior=ConflictBehavior.Fail,  # type: ignore[arg-type]
                ).execute_query_retry()
            else:
                raise

    return folder


def upload_file(
    drive: Drive,
    local_path: str | os.PathLike,
    base_folder: str | None,
    target_filename: str,
) -> None:
    """Upload the local file at local_path to base_folder/target_filename"""
    dest_folder, name = resolve_destination(base_folder, target_filename)
    folder = get_or_create_folder(drive, dest_folder)
    if os.path.getsize(local_path) <= _SIMPLE_UPLOAD_MAX_BYTES:
        with open(local_path, "rb") as fh:
            folder.upload(name, fh.read()).execute_query_retry()
        return

    # resumable_upload names the item after the source basename; when the target
    # leaf differs, upload from a correctly-named temp copy.
    if os.path.basename(os.fspath(local_path)) == name:
        folder.resumable_upload(
            os.fspath(local_path), chunk_size=_RESUMABLE_CHUNK_BYTES
        ).execute_query_retry()
        return

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = os.path.join(tmp, name)
        shutil.copyfile(local_path, tmp_path)
        folder.resumable_upload(
            tmp_path, chunk_size=_RESUMABLE_CHUNK_BYTES
        ).execute_query_retry()


def upload_object(
    drive: Drive,
    obj: str | bytes | BinaryIO,
    base_folder: str | None,
    target_filename: str,
) -> None:
    """Upload in-memory object to base_folder/target_filename"""
    if isinstance(obj, (bytes, bytearray)):
        content = bytes(obj)
    elif isinstance(obj, str):
        content = obj.encode()
    else:
        data = obj.read()
        content = data.encode() if isinstance(data, str) else data

    dest_folder, name = resolve_destination(base_folder, target_filename)
    folder = get_or_create_folder(drive, dest_folder)

    if len(content) <= _SIMPLE_UPLOAD_MAX_BYTES:
        folder.upload(name, content).execute_query_retry()
        return

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = os.path.join(tmp, name)
        with open(tmp_path, "wb") as fh:
            fh.write(content)
        folder.resumable_upload(
            tmp_path, chunk_size=_RESUMABLE_CHUNK_BYTES
        ).execute_query_retry()
