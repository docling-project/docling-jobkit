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
_DEFAULT_PAGE_SIZE = 200

# for upload
_SIMPLE_UPLOAD_MAX_BYTES = 4 * 1024 * 1024  # simple upload for office365 caps at 4 MB
_RESUMABLE_CHUNK_BYTES = 4 * 1024 * 1024  # each chunk should also be 4 MB each
_UPLOAD_MAX_RETRY = 3  # execute_query_retry attempts for folder/upload calls
_UPLOAD_RETRY_DELAY_S = 3

# Statuses where replaying the identical request cannot change the outcome. 429
# (throttled) and 423 (locked) are deliberately absent — those are what retrying is
# for. Failing fast here keeps a bad credential from burning _UPLOAD_MAX_RETRY *
# _UPLOAD_RETRY_DELAY_S seconds on every single artifact upload, and lets the caller
# handle a 409 folder-creation race while it is still cheap.
_NON_RETRYABLE_STATUS = frozenset({400, 401, 403, 404, 409})


class SharePointDriveNotFoundError(LookupError):
    """The coordinates do not resolve to a reachable Graph drive.

    Deliberately connector-neutral: ``resolve_drive`` is shared by the source and the
    target processor, so each one translates this into the error family core expects
    (``SourceConnectorPolicyError`` vs ``TargetConnectorConfigError``) instead of the
    helper picking one and misattributing the other.
    """


def _status_of(exc: BaseException) -> int | None:
    """HTTP status behind a Graph error, or None if *exc* is not one."""
    from office365.runtime.client_request_exception import ClientRequestException

    if isinstance(exc, ClientRequestException) and exc.response is not None:
        return exc.response.status_code
    return None


def _on_retry_failure(attempt: int, exc: Exception) -> None:
    """failure_callback for execute_query_retry.

    Logs each failed attempt and re-raises when retrying is pointless — either
    because the status is terminal or because the attempts are exhausted.
    ``execute_query_retry`` neither re-raises nor signals a status on exhaustion, so
    without this a persistent failure would be silently swallowed and reported as a
    successful upload.
    """
    status = _status_of(exc)
    if status in _NON_RETRYABLE_STATUS:
        _log.debug("SharePoint request failed with terminal status %s: %s", status, exc)
        raise exc

    if attempt >= _UPLOAD_MAX_RETRY:
        _log.error("SharePoint request failed after %d attempts: %s", attempt, exc)
        raise exc

    _log.warning(
        "SharePoint request failed (attempt %d/%d); retrying in %ds: %s",
        attempt,
        _UPLOAD_MAX_RETRY,
        _UPLOAD_RETRY_DELAY_S,
        exc,
    )


def _execute_with_retry(client_object: "DriveItem") -> None:
    """Run pending queries on client_object with bounded retry + logging."""
    client_object.execute_query_retry(
        max_retry=_UPLOAD_MAX_RETRY,
        timeout_secs=_UPLOAD_RETRY_DELAY_S,
        failure_callback=_on_retry_failure,
    )


def is_sharepoint_authentication_error(exc: BaseException) -> bool:
    return _status_of(exc) in (401, 403)


def is_sharepoint_unavailable_error(exc: BaseException) -> bool:
    from requests import ConnectionError, Timeout

    if isinstance(exc, (ConnectionError, Timeout)):
        return True
    status = _status_of(exc)
    return status is not None and status >= 500


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
    # OneDrive for Business: /users/{upn}/drive — there is no /me under app-only auth.
    if not coords.site_url:
        assert coords.onedrive_user is not None  # validated by SharePointConnection
        return client.users.get_by_principal_name(coords.onedrive_user).drive

    site = client.sites.get_by_url(coords.site_url).get().execute_query()
    if not coords.document_library:
        return site.drive

    drives = site.drives.get_all(page_size=_DEFAULT_PAGE_SIZE).execute_query()
    for drive in drives:
        if drive.name == coords.document_library:
            return drive

    raise SharePointDriveNotFoundError(
        f"Document library '{coords.document_library}' not found on site "
        f"'{coords.site_url}'."
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


def _iter_children(folder: "DriveItem") -> Iterator[Any]:
    """Yield the children of *folder* one server page at a time.

    ``get_files(recursive=True)`` and ``get_all()`` both drain every page inside a
    single ``execute_query()`` and accumulate the whole result set before the caller
    sees anything. Iterating a collection in *paged* mode instead fetches page N+1
    only when page N has been consumed, so abandoning the loop early stops the
    enumeration instead of paying for the rest of the library.
    """
    collection = folder.children.paged(_DEFAULT_PAGE_SIZE)
    collection.get().execute_query()
    yield from collection


def list_folder_items(
    drive: Drive, folder_path: str | None, *, limit: int | None = None
) -> Iterator[dict[str, Any]]:
    """Yield file metadata for every file under folder_path (recursively).

    Stops after *limit* files. The cap is honoured during the walk rather than by
    truncating afterwards, so a capped run never enumerates the whole library.
    """
    # Resolved to its id first: ``children`` off a path-addressed handle builds
    # ``/root:/Reports:://children`` (note the doubled slash), whereas an id-addressed
    # item gives the clean ``/items/{id}/children``. Items yielded by a listing are
    # already id-addressed, so only the starting folder needs this.
    start = (
        drive.root.get_by_path(folder_path.strip("/")).get().execute_query()
        if folder_path
        else drive.root
    )
    pending = [start]
    yielded = 0

    while pending:
        for item in _iter_children(pending.pop()):
            if item.is_folder:
                pending.append(item)
                continue

            yield _to_file_meta(item)
            yielded += 1
            if limit is not None and yielded >= limit:
                return


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


def folder_handle(drive: Drive, folder_path: str | None) -> DriveItem:
    """A path-addressed handle for *folder_path*. Costs no request.

    Always addressed from the drive root, and deliberately **not** resolved with
    ``get()``. Resolving mutates an item's resource path from path-addressed
    (``/root:/out/json:/``) to id-addressed (``/items/{id}``), and every child address
    built from it afterwards becomes ``/items/{id}:/name:/…`` — which Graph rejects
    with a 400, "Resource not found for the segment '{id}:'". Handing back an
    unresolved handle keeps uploads on the canonical
    ``/root:/out/json/doc.json:/content`` form.

    Because building one is free, callers should make a fresh handle per upload rather
    than holding one: ``DriveItem.upload()`` appends to the handle's ``children``
    collection, so a shared handle accumulates an entry per uploaded artifact for as
    long as it is alive.
    """
    if not folder_path:
        return drive.root
    return drive.root.get_by_path(folder_path.strip("/"))


def get_or_create_folder(drive: Drive, folder_path: str | None) -> DriveItem:
    """Ensure every segment of *folder_path* exists; return a handle to the leaf.

    Each segment is looked up as a full path *from the drive root*, never by chaining
    ``get_by_path`` onto the previously resolved item — see :func:`folder_handle` for
    why that breaks. The lookups resolve to id-addressed items, which is what creation
    needs (``/items/{id}/children`` is the correct POST target), but the handle handed
    back to callers is a fresh unresolved one.

    Note the failure mode this avoids is silent on a first run: the malformed request
    404s, which reads as "folder missing", so the tree is created and everything looks
    fine until a later run finds the parent already there and gets a 400 instead.
    """
    from office365.runtime.client_request_exception import ClientRequestException

    if not folder_path:
        return drive.root

    parent = drive.root
    parts = PurePosixPath(folder_path.strip("/")).parts
    for index, part in enumerate(parts):
        lookup = drive.root.get_by_path("/".join(parts[: index + 1]))
        try:
            parent = lookup.get().execute_query()
        except ClientRequestException as exc:
            if _status_of(exc) != 404:
                raise
            _log.debug("Creating missing folder segment %r", part)
            parent = _create_folder(parent, lookup, part)

    return folder_handle(drive, folder_path)


def _create_folder(parent: "DriveItem", child: "DriveItem", part: str) -> "DriveItem":
    """Create the *part* segment under *parent*, tolerating a concurrent creator.

    Every worker exporting into the same destination races to create the same folder
    tree, so the check-then-create above is inherently lossy: the loser gets a 409 and
    must adopt the winner's folder rather than fail the upload. ``Fail`` (rather than
    ``Replace``) is kept so an existing folder's contents are never clobbered.
    """
    from office365.onedrive.driveitems.conflict_behavior import ConflictBehavior
    from office365.runtime.client_request_exception import ClientRequestException

    created = parent.create_folder(
        part,
        conflict_behavior=ConflictBehavior.Fail,  # type: ignore[arg-type]
    )
    try:
        _execute_with_retry(created)
    except ClientRequestException as exc:
        if _status_of(exc) != 409:
            raise
        _log.debug("Folder segment %r created concurrently; adopting it", part)
        return child.get().execute_query()

    return created


def _stage_as(src: str | os.PathLike, dst: str) -> None:
    """Materialize *src* at *dst* as cheaply as the filesystem allows.

    A hard link costs nothing; copying is the fallback for a cross-device temp dir and
    can mean duplicating hundreds of megabytes (the parquet export writes 500 MB
    files), so it is worth trying the link first.
    """
    try:
        os.link(src, dst)
    except OSError:
        shutil.copyfile(src, dst)


def _drain_into(fh: BinaryIO, head: bytes, rest: BinaryIO | None) -> None:
    """Write *head* then the remainder of *rest* to *fh* in bounded steps."""
    fh.write(head)
    if rest is None:
        return

    while True:
        block = rest.read(_RESUMABLE_CHUNK_BYTES)
        if not block:
            return
        fh.write(block.encode() if isinstance(block, str) else block)


def upload_file(
    folder: "DriveItem", local_path: str | os.PathLike, target_name: str
) -> None:
    """Upload the local file at local_path into the already-resolved *folder*."""
    size = os.path.getsize(local_path)

    if size <= _SIMPLE_UPLOAD_MAX_BYTES:
        _log.debug("Uploading %r (%d bytes) [simple]", target_name, size)
        with open(local_path, "rb") as fh:
            _execute_with_retry(folder.upload(target_name, fh.read()))
        return

    # resumable_upload streams the file off disk in chunks but names the item after
    # the source basename; when the target leaf differs, upload from a staged copy
    # carrying the right name.
    if os.path.basename(os.fspath(local_path)) == target_name:
        _log.debug("Uploading %r (%d bytes) [resumable]", target_name, size)
        _execute_with_retry(
            folder.resumable_upload(
                os.fspath(local_path), chunk_size=_RESUMABLE_CHUNK_BYTES
            )
        )
        return

    _log.info("Uploading %r (%d bytes) [resumable, staged]", target_name, size)
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = os.path.join(tmp, target_name)
        _stage_as(local_path, tmp_path)
        _execute_with_retry(
            folder.resumable_upload(tmp_path, chunk_size=_RESUMABLE_CHUNK_BYTES)
        )


def upload_object(
    folder: "DriveItem", obj: str | bytes | BinaryIO, target_name: str
) -> None:
    """Upload an in-memory object into the already-resolved *folder*."""
    if isinstance(obj, (bytes, bytearray)):
        head, rest = bytes(obj), None
    elif isinstance(obj, str):
        head, rest = obj.encode(), None
    else:
        # Read only up to the simple-upload cap. Anything larger is spilled to disk
        # and streamed back from there, so a big file-like is never materialized in
        # full on top of the copy the caller already holds.
        chunk = obj.read(_SIMPLE_UPLOAD_MAX_BYTES + 1)
        head = chunk.encode() if isinstance(chunk, str) else chunk
        rest = obj if len(head) > _SIMPLE_UPLOAD_MAX_BYTES else None

    if rest is None and len(head) <= _SIMPLE_UPLOAD_MAX_BYTES:
        _execute_with_retry(folder.upload(target_name, head))
        return

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = os.path.join(tmp, target_name)
        with open(tmp_path, "wb") as fh:
            _drain_into(fh, head, rest)
        del head  # released before the staged copy is streamed back off disk
        _execute_with_retry(
            folder.resumable_upload(tmp_path, chunk_size=_RESUMABLE_CHUNK_BYTES)
        )
