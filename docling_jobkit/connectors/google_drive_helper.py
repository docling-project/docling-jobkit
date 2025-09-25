import json
import logging
from io import BytesIO
from os import PathLike
from pathlib import Path
from typing import BinaryIO, Iterable, List, Optional, TypedDict, Union

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload

from docling_jobkit.datamodel.google_drive_coords import GoogleDriveCoordinates


class FileInfoType(TypedDict):
    id: str
    name: str
    mimeType: str
    path: str


def _get_client_info(coords):
    if coords.credentials:
        return (
            coords.credentials.client_id,
            coords.credentials.client_secret.get_secret_value(),
            str(coords.credentials.token_uri),
        )
    if coords.credentials_path:
        data = json.loads(Path(coords.credentials_path).read_text())
        conf = data.get("installed")
        return conf["client_id"], conf["client_secret"], conf["token_uri"]
    raise ValueError("Missing client credentials")


def get_service(coords: GoogleDriveCoordinates) -> Resource:
    """
    Return an authorized Google Drive service (googleapiclient.discovery.Resource). Build the service using, in priority:
      1) An already-stored token file,
      2) An already-generated refresh token,
      3) The local OAuth flow.
    """
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    creds = None

    # 1) Stored token
    if coords.token_path and Path(coords.token_path).exists():
        creds = Credentials.from_authorized_user_file(coords.token_path, SCOPES)
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())

    # 2) Refresh token
    if (not creds or not creds.valid) and coords.refresh_token:
        try:
            client_id, client_secret, token_uri = _get_client_info(coords)
            creds = Credentials(
                token=None,
                refresh_token=coords.refresh_token,
                token_uri=token_uri,
                client_id=client_id,
                client_secret=client_secret,
                scopes=SCOPES,
            )
            creds.refresh(Request())
        except Exception as e:
            logging.warning("Refresh failed: %s", e)
            creds = None

    # 3) OAuth flow
    if not creds or not creds.valid:
        if coords.credentials:
            cfg = coords.credentials.model_dump(mode="json", exclude_none=True)
            cfg["client_secret"] = coords.credentials.client_secret.get_secret_value()
            flow = InstalledAppFlow.from_client_config({"installed": cfg}, SCOPES)
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                coords.credentials_path, SCOPES
            )

        creds = flow.run_local_server(port=0)

    # Save token
    if coords.token_path:
        Path(coords.token_path).write_text(creds.to_json())

    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _yield_children(service: Resource, folder_id: str):
    """Yield direct children of a folder."""

    query = f"'{folder_id}' in parents and trashed = false"
    fields = (
        "nextPageToken, "
        "files(id, name, mimeType, parents, shortcutDetails(targetId, targetMimeType))"
    )
    page_token = None
    while True:
        resp = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                fields=fields,
                pageToken=page_token,
                pageSize=1000,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )
        for f in resp.get("files", []):
            yield f
        page_token = resp.get("nextPageToken")
        if not page_token:
            break


def _yield_files_infos(
    service: Resource,
    coords: GoogleDriveCoordinates,
) -> Iterable[FileInfoType]:
    """
    Depth-first traversal of Google Drive.
    Yields dicts: {id, name, mimeType, path}
    """

    root_meta = (
        service.files()
        .get(
            fileId=coords.path_id,
            fields="id, name, mimeType",
            supportsAllDrives=True,
        )
        .execute()
    )

    info: FileInfoType
    if not (root_meta.get("mimeType") == "application/vnd.google-apps.folder"):
        info = {
            "id": root_meta["id"],
            "name": root_meta["name"],
            "mimeType": root_meta["mimeType"],
            "path": root_meta["name"],
        }
        yield info
        return

    stack = [(coords.path_id, root_meta["name"])]
    while stack:
        cur_id, cur_path = stack.pop()
        for item in _yield_children(service, cur_id):
            path = f"{cur_path}/{item['name']}"
            if item["mimeType"] == "application/vnd.google-apps.folder":
                stack.append((item["id"], path))
            else:
                info = {
                    "id": item["id"],
                    "name": item["name"],
                    "mimeType": item["mimeType"],
                    "path": path,
                }
                yield info


def get_source_files_infos(
    service: Resource,
    coords: GoogleDriveCoordinates,
) -> List[FileInfoType]:
    return list(_yield_files_infos(service, coords))


def download_file(
    service: Resource,
    file_info: FileInfoType,
    file_stream: BytesIO,
) -> None:
    """
    Download a file from Google Drive.
    The file can be any file stored in Google Drive as well as a document created with Google Slides, Google Sheets or Google Docs.
    """

    EXPORT_MAP = {
        "application/vnd.google-apps.document": (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        ),
        "application/vnd.google-apps.spreadsheet": (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        "application/vnd.google-apps.presentation": (
            "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        ),
    }
    if file_info["mimeType"].startswith("application/vnd.google-apps."):
        export_mime = EXPORT_MAP.get(file_info["mimeType"])
        request = service.files().export_media(
            fileId=file_info["id"], mimeType=export_mime
        )
    else:
        request = service.files().get_media(
            fileId=file_info["id"], supportsAllDrives=True
        )

    downloader = MediaIoBaseDownload(file_stream, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            logging.info("Downloading: %d%%", int(status.progress() * 100))


def upload_file(
    service: Resource,
    target_filename: str,
    content_type: str,
    coords: GoogleDriveCoordinates,
    filename: Optional[Union[str, PathLike[str]]] = None,
    file_stream: Optional[BinaryIO] = None,
) -> None:
    """
    Upload a file to Google Drive.
    """

    meta = (
        service.files()
        .get(
            fileId=coords.path_id,
            fields="id, name, mimeType, capabilities/canAddChildren, "
            "shortcutDetails/targetId, shortcutDetails/targetMimeType",
            supportsAllDrives=True,
        )
        .execute()
    )
    if meta.get("mimeType") != "application/vnd.google-apps.folder":
        logging.error(
            f"Expected a Google Drive folder for path_id, but got {meta.get('mimeType')}."
        )
        exit(0)

    chunk_size: int = 8 * 1024 * 1024
    if file_stream is not None:
        media = MediaIoBaseUpload(
            file_stream, mimetype=content_type, chunksize=chunk_size, resumable=True
        )
    else:
        media = MediaFileUpload(
            filename, mimetype=content_type, chunksize=chunk_size, resumable=True
        )

    metadata = {"name": target_filename, "parents": [coords.path_id]}
    request = service.files().create(
        body=metadata,
        media_body=media,
        fields="id, name, mimeType, parents, webViewLink",
        supportsAllDrives=True,
    )

    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            prog = getattr(status, "resumable_progress", None)
            if prog is not None:
                logging.info("uploading %s: %d bytes sent", filename, prog)
            else:
                logging.info("uploading %s...", filename)

    logging.info("uploaded %s (%s)", response.get("name"), response.get("id"))
