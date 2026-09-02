from __future__ import annotations

import logging
from io import BytesIO
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from box_sdk_gen import BoxClient

from docling_jobkit.connectors.box.models import BoxSource

_log = logging.getLogger(__name__)

# get_folder_items returns "mini" item entries (id/name/type/etag only) unless these
# are requested explicitly — without this, size/modified_at are silently missing.
_ITEM_FIELDS = ["id", "name", "type", "size", "modified_at"]
_PAGE_SIZE = 1000


def is_box_authentication_error(exc: BaseException) -> bool:
    from box_sdk_gen import BoxAPIError

    return isinstance(exc, BoxAPIError) and exc.response_info.status_code in (401, 403)


def is_box_unavailable_error(exc: BaseException) -> bool:
    from box_sdk_gen import BoxAPIError, RequestException

    if isinstance(exc, RequestException):
        return True
    if isinstance(exc, BoxAPIError):
        return exc.response_info.status_code in (429, 500, 502, 503, 504)
    return False


def get_client(config: BoxSource) -> BoxClient:
    """Build an authenticated Box client for either JWT or CCG auth."""
    from box_sdk_gen import BoxCCGAuth, BoxClient, BoxJWTAuth, CCGConfig, JWTConfig

    if config.auth_mode == "jwt":
        assert config.jwt_key_id is not None
        assert config.private_key is not None
        assert config.private_key_passphrase is not None
        jwt_config = JWTConfig(
            client_id=config.client_id,
            client_secret=config.client_secret.get_secret_value(),
            jwt_key_id=config.jwt_key_id,
            private_key=config.private_key.get_secret_value(),
            private_key_passphrase=config.private_key_passphrase.get_secret_value(),
            enterprise_id=config.enterprise_id,
            user_id=config.user_id,
        )
        return BoxClient(auth=BoxJWTAuth(config=jwt_config))

    ccg_config = CCGConfig(
        client_id=config.client_id,
        client_secret=config.client_secret.get_secret_value(),
        enterprise_id=config.enterprise_id,
        user_id=config.user_id,
    )
    return BoxClient(auth=BoxCCGAuth(config=ccg_config))


def check_connection(client: BoxClient) -> None:
    """Validate creds by making a single lightweight authenticated call."""
    client.users.get_user_me()


def _to_file_meta(item: Any) -> dict[str, Any]:
    return {
        "id": item.id,
        "name": item.name,
        "size": item.size or 0,
        "modified_at": item.modified_at,
    }


def _iter_folder_page(client: BoxClient, folder_id: str) -> Iterator[Any]:
    """Yield every entry of *folder_id* one server page at a time (offset paging)."""
    offset = 0
    while True:
        page = client.folders.get_folder_items(
            folder_id,
            fields=_ITEM_FIELDS,
            offset=offset,
            limit=_PAGE_SIZE,
        )
        entries = page.entries or []
        yield from entries
        if len(entries) < _PAGE_SIZE:
            return
        offset += _PAGE_SIZE


def list_folder_items(
    client: BoxClient, folder_id: str, *, limit: int | None = None
) -> Iterator[dict[str, Any]]:
    """Yield file metadata for every file under folder_id (recursively).

    Stops after *limit* files. The cap is honoured during the walk rather than by
    truncating afterwards, so a capped run never enumerates the whole tree.
    """
    from box_sdk_gen import FileFull, FolderMini

    pending = [folder_id]
    yielded = 0

    while pending:
        for item in _iter_folder_page(client, pending.pop()):
            if isinstance(item, FolderMini):
                pending.append(item.id)
                continue
            if not isinstance(item, FileFull):
                continue  # WebLink entries are not downloadable documents

            yield _to_file_meta(item)
            yielded += 1
            if limit is not None and yielded >= limit:
                return


def fetch_file_by_id(client: BoxClient, file_id: str) -> dict[str, Any]:
    """Fetch metadata for a single explicit file id (the file_ids override path)."""
    file_info = client.files.get_file_by_id(file_id, fields=_ITEM_FIELDS)
    return _to_file_meta(file_info)


def download_file(client: BoxClient, file_id: str) -> BytesIO:
    buffer = BytesIO()
    client.downloads.download_file_to_output_stream(file_id, buffer)
    buffer.seek(0)
    return buffer


__all__ = [
    "check_connection",
    "download_file",
    "fetch_file_by_id",
    "get_client",
    "is_box_authentication_error",
    "is_box_unavailable_error",
    "list_folder_items",
]
