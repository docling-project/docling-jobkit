from __future__ import annotations

import logging
from io import BytesIO
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from box_sdk_gen import BoxClient

from docling_jobkit.connectors.box.models import BoxSource
from docling_jobkit.connectors.errors import SourceConnectorPolicyError

_log = logging.getLogger(__name__)

# get_folder_items returns "mini" item entries (id/name/type/etag only) unless these
# are requested explicitly — without this, size/modified_at are silently missing.
_ITEM_FIELDS = ["id", "name", "type", "size", "modified_at"]

# 1000 is the documented maximum for get_folder_items' `limit`
# Box's own default is 100.
_PAGE_SIZE = 1000

# Terminal statuses that describe the *request*, not the credentials and not an
# outage (429/5xx). 404 is the one that matters in practice: an unresolvable
# folder_id or file_ids entry.
_POLICY_STATUS = (400, 404, 405, 409, 413, 415, 422)

# OAuth2 failures come back from POST /oauth2/token as HTTP 400 with the real cause
# in the body's `error`, not as a 401. Observed live: a wrong enterprise_id/user_id
# gives 'invalid_grant' / "Grant credentials are invalid", and an app that has not
# been authorized in the Admin Console gives 'invalid_grant' / "App is not yet
# authorized for use". Without this, a credentials problem falls into _POLICY_STATUS
# and is reported to the operator as "verify 'folder_id'", pointing at the wrong
# thing entirely.
_AUTH_ERROR_CODES = frozenset(
    {"invalid_grant", "invalid_client", "unauthorized_client", "access_denied"}
)


def _oauth_error_code(exc: BaseException) -> str | None:
    """The OAuth2 `error` code in a Box error body, when there is one."""
    from box_sdk_gen import BoxAPIError

    if not isinstance(exc, BoxAPIError):
        return None
    body = exc.response_info.body or {}
    code = body.get("error") if isinstance(body, dict) else None
    return code if isinstance(code, str) else None


def is_box_authentication_error(exc: BaseException) -> bool:
    from box_sdk_gen import BoxAPIError

    if not isinstance(exc, BoxAPIError):
        return False
    if exc.response_info.status_code in (401, 403):
        return True
    return _oauth_error_code(exc) in _AUTH_ERROR_CODES


def is_box_policy_error(exc: BaseException) -> bool:
    from box_sdk_gen import BoxAPIError

    if not isinstance(exc, BoxAPIError):
        return False
    if _oauth_error_code(exc) in _AUTH_ERROR_CODES:
        return False  # credentials, not a bad request — see is_box_authentication_error
    return exc.response_info.status_code in _POLICY_STATUS


def is_box_unavailable_error(exc: BaseException) -> bool:
    from box_sdk_gen import BoxAPIError, BoxSDKError, RequestException

    # BoxAPIError first: it subclasses BoxSDKError, and only its status decides.
    if isinstance(exc, BoxAPIError):
        return exc.response_info.status_code in (429, 500, 502, 503, 504)

    # A drop *during* the response body. `iter_content` is consumed outside the
    # SDK's own try/except, so requests' exception surfaces unwrapped here.
    if isinstance(exc, RequestException):
        return True

    # Connect / DNS / read-timeout failures never reach us as RequestException:
    # BoxNetworkClient catches them around the request call and re-raises a bare
    # BoxSDKError carrying the original in `.error`. Matching on that wrapped cause
    # keeps this narrow — the SDK's other BoxSDKError sites (upload retries of a
    # non-seekable stream, browser-environment guards) are not transport failures
    # and must stay non-retryable. Without this branch the single most common
    # transient failure is reported to the client as a permanent internal error.
    if isinstance(exc, BoxSDKError):
        return isinstance(getattr(exc, "error", None), RequestException)

    return False


def get_client(config: BoxSource) -> BoxClient:
    """Build an authenticated Box client for either JWT or CCG auth."""
    from box_sdk_gen import BoxCCGAuth, BoxClient, BoxJWTAuth, CCGConfig, JWTConfig

    if config.auth_mode == "jwt":
        if (
            config.jwt_key_id is None
            or config.private_key is None
            or config.private_key_passphrase is None
        ):  # pragma: no cover - guaranteed by BoxSource._validate_auth
            raise SourceConnectorPolicyError(
                "Box JWT authentication requires 'jwt_key_id', 'private_key' and "
                "'private_key_passphrase'.",
                source_kind="box",
            )
        _require_jwt_dependencies()
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


def _require_jwt_dependencies() -> None:
    """Fail legibly when the JWT auth path's optional dependencies are absent.

    ``check_dependencies`` deliberately probes only ``box_sdk_gen``: CCG auth works
    without PyJWT/cryptography, so probing them there would unregister the whole
    connector for CCG-only installs. They are pulled in by the ``box`` extra via
    ``boxsdk[jwt]``; a hand-pinned bare ``boxsdk`` would otherwise surface the SDK's
    ImportError as an opaque "internal error" only once a task is already running.
    """
    try:
        import jwt  # noqa: F401
    except ImportError as exc:
        raise SourceConnectorPolicyError(
            "Box JWT authentication requires the PyJWT and cryptography packages; "
            "install the 'box' extra (which pulls boxsdk[jwt]).",
            source_kind="box",
        ) from exc


def check_connection(client: BoxClient) -> None:
    """Validate creds by making a single lightweight authenticated call."""
    client.users.get_user_me()


def _to_file_meta(item: Any) -> dict[str, Any]:
    if item.size is None:
        # `size` is Optional in the SDK schema even when requested through `fields`.
        # Without it max_file_size cannot be enforced before the download, so the
        # item is refused rather than silently treated as zero-length — but as a
        # classified, client-readable failure instead of a raw ValidationError out
        # of BoxFileIdentifier, which would surface as "internal error".
        raise SourceConnectorPolicyError(
            f"Box item {item.id!r} ({item.name!r}) was returned without a 'size' "
            "attribute and cannot be size-checked before download.",
            source_kind="box",
        )
    return {
        "id": item.id,
        "name": item.name,
        "size": item.size,
        "modified_at": item.modified_at,
    }


def _iter_folder_page(client: BoxClient, folder_id: str) -> Iterator[Any]:
    """Yield every entry of *folder_id* one server page at a time (marker paging).

    Marker rather than offset: Box documents offset paging as "not guaranteed to
    work reliably for high offset values and may fail for large datasets", which is
    exactly the shape of a recursive tree walk, and an offset walk over a folder
    being written to concurrently silently skips or repeats entries. ``next_marker``
    also ends the listing exactly, instead of inferring the end from a short page —
    a page shorter than the requested limit is not a documented end-of-list signal.
    """
    marker: str | None = None
    while True:
        page = client.folders.get_folder_items(
            folder_id,
            fields=_ITEM_FIELDS,
            usemarker=True,
            marker=marker,
            limit=_PAGE_SIZE,
        )
        yield from page.entries or []

        marker = page.next_marker
        if not marker:
            return


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
                # WebLink entries are not downloadable documents.
                _log.debug("Skipping non-file Box entry %s (%s)", item.id, type(item))
                continue

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
    "is_box_policy_error",
    "is_box_unavailable_error",
    "list_folder_items",
]
