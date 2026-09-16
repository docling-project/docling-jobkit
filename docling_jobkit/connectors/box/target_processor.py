from io import BytesIO
from pathlib import Path
from typing import BinaryIO

from pydantic import BaseModel

from docling_jobkit.connectors.box.helper import (
    check_connection,
    get_client,
    is_box_authentication_error,
    resolve_target_folder,
    upload_document,
)
from docling_jobkit.connectors.box.models import BoxTarget
from docling_jobkit.connectors.errors import map_connector_authentication_errors
from docling_jobkit.connectors.target_processor import BaseTargetProcessor

# Target-mode: only auth failures are translated here; everything else (conflict
# resolution, policy, transport) is already classified inside helper.py's
# upload_document, which raises TargetWriteError directly for anything
# unclassified rather than letting it escape as an opaque internal error.
_map_box_target_errors = map_connector_authentication_errors(
    "Box", is_box_authentication_error
)


def _as_seekable_stream(obj: str | bytes | BinaryIO) -> tuple[BinaryIO, int]:
    """Normalize *obj* into a seekable stream plus its size in bytes.

    Box needs the size up front (for both the simple and chunked-session
    upload paths) and needs to re-``seek(0)`` the stream if the first upload
    attempt fails with a name conflict and must be retried as a version
    upload.
    """
    if isinstance(obj, str):
        obj = obj.encode("utf-8")
    if isinstance(obj, (bytes, bytearray)):
        return BytesIO(obj), len(obj)
    if obj.seekable():
        obj.seek(0, 2)
        size = obj.tell()
        obj.seek(0)
        return obj, size
    data = obj.read()
    return BytesIO(data), len(data)


class BoxTargetProcessor(BaseTargetProcessor):
    def __init__(self, coords: BoxTarget) -> None:
        super().__init__()
        self._coords = coords
        # Subfolder ids already resolved during this open/close cycle, keyed by
        # relative path (e.g. "json"). Reused across upload_file/upload_object
        # calls so a batch walks each subfolder only once.
        self._folder_cache: dict[str, str] = {}

    @classmethod
    def check_dependencies(cls) -> None:
        import box_sdk_gen  # noqa: F401

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (BoxTarget,)

    @_map_box_target_errors
    def _initialize(self) -> None:
        self._folder_cache.clear()
        self._client = get_client(self._coords)
        check_connection(self._client)

    def _finalize(self) -> None:
        self._folder_cache.clear()

    @_map_box_target_errors
    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        """Upload a local file from disk into the configured Box folder."""
        folder_id, name = resolve_target_folder(
            self._client, self._coords.folder_id, target_filename, self._folder_cache
        )
        size = Path(filename).stat().st_size
        with open(filename, "rb") as fh:
            upload_document(self._client, folder_id, name, fh, size)

    @_map_box_target_errors
    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
    ) -> None:
        """Upload an in-memory object (bytes or file-like) into Box."""
        folder_id, name = resolve_target_folder(
            self._client, self._coords.folder_id, target_filename, self._folder_cache
        )
        stream, size = _as_seekable_stream(obj)
        upload_document(self._client, folder_id, name, stream, size)
