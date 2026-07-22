import logging
import time
from pathlib import Path
from typing import BinaryIO, Optional

import httpx
from pydantic import BaseModel

from docling.datamodel.service.targets import PutTarget

from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.public_errors import TargetWriteError

_log = logging.getLogger(__name__)


class HttpPutTargetProcessor(BaseTargetProcessor):
    """Target processor that PUTs file/object bytes to a remote HTTP URL."""

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (PutTarget,)

    @classmethod
    def result_mode(cls):
        return "archive"

    def __init__(
        self, target: PutTarget, *, max_retries: int = 3, retry_delay: float = 1.0
    ):
        super().__init__()
        self._target = target
        self._max_retries = max_retries
        self._retry_delay = retry_delay

    def _initialize(self) -> None:
        """No persistent client state required."""

    def _finalize(self) -> None:
        """No cleanup required."""

    def upload_file(
        self,
        filename: str | Path,
        target_filename: str,
        content_type: str,
    ) -> None:
        """Read *filename* from disk and PUT its contents to the target URL."""
        url = str(self._target.url)
        last_exc: Optional[Exception] = None
        for attempt in range(self._max_retries):
            try:
                with Path(filename).open("rb") as file_data:
                    r = httpx.put(url, files={"file": file_data})
                    r.raise_for_status()
                return
            except Exception as exc:
                last_exc = exc
                _log.warning(
                    "PUT to %s failed (attempt %d/%d): %s",
                    url,
                    attempt + 1,
                    self._max_retries,
                    exc,
                )
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_delay * (attempt + 1))
        raise TargetWriteError(
            f"Failed to upload file to target url after {self._max_retries} attempts."
        ) from last_exc

    def upload_object(
        self,
        obj: str | bytes | BinaryIO,
        target_filename: str,
        content_type: str,
    ) -> None:
        """PUT *obj* bytes/file-like to the target URL as multipart form data."""
        if isinstance(obj, str):
            data: bytes = obj.encode()
        elif isinstance(obj, (bytes, bytearray)):
            data = bytes(obj)
        else:
            data = obj.read()

        url = str(self._target.url)
        last_exc: Optional[Exception] = None
        for attempt in range(self._max_retries):
            try:
                r = httpx.put(
                    url, files={"file": (target_filename, data, content_type)}
                )
                r.raise_for_status()
                return
            except Exception as exc:
                last_exc = exc
                _log.warning(
                    "PUT to %s failed (attempt %d/%d): %s",
                    url,
                    attempt + 1,
                    self._max_retries,
                    exc,
                )
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_delay * (attempt + 1))
        raise TargetWriteError(
            f"Failed to upload object to target url after {self._max_retries} attempts."
        ) from last_exc
