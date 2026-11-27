"""HTTP sources must refuse private addresses on every path that contacts them.

The converter fetches URL sources through docling-core, which refuses private
addresses unless ``DOCLINGCORE_ALLOWED_PRIVATE_IPS`` allows them. These tests
cover the two places where jobkit contacts the URL itself: the HEAD probe of
``HttpSourceProcessor`` and the download in ``fetch_http_source_bytes_async``.
"""

import asyncio
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from docling.datamodel.service.sources import HttpSource
from docling_core.utils.settings import settings as core_settings

from docling_jobkit.connectors.http.source_processor import HttpSourceProcessor
from docling_jobkit.convert.materialization import (
    MaterializationLimitExceededError,
    SourceFetchError,
    SourceLimitExceededError,
    fetch_http_source_bytes_async,
)

_BODY = b"%PDF-loopback"
# Link-local metadata address: never allowed, and refused before any connection.
_PRIVATE_REDIRECT_TARGET = "http://169.254.169.254/doc.pdf"


class _LoopbackServer:
    def __init__(self) -> None:
        self.seen: list[tuple[str, str]] = []
        seen = self.seen

        class Handler(BaseHTTPRequestHandler):
            def _respond(self, method: str) -> None:
                seen.append((method, self.path))
                if self.path == "/redirect.pdf":
                    self.send_response(302)
                    self.send_header("Location", _PRIVATE_REDIRECT_TARGET)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(_BODY)))
                self.send_header("ETag", '"loopback"')
                self.end_headers()
                if method == "GET":
                    self.wfile.write(_BODY)

            def do_HEAD(self) -> None:
                self._respond("HEAD")

            def do_GET(self) -> None:
                self._respond("GET")

            def log_message(self, *args) -> None:
                pass

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def url(self, path: str) -> str:
        return f"http://127.0.0.1:{self._server.server_port}{path}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()


@pytest.fixture
def loopback_server():
    server = _LoopbackServer()
    yield server
    server.close()


@pytest.fixture
def allow_loopback(monkeypatch):
    monkeypatch.setattr(core_settings, "allowed_private_ips", ["127.0.0.1"])


def _fetch(url: str, **kwargs) -> bytes:
    return asyncio.run(
        fetch_http_source_bytes_async(
            HttpSource(url=url), **{"max_file_size": None, **kwargs}
        )
    )


def _head(url: str) -> tuple[int | None, str | None]:
    source = HttpSource(url=url)
    return HttpSourceProcessor(source)._try_head_request(source)


def test_fetch_refuses_private_address(loopback_server):
    with pytest.raises(SourceFetchError, match="not allowed"):
        _fetch(loopback_server.url("/doc.pdf"))

    assert loopback_server.seen == []


def test_fetch_reads_allowlisted_private_address(loopback_server, allow_loopback):
    assert _fetch(loopback_server.url("/doc.pdf")) == _BODY


def test_fetch_refuses_redirect_to_private_address(loopback_server, allow_loopback):
    with pytest.raises(SourceFetchError, match="not allowed"):
        _fetch(loopback_server.url("/redirect.pdf"))


@pytest.mark.parametrize(
    "error_cls", [SourceLimitExceededError, MaterializationLimitExceededError]
)
def test_fetch_enforces_max_file_size(loopback_server, allow_loopback, error_cls):
    with pytest.raises(error_cls, match="exceeds max_file_size=4 bytes"):
        _fetch(loopback_server.url("/doc.pdf"), max_file_size=4, error_cls=error_cls)


def test_head_probe_refuses_private_address(loopback_server):
    assert _head(loopback_server.url("/doc.pdf")) == (None, None)
    assert loopback_server.seen == []


def test_head_probe_reads_allowlisted_private_address(loopback_server, allow_loopback):
    assert _head(loopback_server.url("/doc.pdf")) == (len(_BODY), '"loopback"')


def test_head_probe_does_not_follow_redirects(loopback_server, allow_loopback):
    assert _head(loopback_server.url("/redirect.pdf")) == (None, None)
    assert loopback_server.seen == [("HEAD", "/redirect.pdf")]
