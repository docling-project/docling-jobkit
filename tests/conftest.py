"""Shared test fixtures and utilities for docling-jobkit tests."""

import logging
from typing import Any, Callable

import pytest
import pytest_asyncio
from aiohttp import web
from pydantic import SecretStr

from docling_jobkit.datamodel.callback import ProgressKind
from docling_jobkit.datamodel.sharepoint_coords import (
    SharePointConnection,
    SharePointSourceCoordinates,
    SharePointTargetCoordinates,
)


def pytest_configure(config):
    """Configure logging for tests."""
    logging.getLogger("docling").setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# SharePoint / OneDrive
#
# The coords models are pure pydantic, but ``graph_error`` needs the office365 SDK,
# so that import stays inside the factory — importing it here would break collection
# of the whole suite for anyone without the ``sharepoint`` extra.
# ---------------------------------------------------------------------------

_SP_CREDS = {"tenant": "tenant-guid", "client_id": "client-id"}


@pytest.fixture
def graph_error() -> Callable[[int], Exception]:
    """Build a ClientRequestException carrying *status*, bypassing its __init__."""
    from office365.runtime.client_request_exception import ClientRequestException
    from requests import Response

    def _make(status: int) -> ClientRequestException:
        exc = ClientRequestException.__new__(ClientRequestException)
        response = Response()
        response.status_code = status
        exc.response = response
        return exc

    return _make


@pytest.fixture
def sp_connection() -> SharePointConnection:
    """Connection coords pointing at a SharePoint site."""
    return SharePointConnection(
        **_SP_CREDS,
        client_secret=SecretStr("secret"),
        site_url="https://contoso.sharepoint.com/sites/Marketing",
    )


@pytest.fixture
def od_connection() -> SharePointConnection:
    """Connection coords pointing at a user's OneDrive for Business."""
    return SharePointConnection(
        **_SP_CREDS,
        client_secret=SecretStr("secret"),
        onedrive_user="alice@contoso.com",
    )


@pytest.fixture
def sp_source_coords() -> SharePointSourceCoordinates:
    return SharePointSourceCoordinates(
        **_SP_CREDS,
        client_secret=SecretStr("secret"),
        site_url="https://contoso.sharepoint.com/sites/Marketing",
    )


@pytest.fixture
def sp_target_coords() -> SharePointTargetCoordinates:
    return SharePointTargetCoordinates(
        **_SP_CREDS,
        client_secret=SecretStr("secret"),
        site_url="https://contoso.sharepoint.com/sites/Marketing",
        folder_path="out",
    )


class CallbackServer:
    """Mock HTTP server to capture callback invocations."""

    def __init__(self):
        self.callbacks: list[dict[str, Any]] = []
        self.app = web.Application()
        self.app.router.add_post("/callback", self.handle_callback)
        self.runner = None
        self.site = None

    async def handle_callback(self, request: web.Request) -> web.Response:
        """Handle incoming callback requests."""
        data = await request.json()
        self.callbacks.append(data)
        logging.info(f"Received callback: {data.get('progress', {}).get('kind')}")
        return web.Response(status=200)

    async def start(self, port: int = 8765):
        """Start the callback server."""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "localhost", port)
        await self.site.start()

    async def stop(self):
        """Stop the callback server."""
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()

    def get_callbacks_by_kind(self, kind: ProgressKind) -> list[dict[str, Any]]:
        """Get all callbacks of a specific kind."""
        return [
            cb for cb in self.callbacks if cb.get("progress", {}).get("kind") == kind
        ]


@pytest_asyncio.fixture
async def callback_server():
    """Fixture to provide a mock callback server."""
    server = CallbackServer()
    await server.start()
    yield server
    await server.stop()


@pytest_asyncio.fixture
async def callback_server_rq():
    """Fixture to provide a mock callback server for RQ tests (different port)."""
    server = CallbackServer()
    await server.start(port=8766)
    yield server
    await server.stop()
