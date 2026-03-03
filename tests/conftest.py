"""Shared test fixtures and utilities for docling-jobkit tests."""

import logging
from typing import Any

import pytest_asyncio
from aiohttp import web

from docling_jobkit.datamodel.callback import ProgressKind


def pytest_configure(config):
    """Configure logging for tests."""
    logging.getLogger("docling").setLevel(logging.INFO)


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
