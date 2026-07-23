"""Resilience tests for the Ray orchestrator pub/sub subscriber.

Regression coverage for the WebSocket-push outage where an idle-read
``socket_timeout`` (or a single malformed message) killed the listener with no
resubscribe, so terminal task updates were published to a channel nobody was
subscribed to. ``subscribe_to_updates`` must instead treat idle gaps as normal,
skip malformed messages, and reconnect with backoff.
"""

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# models/redis_helper (imported below) pull in ray transitively; skip cleanly
# when ray is absent, and never run Ray tests in CI (CI does not provision Ray).
pytest.importorskip("ray")
if os.getenv("CI"):
    pytest.skip("Skipping Ray tests in CI", allow_module_level=True)

from redis.exceptions import ConnectionError as RedisConnectionError

from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.ray.models import TaskUpdate
from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager


def _message(task_id: str, status: TaskStatus) -> dict:
    update = TaskUpdate(task_id=task_id, task_status=status)
    return {"type": "message", "data": update.model_dump_json()}


def _make_pubsub(script):
    """Build a fake pubsub whose ``get_message`` replays ``script``.

    Each scripted item is either a message dict, ``None`` (idle poll), or an
    Exception instance to raise. The script is exhausted by raising
    ``CancelledError`` so a misbehaving test can never hang the forever-loop.
    """
    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock()
    pubsub.aclose = AsyncMock()
    items = iter(script)

    async def get_message(ignore_subscribe_messages=False, timeout=None):
        try:
            item = next(items)
        except StopIteration as exc:
            raise asyncio.CancelledError() from exc
        if isinstance(item, BaseException):
            raise item
        return item

    pubsub.get_message = get_message
    return pubsub


def _make_client(pubsub) -> MagicMock:
    client = MagicMock()
    client.pubsub.return_value = pubsub
    client.aclose = AsyncMock()
    return client


def _manager() -> RedisStateManager:
    # __init__ stores params only; no connection is opened.
    return RedisStateManager(redis_url="redis://localhost:6379/0")


async def _collect(mgr: RedisStateManager, expected: int) -> list:
    results: list = []
    gen = mgr.subscribe_to_updates()
    try:
        async for update in gen:
            results.append(update)
            if len(results) >= expected:
                break
    finally:
        await gen.aclose()
    return results


@pytest.mark.asyncio
async def test_skips_malformed_message_and_keeps_going():
    mgr = _manager()
    pubsub = _make_pubsub(
        [
            {"type": "message", "data": "{not valid json"},
            _message("task-1", TaskStatus.SUCCESS),
        ]
    )
    with patch.object(mgr, "_build_pubsub_redis", return_value=_make_client(pubsub)):
        results = await _collect(mgr, expected=1)

    assert [u.task_id for u in results] == ["task-1"]
    assert results[0].task_status == TaskStatus.SUCCESS


@pytest.mark.asyncio
async def test_idle_poll_does_not_end_subscription():
    mgr = _manager()
    # ``None`` is an idle poll window (no message); must not be fatal.
    pubsub = _make_pubsub([None, None, _message("task-2", TaskStatus.SUCCESS)])
    with patch.object(mgr, "_build_pubsub_redis", return_value=_make_client(pubsub)):
        results = await _collect(mgr, expected=1)

    assert [u.task_id for u in results] == ["task-2"]


@pytest.mark.asyncio
async def test_reconnects_after_connection_drop():
    mgr = _manager()
    # First connection dies on read; second delivers the update.
    pubsub_dead = _make_pubsub([RedisConnectionError("Timeout reading from redis")])
    pubsub_ok = _make_pubsub([_message("task-3", TaskStatus.SUCCESS)])
    build = MagicMock(side_effect=[_make_client(pubsub_dead), _make_client(pubsub_ok)])

    with (
        patch.object(mgr, "_build_pubsub_redis", build),
        patch("asyncio.sleep", new=AsyncMock()),
    ):
        results = await _collect(mgr, expected=1)

    assert [u.task_id for u in results] == ["task-3"]
    assert build.call_count == 2  # rebuilt the client after the drop
    pubsub_dead.subscribe.assert_awaited_once()
    pubsub_ok.subscribe.assert_awaited_once()
