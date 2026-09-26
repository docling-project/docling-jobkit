import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import redis

from docling.datamodel.service.targets import InBodyTarget

from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestrator,
    RQOrchestratorConfig,
    _TaskUpdate,
)


def _update(task_id: str, status: TaskStatus) -> dict:
    update = _TaskUpdate(task_id=task_id, task_status=status)
    return {"type": "message", "data": update.model_dump_json()}


def _pubsub(feed=(), subscribe_error=None):
    """A pub/sub whose get_message() returns `feed` item by item.

    Items that are exceptions are raised. The feed ends with a cancellation,
    which is how the listener is stopped in production.
    """
    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock(side_effect=subscribe_error)
    pubsub.aclose = AsyncMock()
    pubsub.get_message = AsyncMock(side_effect=[*feed, asyncio.CancelledError()])
    return pubsub


def _client(pubsub):
    client = MagicMock()
    client.pubsub.return_value = pubsub
    client.aclose = AsyncMock()
    return client


def _orchestrator(*pubsubs, config=None):
    config = config or RQOrchestratorConfig()
    with patch.object(RQOrchestrator, "__init__", lambda self, **kw: None):
        orch = object.__new__(RQOrchestrator)
    orch.config = config
    orch.tasks = {}
    orch.notifier = None
    orch._task_result_keys = {}
    orch._store_task_in_redis = AsyncMock()
    clients = [_client(p) for p in pubsubs]
    orch._build_pubsub_redis = MagicMock(side_effect=clients)
    task = Task(task_id="task-1", sources=[], target=InBodyTarget())
    orch.tasks[task.task_id] = task
    return orch, task, clients


@pytest.fixture
def no_sleep():
    with patch(
        "docling_jobkit.orchestrators.rq.orchestrator.asyncio.sleep",
        new_callable=AsyncMock,
    ) as sleep:
        yield sleep


def _dropped(message="Connection closed by server."):
    return _pubsub([redis.exceptions.ConnectionError(message)])


@pytest.mark.asyncio
async def test_resubscribes_on_a_new_client_after_connection_drop(no_sleep):
    orch, task, clients = _orchestrator(
        _dropped(), _pubsub([_update("task-1", TaskStatus.STARTED)])
    )

    await orch._listen_for_updates()

    assert task.task_status == TaskStatus.STARTED
    clients[0].aclose.assert_awaited_once()  # broken client discarded
    clients[1].pubsub.return_value.subscribe.assert_awaited_once_with(
        orch.config.sub_channel
    )
    no_sleep.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_retries_when_redis_is_down_at_subscribe(no_sleep):
    orch, task, _ = _orchestrator(
        _pubsub(subscribe_error=redis.exceptions.ConnectionError("refused")),
        _pubsub([_update("task-1", TaskStatus.SUCCESS)]),
    )

    await orch._listen_for_updates()

    assert task.task_status == TaskStatus.SUCCESS


@pytest.mark.asyncio
async def test_idle_polls_do_not_resubscribe(no_sleep):
    feed = [None, None, None, _update("task-1", TaskStatus.STARTED)]
    orch, task, _ = _orchestrator(_pubsub(feed))

    await orch._listen_for_updates()

    assert task.task_status == TaskStatus.STARTED
    assert orch._build_pubsub_redis.call_count == 1
    no_sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_backoff_doubles_is_capped_and_resets_after_subscribe(no_sleep):
    refused = [
        _pubsub(subscribe_error=redis.exceptions.ConnectionError("refused"))
        for _ in range(7)
    ]
    orch, _, _ = _orchestrator(*refused, _dropped(), _pubsub())

    await orch._listen_for_updates()

    delays = [call.args[0] for call in no_sleep.await_args_list]
    # seven failed subscribes, then a successful one that later drops
    assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 1.0]


@pytest.mark.asyncio
async def test_a_bad_message_is_skipped(no_sleep):
    feed = [
        {"type": "message", "data": b"not a task update"},
        _update("task-1", TaskStatus.STARTED),
    ]
    orch, task, _ = _orchestrator(_pubsub(feed))

    await orch._listen_for_updates()

    assert task.task_status == TaskStatus.STARTED
    assert orch._build_pubsub_redis.call_count == 1


@pytest.mark.asyncio
async def test_cancel_during_backoff_stops_the_listener():
    orch, _, _ = _orchestrator(_dropped(), _pubsub())

    listener = asyncio.create_task(orch._listen_for_updates())
    await asyncio.sleep(0.1)  # listener is now waiting out the 1 s backoff
    listener.cancel()
    await listener  # returns quietly, as on a normal shutdown

    assert orch._build_pubsub_redis.call_count == 1


@pytest.mark.asyncio
async def test_subscription_client_has_no_read_timeout():
    # The command pool's socket_timeout would turn every idle gap between
    # updates into a read timeout on the subscriber.
    config = RQOrchestratorConfig(
        redis_socket_timeout=5.0, redis_socket_connect_timeout=2.0
    )
    with patch.object(RQOrchestrator, "__init__", lambda self, **kw: None):
        orch = object.__new__(RQOrchestrator)
    orch.config = config

    client = orch._build_pubsub_redis()

    kwargs = client.connection_pool.connection_kwargs
    assert kwargs["socket_timeout"] is None
    assert kwargs["socket_connect_timeout"] == 2.0
    assert kwargs["socket_keepalive"] is True
    assert kwargs["health_check_interval"] > 0
    await client.aclose()
