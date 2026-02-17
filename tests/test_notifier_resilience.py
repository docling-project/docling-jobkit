import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.datamodel.task_targets import InBodyTarget
from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestrator,
    RQOrchestratorConfig,
    _TaskUpdate,
)


def _make_pubsub_message(task_id: str, status: TaskStatus) -> dict:
    update = _TaskUpdate(task_id=task_id, task_status=status)
    return {"type": "message", "data": update.model_dump_json()}


async def _fake_listen(messages):
    for msg in messages:
        yield msg


def _make_pubsub(messages):
    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock()
    pubsub.listen.return_value = _fake_listen(messages)
    return pubsub


def _make_orchestrator_with_task():
    config = RQOrchestratorConfig()
    with patch.object(RQOrchestrator, "__init__", lambda self, **kw: None):
        orch = object.__new__(RQOrchestrator)
    orch.config = config
    orch.tasks = {}
    orch.notifier = None
    orch._task_result_keys = {}
    orch._async_redis_conn = MagicMock()

    task = Task(
        task_id="test-task-1",
        sources=[],
        target=InBodyTarget(),
    )
    orch.tasks[task.task_id] = task
    return orch, task


@pytest.mark.asyncio
async def test_notifier_called_on_task_update():
    orch, task = _make_orchestrator_with_task()

    notifier = AsyncMock()
    orch.notifier = notifier

    messages = [_make_pubsub_message(task.task_id, TaskStatus.STARTED)]
    orch._async_redis_conn.pubsub.return_value = _make_pubsub(messages)

    await orch._listen_for_updates()

    notifier.notify_task_subscribers.assert_awaited_once_with(task.task_id)
    notifier.notify_queue_positions.assert_awaited_once()


@pytest.mark.asyncio
async def test_notifier_exception_does_not_kill_pubsub_loop(caplog):
    orch, task = _make_orchestrator_with_task()

    notifier = AsyncMock()
    notifier.notify_task_subscribers.side_effect = RuntimeError("ws disconnected")
    orch.notifier = notifier

    messages = [_make_pubsub_message(task.task_id, TaskStatus.STARTED)]
    orch._async_redis_conn.pubsub.return_value = _make_pubsub(messages)

    with caplog.at_level(logging.ERROR):
        await orch._listen_for_updates()

    assert "Notifier error for task test-task-1" in caplog.text
    assert "ws disconnected" in caplog.text
    assert task.task_status == TaskStatus.STARTED


@pytest.mark.asyncio
async def test_notifier_exception_does_not_prevent_subsequent_updates():
    orch, task = _make_orchestrator_with_task()

    task2 = Task(
        task_id="test-task-2",
        sources=[],
        target=InBodyTarget(),
    )
    orch.tasks[task2.task_id] = task2

    notifier = AsyncMock()
    notifier.notify_task_subscribers.side_effect = [
        RuntimeError("boom"),
        None,
    ]
    orch.notifier = notifier

    messages = [
        _make_pubsub_message(task.task_id, TaskStatus.STARTED),
        _make_pubsub_message(task2.task_id, TaskStatus.STARTED),
    ]
    orch._async_redis_conn.pubsub.return_value = _make_pubsub(messages)

    await orch._listen_for_updates()

    assert notifier.notify_task_subscribers.await_count == 2
    assert task.task_status == TaskStatus.STARTED
    assert task2.task_status == TaskStatus.STARTED


@pytest.mark.asyncio
async def test_listen_without_notifier():
    orch, task = _make_orchestrator_with_task()

    messages = [_make_pubsub_message(task.task_id, TaskStatus.STARTED)]
    orch._async_redis_conn.pubsub.return_value = _make_pubsub(messages)

    await orch._listen_for_updates()

    assert task.task_status == TaskStatus.STARTED
