"""Tests for error_message propagation through _TaskUpdate -> Task."""

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


def _make_pubsub_message(
    task_id: str,
    status: TaskStatus,
    error_message: str | None = None,
    result_key: str | None = None,
) -> dict:
    update = _TaskUpdate(
        task_id=task_id,
        task_status=status,
        error_message=error_message,
        result_key=result_key,
    )
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


class TestTaskUpdateErrorMessage:
    def test_task_update_with_error_message(self):
        update = _TaskUpdate(
            task_id="t1",
            task_status=TaskStatus.FAILURE,
            error_message="conversion failed: corrupt PDF",
        )
        assert update.error_message == "conversion failed: corrupt PDF"

    def test_task_update_without_error_message(self):
        update = _TaskUpdate(
            task_id="t1",
            task_status=TaskStatus.SUCCESS,
        )
        assert update.error_message is None

    def test_task_update_serialization_roundtrip(self):
        update = _TaskUpdate(
            task_id="t1",
            task_status=TaskStatus.FAILURE,
            error_message="OOM killed",
        )
        json_str = update.model_dump_json()
        restored = _TaskUpdate.model_validate_json(json_str)
        assert restored.error_message == "OOM killed"
        assert restored.task_status == TaskStatus.FAILURE

    def test_task_update_backward_compatible(self):
        json_without_error = '{"task_id": "t1", "task_status": "failure"}'
        update = _TaskUpdate.model_validate_json(json_without_error)
        assert update.error_message is None
        assert update.task_status == TaskStatus.FAILURE


class TestTaskErrorMessage:
    def test_task_has_error_message_field(self):
        task = Task(task_id="t1", sources=[], target=InBodyTarget())
        assert task.error_message is None

    def test_task_with_error_message(self):
        task = Task(
            task_id="t1",
            sources=[],
            target=InBodyTarget(),
            error_message="something broke",
        )
        assert task.error_message == "something broke"

    def test_task_error_message_serialization(self):
        task = Task(
            task_id="t1",
            sources=[],
            target=InBodyTarget(),
            error_message="timeout after 300s",
        )
        data = task.model_dump(mode="json", serialize_as_any=True)
        assert data["error_message"] == "timeout after 300s"


class TestListenForUpdatesErrorPropagation:
    @pytest.mark.asyncio
    async def test_failure_with_error_message_sets_task_error(self):
        orch, task = _make_orchestrator_with_task()

        messages = [
            _make_pubsub_message(
                task.task_id,
                TaskStatus.FAILURE,
                error_message="RuntimeError: No converter",
            )
        ]
        orch._async_redis_conn.pubsub.return_value = _make_pubsub(messages)

        await orch._listen_for_updates()

        assert task.task_status == TaskStatus.FAILURE
        assert task.error_message == "RuntimeError: No converter"

    @pytest.mark.asyncio
    async def test_failure_without_error_message_leaves_none(self):
        orch, task = _make_orchestrator_with_task()

        messages = [_make_pubsub_message(task.task_id, TaskStatus.FAILURE)]
        orch._async_redis_conn.pubsub.return_value = _make_pubsub(messages)

        await orch._listen_for_updates()

        assert task.task_status == TaskStatus.FAILURE
        assert task.error_message is None

    @pytest.mark.asyncio
    async def test_success_does_not_set_error_message(self):
        orch, task = _make_orchestrator_with_task()

        messages = [
            _make_pubsub_message(
                task.task_id,
                TaskStatus.SUCCESS,
                result_key="docling:results:test-task-1",
            )
        ]
        orch._async_redis_conn.pubsub.return_value = _make_pubsub(messages)

        await orch._listen_for_updates()

        assert task.task_status == TaskStatus.SUCCESS
        assert task.error_message is None

    @pytest.mark.asyncio
    async def test_started_then_failure_preserves_error(self):
        orch, task = _make_orchestrator_with_task()

        messages = [
            _make_pubsub_message(task.task_id, TaskStatus.STARTED),
            _make_pubsub_message(
                task.task_id,
                TaskStatus.FAILURE,
                error_message="GPU OOM",
            ),
        ]
        orch._async_redis_conn.pubsub.return_value = _make_pubsub(messages)

        await orch._listen_for_updates()

        assert task.task_status == TaskStatus.FAILURE
        assert task.error_message == "GPU OOM"
