import asyncio
import datetime
import json
import logging
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from rq.exceptions import NoSuchJobError
from rq.job import JobStatus

from docling.datamodel.service.targets import InBodyTarget

from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators._redis_gate import RedisCallerGate
from docling_jobkit.orchestrators.base_orchestrator import (
    RedisBackpressureError,
    TaskNotFoundError,
)
from docling_jobkit.orchestrators.rq.orchestrator import (
    _RQ_JOB_GONE,
    RQOrchestrator,
    RQOrchestratorConfig,
    _RQJobGone,
    _TaskUpdate,
)


def _make_task(
    task_id: str = "test-task-1",
    status: TaskStatus = TaskStatus.SUCCESS,
    error_message: str | None = None,
    finished_at: datetime.datetime | None = None,
) -> Task:
    task = Task(
        task_id=task_id,
        task_type="convert",
        task_status=status,
        target=InBodyTarget(),
        processing_meta={
            "num_docs": 0,
            "num_processed": 0,
            "num_succeeded": 0,
            "num_failed": 0,
        },
    )
    if error_message:
        task.error_message = error_message
    if finished_at:
        task.finished_at = finished_at
    return task


class _FakePipeline:
    """Stand-in for the async Redis pipeline used by the metadata guard."""

    def __init__(self, redis_client: "_FakeAsyncRedis") -> None:
        self._redis = redis_client
        self.watched: list[str] = []
        self.commands: list[tuple[str, str]] = []
        self.multi_called = False

    async def __aenter__(self) -> "_FakePipeline":
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None

    async def watch(self, key: str) -> None:
        self.watched.append(key)

    async def get(self, key: str) -> bytes | None:
        value = self._redis.stored.get(key)
        return None if value is None else value.encode()

    def multi(self) -> None:
        self.multi_called = True

    def set(self, key: str, value: str) -> None:
        self.commands.append((key, value))

    async def execute(self) -> list[bool]:
        for key, value in self.commands:
            self._redis.stored[key] = value
        self._redis.pipeline_executions += 1
        return [True] * len(self.commands)


class _FakeAsyncRedis:
    """In-memory stand-in for the async Redis client."""

    def __init__(self) -> None:
        self.stored: dict[str, str] = {}
        self.set_calls: list[dict[str, Any]] = []
        self.pipeline_executions = 0

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        self.set_calls.append({"key": key, "value": value, "ex": ex})
        self.stored[key] = value
        return True

    async def get(self, key: str) -> bytes | None:
        value = self.stored.get(key)
        return None if value is None else value.encode()

    def pipeline(self, *args: Any, **kwargs: Any) -> _FakePipeline:
        return _FakePipeline(self)


def _make_orchestrator(
    config: RQOrchestratorConfig | None = None,
) -> RQOrchestrator:
    config = config or RQOrchestratorConfig()
    with patch.object(RQOrchestrator, "__init__", lambda self, **kw: None):
        orch = object.__new__(RQOrchestrator)
    orch.config = config
    orch.allowed_target_kinds = config.allowed_target_kinds
    orch.tasks = {}
    orch.notifier = None
    orch._task_result_keys = {}
    orch._async_redis_conn = _FakeAsyncRedis()
    orch._redis_conn = MagicMock()
    orch._rq_queue = MagicMock()
    orch._redis_gate = RedisCallerGate(config.redis_gate_concurrency or 1)
    orch._rq_job_function = "docling_jobkit.orchestrators.rq.worker.docling_task"
    return orch


async def _fake_listen(messages):
    for msg in messages:
        yield msg


def _make_pubsub(messages):
    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock()
    pubsub.listen.return_value = _fake_listen(messages)
    return pubsub


def _make_pubsub_message(task_id: str, status: TaskStatus) -> dict:
    update = _TaskUpdate(task_id=task_id, task_status=status)
    return {"type": "message", "data": update.model_dump_json()}


class TestRQDurableStatus:
    def test_redis_gate_concurrency_defaults_from_pool_size(self):
        config = RQOrchestratorConfig(redis_max_connections=25)
        assert config.redis_gate_concurrency == 15

    @pytest.mark.asyncio
    async def test_returns_sentinel_on_no_such_job_error(self):
        orch = _make_orchestrator()
        with patch.object(
            orch,
            "_refresh_task_from_rq",
            AsyncMock(side_effect=NoSuchJobError("missing")),
        ):
            result = await orch._get_task_from_rq_direct("missing-job")
        assert isinstance(result, _RQJobGone)

    @pytest.mark.asyncio
    async def test_returns_none_on_generic_rq_exception(self):
        orch = _make_orchestrator()
        with patch.object(
            orch,
            "_refresh_task_from_rq",
            AsyncMock(side_effect=RuntimeError("redis down")),
        ):
            result = await orch._get_task_from_rq_direct("some-task")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_task_when_rq_has_job(self):
        orch = _make_orchestrator()
        expected_task = _make_task("rq-task", TaskStatus.SUCCESS)

        async def update_with_success(task_id: str) -> None:
            orch.tasks[task_id] = expected_task

        with patch.object(orch, "_refresh_task_from_rq", update_with_success):
            result = await orch._get_task_from_rq_direct("rq-task")

        assert result is expected_task

    @pytest.mark.asyncio
    async def test_terminal_redis_state_beats_stale_rq_state(self):
        orch = _make_orchestrator()
        redis_task = _make_task("t1", TaskStatus.FAILURE)

        with (
            patch.object(
                orch, "_get_task_from_redis", AsyncMock(return_value=redis_task)
            ),
            patch(
                "docling_jobkit.orchestrators.rq.orchestrator.Job.exists",
                return_value=True,
            ),
        ):
            result = await orch.task_status("t1")

        assert result.task_status == TaskStatus.FAILURE
        assert orch.tasks["t1"] is redis_task

    @pytest.mark.asyncio
    async def test_rq_gone_redis_success_cleans_up(self):
        orch = _make_orchestrator()
        cached_task = _make_task("t1", TaskStatus.SUCCESS)
        orch.tasks["t1"] = cached_task
        orch._task_result_keys["t1"] = "some-key"

        with (
            patch.object(
                orch, "_get_task_from_redis", AsyncMock(return_value=cached_task)
            ),
            patch(
                "docling_jobkit.orchestrators.rq.orchestrator.Job.exists",
                return_value=False,
            ),
        ):
            result = await orch.task_status("t1")

        assert result.task_status == TaskStatus.SUCCESS
        assert "t1" not in orch.tasks
        assert "t1" not in orch._task_result_keys

    @pytest.mark.asyncio
    async def test_rq_gone_redis_pending_marks_failure(self):
        orch = _make_orchestrator()
        cached_task = _make_task("t2", TaskStatus.PENDING)
        orch.tasks["t2"] = cached_task

        with (
            patch.object(
                orch, "_get_task_from_rq_direct", AsyncMock(return_value=_RQ_JOB_GONE)
            ),
            patch.object(
                orch, "_get_task_from_redis", AsyncMock(return_value=cached_task)
            ),
            patch.object(orch, "_store_task_in_redis", AsyncMock()) as mock_store,
        ):
            result = await orch.task_status("t2")

        assert result.task_status == TaskStatus.FAILURE
        assert result.error_message is not None
        assert "orphaned" in result.error_message.lower()
        assert "t2" not in orch.tasks
        mock_store.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_rq_gone_no_redis_raises_not_found(self):
        orch = _make_orchestrator()

        with (
            patch.object(
                orch, "_get_task_from_rq_direct", AsyncMock(return_value=_RQ_JOB_GONE)
            ),
            patch.object(orch, "_get_task_from_redis", AsyncMock(return_value=None)),
        ):
            with pytest.raises(TaskNotFoundError):
                await orch.task_status("ghost-task")

    @pytest.mark.asyncio
    async def test_rq_has_task_returns_directly(self):
        orch = _make_orchestrator()
        rq_task = _make_task("t5", TaskStatus.SUCCESS)

        with (
            patch.object(orch, "_get_task_from_redis", AsyncMock(return_value=None)),
            patch.object(
                orch, "_get_task_from_rq_direct", AsyncMock(return_value=rq_task)
            ),
            patch.object(orch, "_store_task_in_redis", AsyncMock()) as mock_store,
        ):
            result = await orch.task_status("t5")

        assert result is rq_task
        assert orch.tasks["t5"] is rq_task
        mock_store.assert_awaited_once_with(rq_task)

    @pytest.mark.asyncio
    async def test_reaps_old_completed_tasks(self):
        config = RQOrchestratorConfig(
            zombie_reaper_interval=0.01,
            zombie_reaper_max_age=3600.0,
        )
        orch = _make_orchestrator(config)
        old_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            hours=2
        )
        old_task = _make_task("old-1", TaskStatus.SUCCESS, finished_at=old_time)
        orch.tasks["old-1"] = old_task
        orch._task_result_keys["old-1"] = "key-1"

        reaper = asyncio.create_task(orch._reap_zombie_tasks())
        await asyncio.sleep(0.05)
        reaper.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reaper

        assert "old-1" not in orch.tasks
        assert "old-1" not in orch._task_result_keys

    @pytest.mark.asyncio
    async def test_store_task_uses_results_ttl(self):
        config = RQOrchestratorConfig(results_ttl=14400)
        orch = _make_orchestrator(config)
        task = _make_task("ttl-task", TaskStatus.SUCCESS)

        await orch._store_task_in_redis(task)

        assert len(orch._async_redis_conn.set_calls) == 1
        assert orch._async_redis_conn.set_calls[0]["ex"] == 14400

    @pytest.mark.asyncio
    async def test_store_active_task_has_no_expiry(self):
        orch = _make_orchestrator()
        task = _make_task("active-task", TaskStatus.PENDING)

        await orch._store_task_in_redis(task)

        assert orch._async_redis_conn.set_calls == []
        assert orch._async_redis_conn.pipeline_executions == 1
        stored = orch._async_redis_conn.stored["docling:tasks:active-task:metadata"]
        assert json.loads(stored)["task_status"] == "pending"

    @pytest.mark.asyncio
    async def test_store_active_task_keeps_terminal_metadata(self):
        orch = _make_orchestrator()
        key = "docling:tasks:terminal-task:metadata"
        orch._async_redis_conn.stored[key] = json.dumps(
            {"task_id": "terminal-task", "task_status": "success"}
        )

        await orch._store_task_in_redis(_make_task("terminal-task", TaskStatus.PENDING))

        assert (
            json.loads(orch._async_redis_conn.stored[key])["task_status"] == "success"
        )
        assert orch._async_redis_conn.pipeline_executions == 0

    @pytest.mark.asyncio
    async def test_store_active_task_skips_malformed_metadata(self, caplog):
        orch = _make_orchestrator()
        key = "docling:tasks:broken-task:metadata"
        orch._async_redis_conn.stored[key] = "not-json"

        with caplog.at_level(
            logging.ERROR, logger="docling_jobkit.orchestrators.rq.orchestrator"
        ):
            await orch._store_task_in_redis(
                _make_task("broken-task", TaskStatus.PENDING)
            )

        assert orch._async_redis_conn.stored[key] == "not-json"
        assert orch._async_redis_conn.pipeline_executions == 0
        assert "malformed" in caplog.text

    @pytest.mark.asyncio
    async def test_store_and_retrieve_error_message(self):
        orch = _make_orchestrator()
        task = _make_task("err-task", TaskStatus.FAILURE, error_message="Out of memory")

        stored_data: dict[str, str] = {}

        async def fake_set(key: str, value: str, ex: int) -> None:
            stored_data[key] = value

        async def fake_get(key: str) -> bytes | None:
            value = stored_data.get(key)
            return None if value is None else value.encode()

        orch._async_redis_conn.set = fake_set
        orch._async_redis_conn.get = fake_get

        await orch._store_task_in_redis(task)
        retrieved = await orch._get_task_from_redis("err-task")

        assert retrieved is not None
        assert retrieved.error_message == "Out of memory"
        assert retrieved.task_status == TaskStatus.FAILURE

    @pytest.mark.asyncio
    async def test_backward_compatible_metadata_without_error_message(self):
        orch = _make_orchestrator()
        old_data = json.dumps(
            {
                "task_id": "old-task-1",
                "task_type": "convert",
                "task_status": "success",
                "processing_meta": {
                    "num_docs": 1,
                    "num_processed": 1,
                    "num_succeeded": 1,
                    "num_failed": 0,
                },
            }
        ).encode()
        orch._async_redis_conn.get = AsyncMock(return_value=old_data)

        restored = await orch._get_task_from_redis("old-task-1")

        assert restored is not None
        assert restored.error_message is None
        assert restored.task_status == TaskStatus.SUCCESS


class TestRQRedisGate:
    @pytest.mark.asyncio
    async def test_task_result_raises_backpressure_when_gate_saturated(self):
        config = RQOrchestratorConfig(
            redis_gate_concurrency=1,
            redis_gate_wait_timeout=0.01,
        )
        orch = _make_orchestrator(config)

        async with orch._redis_gate.acquire(1.0):
            with pytest.raises(RedisBackpressureError):
                await orch.task_result("busy-task")

    @pytest.mark.asyncio
    async def test_enqueue_raises_backpressure_when_gate_saturated(self):
        config = RQOrchestratorConfig(
            redis_gate_concurrency=1,
            redis_gate_wait_timeout=0.01,
        )
        orch = _make_orchestrator(config)

        async with orch._redis_gate.acquire(1.0):
            with pytest.raises(RedisBackpressureError):
                await orch.enqueue(
                    sources=[],
                    target=InBodyTarget(),
                )

    @pytest.mark.asyncio
    async def test_task_status_uses_status_poll_wait_timeout(self):
        config = RQOrchestratorConfig(
            redis_gate_concurrency=1,
            redis_gate_wait_timeout=0.01,
            redis_gate_status_poll_wait_timeout=0.05,
        )
        orch = _make_orchestrator(config)

        start = time.monotonic()
        async with orch._redis_gate.acquire(1.0):
            with pytest.raises(RedisBackpressureError):
                await orch.task_status("busy-task")
        elapsed = time.monotonic() - start

        assert elapsed >= 0.04

    @pytest.mark.asyncio
    async def test_pubsub_listener_is_not_gated(self):
        orch = _make_orchestrator()
        task = Task(task_id="task-1", sources=[], target=InBodyTarget())
        orch.tasks[task.task_id] = task
        orch._async_redis_conn.pubsub = MagicMock(
            return_value=_make_pubsub(
                [_make_pubsub_message(task.task_id, TaskStatus.STARTED)]
            )
        )

        async with orch._redis_gate.acquire(1.0):
            await orch._listen_for_updates()

        assert task.task_status == TaskStatus.STARTED

    @pytest.mark.asyncio
    async def test_watchdog_is_not_gated(self):
        orch = _make_orchestrator()
        orch._async_redis_conn.exists = AsyncMock(return_value=1)
        registry = MagicMock()
        registry.get_job_ids.return_value = []

        sleep_mock = AsyncMock(side_effect=[None, asyncio.CancelledError()])
        with (
            patch(
                "docling_jobkit.orchestrators.rq.orchestrator.StartedJobRegistry",
                return_value=registry,
            ),
            patch(
                "docling_jobkit.orchestrators.rq.orchestrator.asyncio.sleep", sleep_mock
            ),
        ):
            async with orch._redis_gate.acquire(1.0):
                with pytest.raises(asyncio.CancelledError):
                    await orch._watchdog_task()

    @pytest.mark.asyncio
    async def test_watchdog_orphan_uses_remove_executions(self):
        from docling_jobkit.orchestrators.rq.orchestrator import (
            _WATCHDOG_GRACE_PERIOD,
        )

        orch = _make_orchestrator()
        orch._async_redis_conn.exists = AsyncMock(return_value=0)
        orch._async_redis_conn.publish = AsyncMock()

        job = MagicMock()
        job.set_status = MagicMock()

        base_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
        future_time = base_time + datetime.timedelta(
            seconds=_WATCHDOG_GRACE_PERIOD + 60
        )

        registry = MagicMock()
        registry.get_job_ids.return_value = ["orphan-1"]

        fake_dt = MagicMock()
        fake_dt.now = MagicMock(side_effect=[base_time, future_time])
        fake_dt.timezone = datetime.timezone

        sleep_mock = AsyncMock(side_effect=[None, None, asyncio.CancelledError()])
        with (
            patch(
                "docling_jobkit.orchestrators.rq.orchestrator.StartedJobRegistry",
                return_value=registry,
            ),
            patch(
                "docling_jobkit.orchestrators.rq.orchestrator.Job.fetch",
                return_value=job,
            ) as fetch_mock,
            patch(
                "docling_jobkit.orchestrators.rq.orchestrator.asyncio.sleep",
                sleep_mock,
            ),
            patch(
                "docling_jobkit.orchestrators.rq.orchestrator.datetime.datetime",
                new=fake_dt,
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await orch._watchdog_task()

        fetch_mock.assert_called()
        job.set_status.assert_called_once_with(JobStatus.FAILED)
        registry.remove_executions.assert_called_once_with(job)
        registry.remove.assert_not_called()
        orch._async_redis_conn.publish.assert_awaited()

    @pytest.mark.asyncio
    async def test_watchdog_missing_job_is_nonfatal(self):
        from docling_jobkit.orchestrators.rq.orchestrator import (
            _WATCHDOG_GRACE_PERIOD,
        )

        orch = _make_orchestrator()
        orch._async_redis_conn.exists = AsyncMock(return_value=0)
        orch._async_redis_conn.publish = AsyncMock()

        base_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
        future_time = base_time + datetime.timedelta(
            seconds=_WATCHDOG_GRACE_PERIOD + 60
        )

        registry = MagicMock()
        registry.get_job_ids.return_value = ["gone-1"]

        fake_dt = MagicMock()
        fake_dt.now = MagicMock(side_effect=[base_time, future_time])
        fake_dt.timezone = datetime.timezone

        sleep_mock = AsyncMock(side_effect=[None, None, asyncio.CancelledError()])
        with (
            patch(
                "docling_jobkit.orchestrators.rq.orchestrator.StartedJobRegistry",
                return_value=registry,
            ),
            patch(
                "docling_jobkit.orchestrators.rq.orchestrator.Job.fetch",
                side_effect=NoSuchJobError("already expired"),
            ),
            patch(
                "docling_jobkit.orchestrators.rq.orchestrator.asyncio.sleep",
                sleep_mock,
            ),
            patch(
                "docling_jobkit.orchestrators.rq.orchestrator.datetime.datetime",
                new=fake_dt,
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await orch._watchdog_task()

        registry.remove.assert_not_called()
        registry.remove_executions.assert_not_called()

    @pytest.mark.asyncio
    async def test_reaper_is_not_gated(self):
        config = RQOrchestratorConfig(
            zombie_reaper_interval=0.01,
            zombie_reaper_max_age=3600.0,
        )
        orch = _make_orchestrator(config)
        old_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            hours=2
        )
        orch.tasks["old-1"] = _make_task(
            "old-1", TaskStatus.SUCCESS, finished_at=old_time
        )

        async with orch._redis_gate.acquire(1.0):
            reaper = asyncio.create_task(orch._reap_zombie_tasks())
            await asyncio.sleep(0.05)
            reaper.cancel()
            with pytest.raises(asyncio.CancelledError):
                await reaper

        assert "old-1" not in orch.tasks
