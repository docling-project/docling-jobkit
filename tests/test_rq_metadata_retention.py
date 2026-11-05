"""Real-Redis retention tests with exact cleanup of UUID-named keys."""

import asyncio
import json
import os
import time
import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

pytest.importorskip("msgpack")
pytest.importorskip("redis")

import redis as sync_redis
import redis.asyncio as async_redis
from rq.job import JobStatus

from docling.datamodel.service.targets import InBodyTarget

from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestrator,
    RQOrchestratorConfig,
)

REDIS_URL = os.getenv(
    "DOCLING_TEST_REDIS_URL",
    os.getenv("REDIS_URL", "redis://127.0.0.1:6379/15"),
)
RESULTS_TTL = 120


def _redis_is_available() -> bool:
    client = sync_redis.Redis.from_url(REDIS_URL, socket_connect_timeout=2)
    try:
        client.ping()
    except Exception:
        return False
    finally:
        client.close()
    return True


if not _redis_is_available():
    pytest.skip(f"Redis is not available at {REDIS_URL}", allow_module_level=True)


def _task(task_id: str, status: TaskStatus) -> Task:
    return Task(
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


@pytest_asyncio.fixture
async def redis_client():
    client = async_redis.from_url(REDIS_URL)
    try:
        yield client
    finally:
        await client.aclose()


@pytest_asyncio.fixture
async def orchestrator():
    orch = RQOrchestrator(
        RQOrchestratorConfig(redis_url=REDIS_URL, results_ttl=RESULTS_TTL)
    )
    try:
        yield orch
    finally:
        await orch.close()


@pytest_asyncio.fixture
async def owned_keys(redis_client):
    task_id = str(uuid.uuid4())
    keys = [
        f"docling:tasks:{task_id}:metadata",
        f"docling:results:{task_id}",
    ]
    try:
        yield task_id, keys
    finally:
        await redis_client.delete(*keys)


class _InterleavingPipeline:
    """Wraps a real pipeline and commits a record between GET and EXEC."""

    def __init__(
        self,
        pipeline: Any,
        on_get: Any,
    ) -> None:
        self._pipeline = pipeline
        self._on_get = on_get

    async def __aenter__(self) -> "_InterleavingPipeline":
        await self._pipeline.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> Any:
        return await self._pipeline.__aexit__(*args)

    async def watch(self, key: str) -> Any:
        return await self._pipeline.watch(key)

    async def get(self, key: str) -> Any:
        value = await self._pipeline.get(key)
        await self._on_get(key)
        return value

    def multi(self) -> Any:
        return self._pipeline.multi()

    def set(self, key: str, value: str) -> Any:
        return self._pipeline.set(key, value)

    async def execute(self) -> Any:
        return await self._pipeline.execute()


async def test_active_metadata_has_no_expiry(orchestrator, redis_client, owned_keys):
    task_id, (metadata_key, _) = owned_keys

    await orchestrator._store_task_in_redis(_task(task_id, TaskStatus.PENDING))

    assert await redis_client.exists(metadata_key) == 1
    assert await redis_client.ttl(metadata_key) == -1
    stored = json.loads(await redis_client.get(metadata_key))
    assert stored["task_status"] == "pending"


async def test_active_rewrite_clears_previous_expiry(
    orchestrator, redis_client, owned_keys
):
    task_id, (metadata_key, _) = owned_keys

    await orchestrator._store_task_in_redis(_task(task_id, TaskStatus.PENDING))
    await redis_client.expire(metadata_key, 60)
    assert await redis_client.ttl(metadata_key) > 0

    await orchestrator._store_task_in_redis(_task(task_id, TaskStatus.STARTED))

    assert await redis_client.ttl(metadata_key) == -1
    stored = json.loads(await redis_client.get(metadata_key))
    assert stored["task_status"] == "started"


async def test_terminal_metadata_uses_results_ttl(
    orchestrator, redis_client, owned_keys
):
    task_id, (metadata_key, _) = owned_keys

    await orchestrator._store_task_in_redis(_task(task_id, TaskStatus.SUCCESS))

    assert 0 < await redis_client.ttl(metadata_key) <= RESULTS_TTL
    stored = json.loads(await redis_client.get(metadata_key))
    assert stored["task_status"] == "success"


async def test_terminal_metadata_survives_stale_active_write(
    orchestrator, redis_client, owned_keys
):
    task_id, (metadata_key, _) = owned_keys

    await orchestrator._store_task_in_redis(_task(task_id, TaskStatus.FAILURE))
    ttl_before = await redis_client.ttl(metadata_key)
    assert ttl_before > 0

    await orchestrator._store_task_in_redis(_task(task_id, TaskStatus.PENDING))

    stored = json.loads(await redis_client.get(metadata_key))
    assert stored["task_status"] == "failure"
    assert await redis_client.ttl(metadata_key) > 0


async def test_concurrent_terminal_commit_is_preserved(
    orchestrator, redis_client, owned_keys, monkeypatch
):
    task_id, (metadata_key, _) = owned_keys
    terminal_payload = json.dumps(
        {"task_id": task_id, "task_type": "convert", "task_status": "success"}
    )
    state: dict[str, Any] = {"committed": False, "pipelines": 0}

    async def commit_terminal(key: str) -> None:
        if state["committed"]:
            return
        state["committed"] = True
        await redis_client.set(key, terminal_payload, ex=RESULTS_TTL)

    real_pipeline = orchestrator._async_redis_conn.pipeline

    def pipeline_factory(*args: Any, **kwargs: Any) -> _InterleavingPipeline:
        state["pipelines"] += 1
        return _InterleavingPipeline(real_pipeline(*args, **kwargs), commit_terminal)

    monkeypatch.setattr(orchestrator._async_redis_conn, "pipeline", pipeline_factory)

    await orchestrator._store_task_in_redis(_task(task_id, TaskStatus.PENDING))

    stored = json.loads(await redis_client.get(metadata_key))
    assert stored["task_status"] == "success"
    assert await redis_client.ttl(metadata_key) > 0
    assert state["pipelines"] == 2


async def test_bounded_contention_gives_up(
    orchestrator, redis_client, owned_keys, caplog, monkeypatch
):
    task_id, (metadata_key, _) = owned_keys
    pending_payload = json.dumps(
        {"task_id": task_id, "task_type": "convert", "task_status": "pending"}
    )
    await redis_client.set(metadata_key, pending_payload, ex=60)

    state: dict[str, Any] = {"committed": False, "pipelines": 0}

    async def touch(key: str) -> None:
        await redis_client.set(key, pending_payload, ex=60)

    real_pipeline = orchestrator._async_redis_conn.pipeline

    def pipeline_factory(*args: Any, **kwargs: Any) -> _InterleavingPipeline:
        state["pipelines"] += 1
        return _InterleavingPipeline(real_pipeline(*args, **kwargs), touch)

    monkeypatch.setattr(orchestrator._async_redis_conn, "pipeline", pipeline_factory)

    started = time.monotonic()
    await orchestrator._store_task_in_redis(_task(task_id, TaskStatus.PENDING))
    elapsed = time.monotonic() - started

    assert state["pipelines"] == 3
    assert elapsed < 10
    assert json.loads(await redis_client.get(metadata_key))["task_status"] == "pending"
    assert await redis_client.ttl(metadata_key) > 0
    assert "contention" in caplog.text


async def test_pending_task_stays_queryable_after_retention_boundary(
    redis_client, owned_keys, monkeypatch
):
    task_id, (metadata_key, result_key) = owned_keys
    short_ttl = 1
    writer = RQOrchestrator(
        RQOrchestratorConfig(redis_url=REDIS_URL, results_ttl=short_ttl)
    )
    replica = RQOrchestrator(
        RQOrchestratorConfig(redis_url=REDIS_URL, results_ttl=short_ttl)
    )
    try:
        await writer._store_task_in_redis(_task(task_id, TaskStatus.PENDING))
        assert await redis_client.ttl(metadata_key) == -1

        await asyncio.sleep(short_ttl + 0.2)

        assert await redis_client.exists(metadata_key) == 1
        assert await redis_client.ttl(metadata_key) == -1

        # Read metadata from Redis on an empty replica; mock only the RQ job.
        queued_job = MagicMock()
        queued_job.kwargs = {"task_data": {"metadata": {}}}
        queued_job.get_status.return_value = JobStatus.QUEUED
        monkeypatch.setattr(
            "docling_jobkit.orchestrators.rq.orchestrator.Job.fetch",
            lambda *args, **kwargs: queued_job,
        )
        assert replica.tasks == {}

        status = await replica.task_status(task_id)

        assert status.task_status == TaskStatus.PENDING
        assert status.task_id == task_id
        assert task_id not in replica.tasks
        assert await redis_client.ttl(metadata_key) == -1
        assert await redis_client.exists(result_key) == 0
    finally:
        await writer.close()
        await replica.close()


async def test_metadata_write_leaves_result_key_untouched(
    orchestrator, redis_client, owned_keys
):
    task_id, (_, result_key) = owned_keys
    await redis_client.set(result_key, b"result-payload", ex=RESULTS_TTL)

    await orchestrator._store_task_in_redis(_task(task_id, TaskStatus.SUCCESS))

    assert await redis_client.get(result_key) == b"result-payload"
    assert 0 < await redis_client.ttl(result_key) <= RESULTS_TTL
