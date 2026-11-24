"""A queued task that no longer validates must not block its tenant's queue.

Reproduces a rolling-upgrade incident: the API enqueued tasks whose
``convert_options.from_formats`` contained a format unknown to the dispatcher.
``peek_task`` raised on every dispatch round and every task queued behind the
invalid one waited forever.

Redis tests use the real Redis service provided by CI.
"""

import json
import os
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

pytest.importorskip("redis")

from docling.datamodel.service.targets import InBodyTarget

from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.ray.redis_helper import (
    InvalidQueuedTaskError,
    RedisStateManager,
)


def _invalid_payload(task_id: str) -> str:
    raw = Task(task_id=task_id, sources=[], target=InBodyTarget()).model_dump(
        mode="json"
    )
    raw["convert_options"] = {"from_formats": ["pdf", "not_a_known_format"]}
    return json.dumps(raw)


def _valid_payload(task_id: str) -> str:
    return Task(task_id=task_id, sources=[], target=InBodyTarget()).model_dump_json()


@pytest_asyncio.fixture
async def real_redis():
    prefix = uuid.uuid4().hex
    tenant_id = f"test-{prefix}"
    manager = RedisStateManager(
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0")
    )
    await manager.connect()
    redis = manager._ensure_redis()
    try:
        await redis.ping()
    except Exception as exc:
        await manager.disconnect()
        pytest.fail(f"Redis is required for queue tests: {exc}")

    try:
        yield manager, redis, tenant_id
    finally:
        await redis.delete(
            f"tenant:{tenant_id}:tasks",
            f"tenant:{tenant_id}:limits",
        )
        await manager.disconnect()


@pytest.mark.asyncio
async def test_peek_task_raises_invalid_queued_task_error(real_redis) -> None:
    manager, redis, tenant_id = real_redis
    await redis.rpush(f"tenant:{tenant_id}:tasks", _invalid_payload("bad-1"))

    with pytest.raises(InvalidQueuedTaskError) as exc_info:
        await manager.peek_task(tenant_id)

    assert exc_info.value.tenant_id == tenant_id
    assert exc_info.value.task_id == "bad-1"
    assert exc_info.value.task_size == 1


@pytest.mark.asyncio
async def test_remove_queued_task_payload_pops_matching_head(real_redis) -> None:
    manager, redis, tenant_id = real_redis
    queue_key = f"tenant:{tenant_id}:tasks"
    bad = _invalid_payload("bad-1")
    await redis.rpush(queue_key, bad, _valid_payload("good-1"))
    await redis.hset(f"tenant:{tenant_id}:limits", "queued_tasks", "2")

    assert await manager.remove_queued_task_payload(tenant_id, bad) is True

    assert await redis.llen(queue_key) == 1
    task = await manager.peek_task(tenant_id)
    assert task is not None
    assert task.task_id == "good-1"
    limits = await redis.hget(f"tenant:{tenant_id}:limits", "queued_tasks")
    assert limits == b"1"


@pytest.mark.asyncio
async def test_remove_queued_task_payload_keeps_different_head(real_redis) -> None:
    manager, redis, tenant_id = real_redis
    queue_key = f"tenant:{tenant_id}:tasks"
    await redis.rpush(queue_key, _valid_payload("good-1"))

    removed = await manager.remove_queued_task_payload(
        tenant_id, _invalid_payload("bad-1")
    )

    assert removed is False
    assert await redis.llen(queue_key) == 1


@pytest.mark.asyncio
async def test_dispatcher_drops_invalid_head_and_dispatches_next() -> None:
    pytest.importorskip("ray")
    if os.getenv("CI"):
        pytest.skip("Skipping Ray tests in CI")
    from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
    from docling_jobkit.orchestrators.ray.dispatcher import RayTaskDispatcher

    config = RayOrchestratorConfig(redis_url="redis://localhost:6379/")
    dispatcher = RayTaskDispatcher.__ray_actor_class__(config, object())
    manager = AsyncMock()
    dispatcher.redis_manager = manager

    bad = _invalid_payload("bad-1")
    invalid = InvalidQueuedTaskError("tenant-a", bad, ValueError("bad format"))
    good = Task(task_id="good-1", sources=[], target=InBodyTarget())
    manager.peek_task = AsyncMock(side_effect=[invalid, good])
    manager.remove_queued_task_payload = AsyncMock(return_value=True)
    manager.finalize_task_failure_atomic = AsyncMock(
        return_value=MagicMock(final_status=TaskStatus.FAILURE, status_changed=True)
    )
    manager.check_tenant_can_process = AsyncMock(return_value=(True, ""))
    manager.dispatch_task_atomic = AsyncMock(return_value=True)
    dispatcher._process_task_async = AsyncMock()

    dispatched = await dispatcher._dispatch_tenant_task("tenant-a")

    assert dispatched is True
    manager.remove_queued_task_payload.assert_awaited_once_with("tenant-a", bad)
    failure_call = manager.finalize_task_failure_atomic.await_args
    assert failure_call.kwargs["task_id"] == "bad-1"
    assert failure_call.kwargs["tenant_id"] == "tenant-a"
    update = manager.publish_update.await_args.args[0]
    assert update.task_id == "bad-1"
    assert update.task_status == TaskStatus.FAILURE
    manager.dispatch_task_atomic.assert_awaited_once_with("tenant-a", "good-1", 0)
