"""Tenant registry, bulk tenant snapshots and finished-task TTL (real Redis).

The registry key is global, so these tests run on their own database (15 by
default) and flush it around every test.
"""

import os

import pytest
import pytest_asyncio

pytest.importorskip("msgpack")
pytest.importorskip("redis")

from docling.datamodel.service.requests import AnyHttpSourceRequest as HttpSource
from docling.datamodel.service.responses import (
    FailureCategory,
    FailurePhase,
    PublicFailureInfo,
)
from docling.datamodel.service.targets import InBodyTarget
from docling.datamodel.service.tasks import TaskType

from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.ray.redis_helper import (
    TENANT_REGISTRY_BACKFILLED_KEY,
    TENANT_REGISTRY_KEY,
    RedisStateManager,
)

REDIS_URL = os.getenv("REDIS_REGISTRY_TEST_URL", "redis://localhost:6379/15")


def _task(task_id: str) -> Task:
    return Task(
        task_id=task_id,
        target=InBodyTarget(),
        sources=[HttpSource(url="https://example.com/doc.pdf")],
    )


async def _new_manager(**kwargs) -> RedisStateManager:
    manager = RedisStateManager(redis_url=REDIS_URL, **kwargs)
    await manager.connect()
    return manager


@pytest_asyncio.fixture
async def manager():
    manager = await _new_manager()
    redis = manager._ensure_redis()
    try:
        await redis.ping()
    except Exception as exc:
        await manager.disconnect()
        pytest.fail(f"Redis is required for tenant registry tests: {exc}")
    await redis.flushdb()
    try:
        yield manager
    finally:
        await redis.flushdb()
        await manager.disconnect()


async def test_enqueue_registers_tenant(manager):
    await manager.enqueue_task("acme", _task("t1"))

    redis = manager._ensure_redis()
    assert await redis.sismember(TENANT_REGISTRY_KEY, "acme")
    assert await manager.get_all_tenants_with_tasks() == ["acme"]
    assert await manager.get_all_tenants_with_any_tasks() == ["acme"]
    assert await manager.get_all_tenants_with_task_counters() == ["acme"]

    await manager.dequeue_task("acme")
    assert await manager.get_all_tenants_with_tasks() == []
    assert await manager.get_all_tenants_with_any_tasks() == []
    # Idle tenants keep their counters, so they must keep being listed.
    assert await manager.get_all_tenants_with_task_counters() == ["acme"]


async def test_registry_backfills_existing_keys_once(manager):
    redis = manager._ensure_redis()
    # Keys written by a version without the registry.
    await redis.rpush("tenant:queued:tasks", "x")
    await redis.sadd("tenant:running:active_tasks", "t1")
    await redis.hincrby("tenant:idle:task_counters", "tasks_enqueued_total", 3)
    await redis.hset("task:t1", mapping={"status": "started"})

    assert await manager.get_known_tenants() == ["idle", "queued", "running"]
    assert await redis.exists(TENANT_REGISTRY_BACKFILLED_KEY)
    assert await manager.get_all_tenants_with_tasks() == ["queued"]
    assert await manager.get_all_tenants_with_active_tasks() == ["running"]
    assert sorted(await manager.get_all_tenants_with_any_tasks()) == [
        "queued",
        "running",
    ]

    # Once backfilled, a fresh manager trusts the registry and does not SCAN.
    await redis.rpush("tenant:unregistered:tasks", "x")
    other = await _new_manager()
    try:
        assert "unregistered" not in await other.get_known_tenants()
        # The dispatcher's periodic reconcile picks it up.
        await other.reconcile_tenant_registry()
        assert "unregistered" in await other.get_known_tenants()
    finally:
        await other.disconnect()


async def test_tenant_snapshots_match_single_reads(manager):
    for tenant_id in ("a", "b"):
        await manager.enqueue_task(tenant_id, _task(f"{tenant_id}-1"))
        await manager.enqueue_task(tenant_id, _task(f"{tenant_id}-2"))
        await manager.update_tenant_stats(
            tenant_id, delta_total_documents=4, delta_successful_documents=3
        )
    redis = manager._ensure_redis()
    await redis.sadd("tenant:a:active_tasks", "a-0")

    snapshots = await manager.get_tenant_snapshots(["a", "b", "never-seen"])

    assert [s.tenant_id for s in snapshots] == ["a", "b", "never-seen"]
    for snap in snapshots:
        tenant_id = snap.tenant_id
        assert snap.counters == await manager.get_tenant_task_counters(tenant_id)
        assert snap.stats == await manager.get_tenant_stats(tenant_id)
        assert snap.queue_size == await manager.get_tenant_queue_size(tenant_id)
        assert snap.active_count == await manager.get_tenant_active_task_count(
            tenant_id
        )
        assert snap.limits == await manager.get_tenant_limits(tenant_id)
    assert snapshots[0].queue_size == 2
    assert snapshots[0].active_count == 1
    assert snapshots[1].stats.successful_documents == 3


async def _finalize(manager: RedisStateManager, task_id: str) -> None:
    await manager.set_task_metadata(
        task_id=task_id,
        tenant_id="acme",
        task_type=TaskType.CONVERT,
        task_size=1,
        status=TaskStatus.STARTED,
    )
    await manager.finalize_task_failure_atomic(
        tenant_id="acme",
        task_id=task_id,
        task_size=1,
        error_message="boom",
        failure=PublicFailureInfo(
            category=FailureCategory.INTERNAL,
            message="Internal processing error.",
            retryable=False,
            phase=FailurePhase.ORCHESTRATION,
        ),
    )


async def test_finalize_sets_ttl_on_task_hash(manager):
    manager.task_metadata_ttl = 3600 * 24 * 7
    await _finalize(manager, "t-ttl")

    redis = manager._ensure_redis()
    ttl = await redis.ttl("task:t-ttl")
    assert 3600 * 24 * 7 - 60 < ttl <= 3600 * 24 * 7


async def test_task_hash_ttl_never_shorter_than_results(manager):
    manager.task_metadata_ttl = 60
    await _finalize(manager, "t-short")

    redis = manager._ensure_redis()
    assert await redis.ttl("task:t-short") > manager.results_ttl - 60


async def test_task_hash_ttl_disabled(manager):
    manager.task_metadata_ttl = None
    await _finalize(manager, "t-keep")

    redis = manager._ensure_redis()
    assert await redis.ttl("task:t-keep") == -1
