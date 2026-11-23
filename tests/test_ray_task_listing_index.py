# SPDX-License-Identifier: MIT

"""Tests for the indexed active-task listing of the Ray backend.

These tests run against a real Redis, because the behaviour under test only
exists when the real commands are issued: the listing membership is published
in the enqueue transaction, removed in the terminal transaction, and a ready
page reads one window of the tenant's index instead of the queued Task JSON.

Every test owns a tenant of its own, so it owns a listing index of its own, and
it removes exactly the keys it created.
"""

import datetime
import json
import os
import uuid
from typing import Any, Optional

import pytest
import redis.exceptions

from docling.datamodel.base_models import FailureCategory
from docling.datamodel.service.responses import FailurePhase, PublicFailureInfo

from docling_jobkit.datamodel.task import Task, validate_task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.orchestrator import RayOrchestrator
from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager
from docling_jobkit.orchestrators.redis_task_index import (
    index_score,
    tenant_build_lock_key,
    tenant_generation_key,
    tenant_index_key,
    tenant_ready_key,
)

REDIS_URL = (
    os.getenv("DOCLING_TEST_REDIS_URL")
    or os.getenv("REDIS_URL")
    or "redis://127.0.0.1:6379/15"
)

_T0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)


def _at(seconds: int) -> datetime.datetime:
    return _T0 + datetime.timedelta(seconds=seconds)


class _StubConverterManager:
    # Mirrors the attribute DoclingConverterManagerConfig exposes here.
    class config:
        allow_external_plugins = False


class _CountingPipeline:
    """Pipeline proxy that counts the commands a listing queues."""

    def __init__(self, store: "_CountingRedis", pipe: Any) -> None:
        self._store = store
        self._pipe = pipe

    async def __aenter__(self) -> "_CountingPipeline":
        await self._pipe.__aenter__()
        return self

    async def __aexit__(self, *exc_info: Any) -> bool:
        return await self._pipe.__aexit__(*exc_info)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._pipe, name)
        if not callable(attr):
            return attr

        def _record(*args: Any, **kwargs: Any) -> Any:
            self._store.commands.append(name)
            return attr(*args, **kwargs)

        return _record


class _CountingRedis:
    """Redis proxy that counts the commands a listing issues."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self.commands: list[str] = []

    def pipeline(self, transaction: bool = False) -> Any:
        return _CountingPipeline(self, self._client.pipeline(transaction=transaction))

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._client, name)
        if not callable(attr):
            return attr

        def _record(*args: Any, **kwargs: Any) -> Any:
            self.commands.append(name)
            return attr(*args, **kwargs)

        return _record


class _TransactionRecord:
    """One executed pipeline batch and the transaction mode it was created with."""

    def __init__(
        self,
        transaction: bool,
        commands: list[tuple[str, tuple[Any, ...], dict[str, Any]]],
    ) -> None:
        self.transaction = transaction
        self.commands = commands


class _RecordingPipeline:
    """Forwards a real pipeline and records the commands of each executed batch."""

    _MUTATIONS = frozenset(
        {"hset", "hdel", "hincrby", "setex", "delete", "expire", "srem", "zrem"}
    )

    def __init__(
        self, pipe: Any, recorder: "_RecordingRedis", transaction: bool
    ) -> None:
        self._pipe = pipe
        self._recorder = recorder
        self._transaction = transaction
        self._queued: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def __aenter__(self) -> "_RecordingPipeline":
        await self._pipe.__aenter__()
        return self

    async def __aexit__(self, *exc_info: Any) -> bool:
        return await self._pipe.__aexit__(*exc_info)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._pipe, name)
        if not callable(attr):
            return attr

        def _forward(*args: Any, **kwargs: Any) -> Any:
            result = attr(*args, **kwargs)
            if name in self._MUTATIONS:
                self._queued.append((name, args, kwargs))
            return result

        return _forward

    async def execute(self) -> Any:
        self._recorder.executions.append(
            _TransactionRecord(self._transaction, self._queued)
        )
        self._queued = []
        return await self._pipe.execute()


class _RecordingRedis:
    """Redis proxy that forwards real commands and records executed batches."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self.executions: list[_TransactionRecord] = []

    def pipeline(self, transaction: bool = False) -> Any:
        return _RecordingPipeline(
            self._client.pipeline(transaction=transaction), self, transaction
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


class _InterleavingPipeline:
    """Forwards a real pipeline and fires a hook before a matching commit."""

    def __init__(self, pipe: Any, redis: "_InterleavingRedis") -> None:
        self._pipe = pipe
        self._redis = redis
        self._commands: list[str] = []

    async def __aenter__(self) -> "_InterleavingPipeline":
        await self._pipe.__aenter__()
        return self

    async def __aexit__(self, *exc_info: Any) -> bool:
        return await self._pipe.__aexit__(*exc_info)

    def _write(self, name: str, *args: Any, **kwargs: Any) -> Any:
        self._commands.append(name)
        return getattr(self._pipe, name)(*args, **kwargs)

    def zrem(self, *args: Any, **kwargs: Any) -> Any:
        return self._write("zrem", *args, **kwargs)

    async def execute(self) -> Any:
        commands = list(self._commands)
        self._commands.clear()
        if any(name in self._redis.trigger_commands for name in commands):
            await self._redis.fire_once()
        return await self._pipe.execute()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._pipe, name)


class _InterleavingRedis:
    """Redis proxy that runs a writer just before a selected commit."""

    def __init__(
        self,
        client: Any,
        *,
        trigger_commands: set[str],
        callback: Any,
        once: bool = True,
    ) -> None:
        self._client = client
        self.trigger_commands = trigger_commands
        self._callback = callback
        self._once = once
        self.fired = False
        self.fires = 0

    def pipeline(self, transaction: bool = False) -> Any:
        return _InterleavingPipeline(
            self._client.pipeline(transaction=transaction), self
        )

    async def fire_once(self) -> None:
        if self._once and self.fired:
            return
        self.fired = True
        self.fires += 1
        await self._callback()

    async def zrem(self, *args: Any, **kwargs: Any) -> Any:
        if "zrem" in self.trigger_commands:
            await self.fire_once()
        return await self._client.zrem(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


class _MarkReadyRaceRedis:
    """Runs an interleaving writer when a build tries to publish readiness."""

    def __init__(self, client: Any, callback: Any) -> None:
        self._client = client
        self._callback = callback
        self.fired = False

    async def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        if not self.fired and numkeys == 2 and "ARGV[1]" in script:
            self.fired = True
            await self._callback()
        return await self._client.eval(script, numkeys, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


class _Harness:
    """Publishes Ray tasks into a real Redis and removes what it created."""

    def __init__(self, *, enable_task_listing: bool = True) -> None:
        self.tenant = f"raylist-{uuid.uuid4().hex}"
        self.manager = RedisStateManager(
            redis_url=REDIS_URL, enable_task_listing=enable_task_listing
        )
        self._clients = [self.manager]
        self._task_ids: list[str] = []
        self._tenants: set[str] = {self.tenant}

    @property
    def redis(self) -> Any:
        return self.manager.redis

    async def connect(self) -> None:
        await self.manager.connect()

    def orchestrator(self) -> RayOrchestrator:
        orchestrator = RayOrchestrator(
            config=RayOrchestratorConfig(redis_url=REDIS_URL, enable_task_listing=True),
            converter_manager=_StubConverterManager(),  # type: ignore[arg-type]
        )
        orchestrator.redis_manager = self.manager
        return orchestrator

    def legacy_manager(self) -> RedisStateManager:
        """A producer that predates the index: it never publishes membership."""
        manager = RedisStateManager(redis_url=REDIS_URL, enable_task_listing=False)
        self._clients.append(manager)
        return manager

    async def publish(
        self,
        *,
        tenant_id: Optional[str] = None,
        status: TaskStatus = TaskStatus.PENDING,
        created_at: Optional[datetime.datetime] = None,
        manager: Optional[RedisStateManager] = None,
        in_queue: bool = True,
    ) -> Task:
        """Enqueue a task the way a producer does."""
        tenant = tenant_id or self.tenant
        producer = manager or self.manager
        if producer.redis is None:
            await producer.connect()
        task = validate_task(
            {
                "task_id": f"task-{uuid.uuid4().hex}",
                "task_type": "convert",
                "task_status": status,
                "metadata": {"tenant_id": tenant},
            }
        )
        task.created_at = created_at or _T0
        self._task_ids.append(task.task_id)
        self._tenants.add(tenant)
        await producer.set_task_metadata(
            task_id=task.task_id,
            tenant_id=tenant,
            task_type=task.task_type,
            task_size=1,
            status=status,
            created_at=task.created_at,
        )
        if in_queue:
            await producer.enqueue_task(tenant, task)
        await producer._ensure_redis().hset(
            f"task:{task.task_id}", "status", status.value
        )
        return task

    async def cleanup(self) -> None:
        keys: list[str] = []
        for task_id in self._task_ids:
            keys += [
                f"task:{task_id}",
                f"task:{task_id}:dispatch",
                f"task:{task_id}:execution",
                f"{self.manager.results_prefix}:task:{task_id}:result",
            ]
        for tenant in self._tenants:
            keys += [
                f"tenant:{tenant}:tasks",
                f"tenant:{tenant}:active_tasks",
                f"tenant:{tenant}:task_counters",
                f"tenant:{tenant}:limits",
                f"tenant:{tenant}:stats",
                tenant_index_key(self.manager._index_namespace, tenant),
                tenant_ready_key(self.manager._index_namespace, tenant),
                tenant_build_lock_key(self.manager._index_namespace, tenant),
                tenant_generation_key(self.manager._index_namespace, tenant),
            ]
        cleanup_client = self.manager.redis
        if cleanup_client is not None:
            if keys:
                await cleanup_client.delete(*keys)
            if self._tenants:
                await cleanup_client.srem("tenants:known", *self._tenants)
        for client in self._clients:
            await client.disconnect()


@pytest.fixture
async def harness():
    instance = _Harness()
    await instance.connect()
    try:
        yield instance
    finally:
        await instance.cleanup()


def _failure() -> PublicFailureInfo:
    return PublicFailureInfo(
        category=FailureCategory.UNKNOWN,
        message="boom",
        retryable=False,
        phase=FailurePhase.ORCHESTRATION,
    )


async def test_enqueue_publishes_membership_and_start_keeps_it(harness):
    task = await harness.publish(created_at=_at(10))
    orchestrator = harness.orchestrator()

    first = await orchestrator.list_tasks(tenant_id=harness.tenant)
    assert [item.task_id for item in first.items] == [task.task_id]
    assert [item.task_status for item in first.items] == [TaskStatus.PENDING]

    await harness.manager.mark_task_started(task.task_id, harness.tenant)
    second = await orchestrator.list_tasks(tenant_id=harness.tenant)

    assert [item.task_id for item in second.items] == [task.task_id]
    assert [item.task_status for item in second.items] == [TaskStatus.STARTED]


async def test_dispatch_keeps_the_task_in_the_index(harness):
    task = await harness.publish(created_at=_at(10))
    orchestrator = harness.orchestrator()
    await orchestrator.list_tasks(tenant_id=harness.tenant)

    dispatched = await harness.manager.dispatch_task_atomic(
        harness.tenant, task.task_id, 1
    )

    assert dispatched is True
    page = await orchestrator.list_tasks(tenant_id=harness.tenant)
    assert [item.task_id for item in page.items] == [task.task_id]


async def test_terminal_finalize_removes_the_task_from_the_index(harness):
    task = await harness.publish(created_at=_at(10))
    orchestrator = harness.orchestrator()
    await orchestrator.list_tasks(tenant_id=harness.tenant)

    result = await harness.manager.finalize_task_failure_atomic(
        tenant_id=harness.tenant,
        task_id=task.task_id,
        task_size=1,
        error_message="boom",
        failure=_failure(),
    )

    assert result.final_status == TaskStatus.FAILURE
    page = await orchestrator.list_tasks(tenant_id=harness.tenant)
    assert page.items == []
    members = await harness.redis.zrange(
        tenant_index_key(harness.manager._index_namespace, harness.tenant), 0, -1
    )
    assert members == []


async def test_terminal_finalize_groups_status_write_and_index_removal(harness):
    task = await harness.publish(created_at=_at(10))
    orchestrator = harness.orchestrator()
    await orchestrator.list_tasks(tenant_id=harness.tenant)

    real_redis = harness.manager.redis
    recording = _RecordingRedis(real_redis)
    harness.manager.redis = recording  # type: ignore[assignment]
    try:
        result = await harness.manager.finalize_task_failure_atomic(
            tenant_id=harness.tenant,
            task_id=task.task_id,
            task_size=1,
            error_message="boom",
            failure=_failure(),
        )
    finally:
        harness.manager.redis = real_redis  # type: ignore[assignment]

    assert result.final_status == TaskStatus.FAILURE
    assert result.status_changed is True
    task_key = f"task:{task.task_id}"
    index_key = tenant_index_key(harness.manager._index_namespace, harness.tenant)

    # The terminal write and the membership removal must be queued on the same
    # transactional pipeline and sent by one execute, not just end up correct.
    batches = [
        record
        for record in recording.executions
        if any(
            name == "hset" and args[0] == task_key for name, args, _ in record.commands
        )
    ]
    assert len(batches) == 1
    batch = batches[0]
    assert batch.transaction is True

    status_writes = [
        kwargs["mapping"]
        for name, args, kwargs in batch.commands
        if name == "hset" and args[0] == task_key
    ]
    assert status_writes and status_writes[0]["status"] == TaskStatus.FAILURE.value
    assert any(
        name == "zrem" and args == (index_key, task.task_id)
        for name, args, _ in batch.commands
    )
    # The removal never ran outside the transaction.
    assert not any(
        name == "zrem" and not record.transaction
        for record in recording.executions
        for name, _, _ in record.commands
    )
    assert await real_redis.zrange(index_key, 0, -1) == []


async def test_an_expired_task_hash_is_omitted_and_removed(harness):
    task = await harness.publish(created_at=_at(10))
    orchestrator = harness.orchestrator()
    await orchestrator.list_tasks(tenant_id=harness.tenant)

    await harness.redis.delete(f"task:{task.task_id}")
    page = await orchestrator.list_tasks(tenant_id=harness.tenant)

    assert page.items == []
    members = await harness.redis.zrange(
        tenant_index_key(harness.manager._index_namespace, harness.tenant), 0, -1
    )
    assert members == []


async def test_a_terminal_state_written_after_indexing_is_never_listed(harness):
    task = await harness.publish(created_at=_at(10))
    orchestrator = harness.orchestrator()
    await orchestrator.list_tasks(tenant_id=harness.tenant)

    # Simulate a terminal write that did not reach the membership drop: the
    # page must re-check the durable status instead of trusting the index.
    await harness.redis.hset(f"task:{task.task_id}", "status", "failure")
    page = await orchestrator.list_tasks(tenant_id=harness.tenant)

    assert page.items == []
    members = await harness.redis.zrange(
        tenant_index_key(harness.manager._index_namespace, harness.tenant), 0, -1
    )
    assert members == []


async def _add_stale_members(harness, count: int) -> list[str]:
    """Add *count* index members with no task hash, newest first."""
    index_key = tenant_index_key(harness.manager._index_namespace, harness.tenant)
    members = [f"stale-{uuid.uuid4().hex}" for _ in range(count)]
    await harness.redis.zadd(
        index_key,
        {
            member: index_score(_at(100 + position))
            for position, member in enumerate(members)
        },
    )
    return members


async def test_a_stale_prefix_does_not_consume_the_offset(harness):
    first = await harness.publish(created_at=_at(20))
    second = await harness.publish(created_at=_at(10))
    stale = await _add_stale_members(harness, 1)
    index_key = tenant_index_key(harness.manager._index_namespace, harness.tenant)

    page = await harness.orchestrator().list_tasks(
        tenant_id=harness.tenant, limit=1, offset=1
    )

    assert [item.task_id for item in page.items] == [second.task_id]
    assert page.has_more is False
    assert set(await harness.redis.zrange(index_key, 0, -1)) == {
        first.task_id.encode(),
        second.task_id.encode(),
    }
    assert stale[0].encode() not in set(await harness.redis.zrange(index_key, 0, -1))


async def test_a_page_reads_past_more_than_three_stale_members(harness):
    first = await harness.publish(created_at=_at(20))
    second = await harness.publish(created_at=_at(10))
    await _add_stale_members(harness, 6)
    index_key = tenant_index_key(harness.manager._index_namespace, harness.tenant)

    page = await harness.orchestrator().list_tasks(tenant_id=harness.tenant, limit=1)

    assert [item.task_id for item in page.items] == [first.task_id]
    assert page.has_more is True
    assert set(await harness.redis.zrange(index_key, 0, -1)) == {
        first.task_id.encode(),
        second.task_id.encode(),
    }


async def test_mismatched_and_missing_records_are_removed_without_exposure(harness):
    foreign_tenant = f"raylist-{uuid.uuid4().hex}"
    harness._tenants.add(foreign_tenant)
    foreign = await harness.publish(tenant_id=foreign_tenant, created_at=_at(20))
    live = await harness.publish(created_at=_at(10))
    index_key = tenant_index_key(harness.manager._index_namespace, harness.tenant)
    missing = f"missing-{uuid.uuid4().hex}"
    await harness.redis.zadd(
        index_key,
        {
            foreign.task_id: index_score(_at(20)),
            missing: index_score(_at(30)),
        },
    )

    page = await harness.orchestrator().list_tasks(tenant_id=harness.tenant, limit=5)

    assert [item.task_id for item in page.items] == [live.task_id]
    assert set(await harness.redis.zrange(index_key, 0, -1)) == {live.task_id.encode()}
    foreign_index = tenant_index_key(harness.manager._index_namespace, foreign_tenant)
    assert foreign.task_id.encode() in set(
        await harness.redis.zrange(foreign_index, 0, -1)
    )


async def test_one_tenant_invalidation_during_build_keeps_another_ready(
    harness,
):
    other_tenant = f"raylist-{uuid.uuid4().hex}"
    harness._tenants.add(other_tenant)
    other_task = await harness.publish(tenant_id=other_tenant, created_at=_at(10))
    task = await harness.publish(created_at=_at(10))
    orchestrator = harness.orchestrator()

    other_page = await orchestrator.list_tasks(tenant_id=other_tenant)
    assert [item.task_id for item in other_page.items] == [other_task.task_id]
    assert await harness.manager.is_task_index_ready(other_tenant)

    real_redis = harness.manager.redis

    async def _invalidate_during_build() -> None:
        await harness.manager.invalidate_task_index(harness.tenant)

    racing = _MarkReadyRaceRedis(real_redis, _invalidate_during_build)
    harness.manager.redis = racing  # type: ignore[assignment]
    try:
        page = await orchestrator.list_tasks(tenant_id=harness.tenant)
    finally:
        harness.manager.redis = real_redis  # type: ignore[assignment]

    assert racing.fired is True
    # The invalidated tenant refuses readiness and falls back to the scan; the
    # unrelated tenant's ready scope is untouched.
    assert not await harness.manager.is_task_index_ready(harness.tenant)
    assert await harness.manager.is_task_index_ready(other_tenant)
    assert [item.task_id for item in page.items] == [task.task_id]
    assert [
        item.task_id
        for item in (await orchestrator.list_tasks(tenant_id=other_tenant)).items
    ] == [other_task.task_id]

    repaired = await orchestrator.list_tasks(tenant_id=harness.tenant)
    assert [item.task_id for item in repaired.items] == [task.task_id]
    assert await harness.manager.is_task_index_ready(harness.tenant)


async def test_a_contended_stale_cleanup_leaves_the_row_for_the_next_query(
    harness,
):
    task = await harness.publish(created_at=_at(10))
    orchestrator = harness.orchestrator()
    first = await orchestrator.list_tasks(tenant_id=harness.tenant)
    assert [item.task_id for item in first.items] == [task.task_id]

    real_redis = harness.manager.redis
    task_key = f"task:{task.task_id}"
    index_key = tenant_index_key(harness.manager._index_namespace, harness.tenant)
    await real_redis.delete(task_key)

    async def _touch_hash() -> None:
        # Keep the row unusable while changing the watched key, so every
        # cleanup attempt loses the WATCH race.
        await real_redis.hset(task_key, "probe", uuid.uuid4().hex)

    racing = _InterleavingRedis(
        real_redis,
        trigger_commands={"zrem"},
        callback=_touch_hash,
        once=False,
    )
    harness.manager.redis = racing  # type: ignore[assignment]
    try:
        await orchestrator.list_tasks(tenant_id=harness.tenant)
    finally:
        harness.manager.redis = real_redis  # type: ignore[assignment]

    assert racing.fires == 3
    assert task.task_id.encode() in set(await real_redis.zrange(index_key, 0, -1))


async def test_guarded_cleanup_keeps_a_concurrently_refreshed_task(harness):
    task = await harness.publish(created_at=_at(10))
    orchestrator = harness.orchestrator()
    first = await orchestrator.list_tasks(tenant_id=harness.tenant)
    assert [item.task_id for item in first.items] == [task.task_id]

    real_redis = harness.manager.redis
    task_key = f"task:{task.task_id}"
    index_key = tenant_index_key(harness.manager._index_namespace, harness.tenant)
    # The task hash expires while the index member remains.
    await real_redis.delete(task_key)

    async def _refresh() -> None:
        await harness.manager.set_task_metadata(
            task_id=task.task_id,
            tenant_id=harness.tenant,
            task_type=task.task_type,
            task_size=1,
            status=TaskStatus.STARTED,
            created_at=task.created_at,
        )

    racing = _InterleavingRedis(
        real_redis,
        trigger_commands={"zrem"},
        callback=_refresh,
    )
    harness.manager.redis = racing  # type: ignore[assignment]
    try:
        await orchestrator.list_tasks(tenant_id=harness.tenant)
    finally:
        harness.manager.redis = real_redis  # type: ignore[assignment]

    assert racing.fired is True
    # Repair must not delete the membership the concurrent refresh restored.
    assert task.task_id.encode() in set(await real_redis.zrange(index_key, 0, -1))
    second = await orchestrator.list_tasks(tenant_id=harness.tenant)
    assert [item.task_id for item in second.items] == [task.task_id]
    assert second.items[0].task_status == TaskStatus.STARTED


async def test_disabled_ray_enqueue_invalidates_its_tenant_only(harness):
    old = await harness.publish(created_at=_at(10))
    other_tenant = f"raylist-{uuid.uuid4().hex}"
    harness._tenants.add(other_tenant)
    other_task = await harness.publish(tenant_id=other_tenant, created_at=_at(5))
    orchestrator = harness.orchestrator()
    assert [
        item.task_id
        for item in (await orchestrator.list_tasks(tenant_id=harness.tenant)).items
    ] == [old.task_id]
    await orchestrator.list_tasks(tenant_id=other_tenant)
    assert await harness.manager.is_task_index_ready(harness.tenant)
    assert await harness.manager.is_task_index_ready(other_tenant)

    new = validate_task(
        {
            "task_id": f"task-{uuid.uuid4().hex}",
            "task_type": "convert",
            "task_status": TaskStatus.PENDING,
            "metadata": {"tenant_id": harness.tenant},
        }
    )
    new.created_at = _at(20)
    harness._task_ids.append(new.task_id)
    disabled = RedisStateManager(redis_url=REDIS_URL, enable_task_listing=False)
    try:
        await disabled.set_task_metadata(
            task_id=new.task_id,
            tenant_id=harness.tenant,
            task_type=new.task_type,
            task_size=1,
            created_at=new.created_at,
        )
        await disabled.enqueue_task(harness.tenant, new)

        assert not await harness.manager.is_task_index_ready(harness.tenant)
        assert await harness.manager.is_task_index_ready(other_tenant)
        assert [
            item.task_id
            for item in (await orchestrator.list_tasks(tenant_id=other_tenant)).items
        ] == [other_task.task_id]
    finally:
        await disabled.disconnect()

    repaired = await orchestrator.list_tasks(tenant_id=harness.tenant)
    assert [item.task_id for item in repaired.items] == [new.task_id, old.task_id]
    assert await harness.manager.is_task_index_ready(harness.tenant)


async def test_disabled_ray_fetch_cleanup_invalidates_ready(harness):
    task = await harness.publish(created_at=_at(10))
    orchestrator = harness.orchestrator()
    assert [
        item.task_id
        for item in (await orchestrator.list_tasks(tenant_id=harness.tenant)).items
    ] == [task.task_id]
    assert await harness.manager.is_task_index_ready(harness.tenant)

    disabled = RedisStateManager(redis_url=REDIS_URL, enable_task_listing=False)
    await disabled.connect()
    try:
        orchestrator.redis_manager = disabled
        await orchestrator.on_result_fetched(task.task_id)
    finally:
        await disabled.disconnect()

    assert not await harness.manager.is_task_index_ready(harness.tenant)
    repaired = await harness.orchestrator().list_tasks(tenant_id=harness.tenant)
    assert [item.task_id for item in repaired.items] == [task.task_id]
    assert await harness.manager.is_task_index_ready(harness.tenant)


async def test_a_never_enabled_ray_enqueue_creates_no_index_state():
    manager = RedisStateManager(redis_url=REDIS_URL, enable_task_listing=False)
    tenant = f"raylist-{uuid.uuid4().hex}"
    task = validate_task(
        {
            "task_id": f"task-{uuid.uuid4().hex}",
            "task_type": "convert",
            "task_status": TaskStatus.PENDING,
            "metadata": {"tenant_id": tenant},
        }
    )
    task.created_at = _at(10)
    await manager.connect()
    try:
        await manager.set_task_metadata(
            task_id=task.task_id,
            tenant_id=tenant,
            task_type=task.task_type,
            task_size=1,
            created_at=task.created_at,
        )
        await manager.enqueue_task(tenant, task)

        client = manager.redis
        namespace = manager._index_namespace
        assert not await client.exists(
            tenant_ready_key(namespace, tenant),
            tenant_generation_key(namespace, tenant),
        )
        assert await client.zrange(tenant_index_key(namespace, tenant), 0, -1) == []
    finally:
        await manager.redis.delete(
            f"tenant:{tenant}:tasks",
            f"tenant:{tenant}:task_counters",
            f"tenant:{tenant}:limits",
            f"task:{task.task_id}",
        )
        await manager.redis.srem("tenants:known", tenant)
        await manager.disconnect()


async def test_first_tenant_build_establishes_generation_before_enumeration(harness):
    old = await harness.publish(created_at=_at(10))
    other_tenant = f"raylist-{uuid.uuid4().hex}"
    harness._tenants.add(other_tenant)
    other_task = await harness.publish(tenant_id=other_tenant, created_at=_at(5))
    orchestrator = harness.orchestrator()
    await orchestrator.list_tasks(tenant_id=other_tenant)
    namespace = harness.manager._index_namespace
    generation = tenant_generation_key(namespace, harness.tenant)
    assert not await harness.redis.exists(generation)

    new = validate_task(
        {
            "task_id": f"task-{uuid.uuid4().hex}",
            "task_type": "convert",
            "task_status": TaskStatus.PENDING,
            "metadata": {"tenant_id": harness.tenant},
        }
    )
    new.created_at = _at(20)
    real_redis = harness.manager.redis
    disabled = RedisStateManager(redis_url=REDIS_URL, enable_task_listing=False)

    async def _disabled_write() -> None:
        # The first enabled builder must already have established the fence.
        assert await harness.redis.exists(generation)
        await disabled.set_task_metadata(
            task_id=new.task_id,
            tenant_id=harness.tenant,
            task_type=new.task_type,
            task_size=1,
            created_at=new.created_at,
        )
        await disabled.enqueue_task(harness.tenant, new)
        harness._task_ids.append(new.task_id)

    racing = _MarkReadyRaceRedis(real_redis, _disabled_write)
    harness.manager.redis = racing  # type: ignore[assignment]
    try:
        page = await orchestrator.list_tasks(tenant_id=harness.tenant)
    finally:
        harness.manager.redis = real_redis  # type: ignore[assignment]
        await disabled.disconnect()

    assert racing.fired is True
    assert await harness.redis.exists(generation)
    assert await harness.redis.get(generation) == b"1"
    assert not await harness.manager.is_task_index_ready(harness.tenant)
    assert await harness.manager.is_task_index_ready(other_tenant)
    assert [item.task_id for item in page.items] == [new.task_id, old.task_id]

    repaired = await orchestrator.list_tasks(tenant_id=harness.tenant)
    assert [item.task_id for item in repaired.items] == [new.task_id, old.task_id]
    assert await harness.manager.is_task_index_ready(harness.tenant)
    assert [
        item.task_id
        for item in (await orchestrator.list_tasks(tenant_id=other_tenant)).items
    ] == [other_task.task_id]


async def test_a_cold_build_indexes_legacy_queued_and_active_tasks(harness):
    legacy = harness.legacy_manager()
    queued = await harness.publish(created_at=_at(10), manager=legacy)
    started = await harness.publish(
        created_at=_at(20),
        manager=legacy,
        status=TaskStatus.STARTED,
        in_queue=False,
    )
    # A started task lives in the active set, not in the queue.
    await legacy._ensure_redis().sadd(
        f"tenant:{harness.tenant}:active_tasks", started.task_id
    )

    page = await harness.orchestrator().list_tasks(tenant_id=harness.tenant)

    assert [item.task_id for item in page.items] == [started.task_id, queued.task_id]
    assert await harness.manager.is_task_index_ready(harness.tenant)


async def test_a_cold_build_omits_malformed_stale_and_terminal_payloads(harness):
    legacy = harness.legacy_manager()
    live = await harness.publish(created_at=_at(10), manager=legacy, in_queue=False)
    done = await harness.publish(
        created_at=_at(20),
        manager=legacy,
        status=TaskStatus.SUCCESS,
        in_queue=False,
    )
    missing = await harness.publish(created_at=_at(30), manager=legacy, in_queue=False)
    await harness.redis.delete(f"task:{missing.task_id}")
    await harness.redis.rpush(
        f"tenant:{harness.tenant}:tasks",
        b"not json at all",
        json.dumps({"task_id": ""}),
        json.dumps({"task_id": live.task_id, "task_type": "convert"}),
        json.dumps({"task_id": done.task_id, "task_type": "convert"}),
        json.dumps({"task_id": missing.task_id, "task_type": "convert"}),
    )

    page = await harness.orchestrator().list_tasks(tenant_id=harness.tenant)

    assert [item.task_id for item in page.items] == [live.task_id]


async def test_a_ready_page_reads_only_the_index_window(harness):
    tasks = [await harness.publish(created_at=_at(index)) for index in range(30)]
    other_tenant = f"raylist-{uuid.uuid4().hex}"
    harness._tenants.add(other_tenant)
    for index in range(10):
        await harness.publish(tenant_id=other_tenant, created_at=_at(index))
    orchestrator = harness.orchestrator()
    await orchestrator.list_tasks(tenant_id=harness.tenant)
    real_redis = harness.manager.redis

    counting = _CountingRedis(real_redis)
    harness.manager.redis = counting  # type: ignore[assignment]
    try:
        page = await orchestrator.list_tasks(tenant_id=harness.tenant, limit=5)
    finally:
        harness.manager.redis = real_redis  # type: ignore[assignment]

    assert [item.task_id for item in page.items] == [
        task.task_id for task in reversed(tasks[-5:])
    ]
    assert page.has_more is True
    assert counting.commands.count("zrange") == 1
    assert counting.commands.count("hmget") == 6
    assert "lrange" not in counting.commands
    assert "smembers" not in counting.commands

    counting.commands.clear()
    harness.manager.redis = counting  # type: ignore[assignment]
    try:
        deep = await orchestrator.list_tasks(
            tenant_id=harness.tenant, limit=5, offset=10
        )
    finally:
        harness.manager.redis = real_redis  # type: ignore[assignment]

    assert [item.task_id for item in deep.items] == [
        tasks[index].task_id for index in range(19, 14, -1)
    ]
    assert deep.has_more is True
    # A nonzero offset walks the compact rows before it: the truthful budget
    # is offset + limit + 1 rows, not a page-sized read.
    assert counting.commands.count("zrange") == 1
    assert counting.commands.count("hmget") == 16


async def test_a_page_keeps_the_order_and_the_tie_break(harness):
    first = await harness.publish(created_at=_at(10))
    second = await harness.publish(created_at=_at(10))
    newest = await harness.publish(created_at=_at(20))

    page = await harness.orchestrator().list_tasks(tenant_id=harness.tenant)

    assert [item.task_id for item in page.items] == [
        newest.task_id,
        *sorted([first.task_id, second.task_id]),
    ]


async def test_a_task_of_another_tenant_is_never_listed(harness):
    other_tenant = f"raylist-{uuid.uuid4().hex}"
    harness._tenants.add(other_tenant)
    await harness.publish(tenant_id=other_tenant, created_at=_at(10))

    page = await harness.orchestrator().list_tasks(tenant_id=harness.tenant)

    assert page.items == []


async def test_a_build_held_by_another_caller_falls_back_to_the_scan(harness):
    task = await harness.publish(created_at=_at(10))
    await harness.redis.set(
        tenant_build_lock_key(harness.manager._index_namespace, harness.tenant),
        "other-caller",
    )

    page = await harness.orchestrator().list_tasks(tenant_id=harness.tenant)

    assert [item.task_id for item in page.items] == [task.task_id]
    assert not await harness.manager.is_task_index_ready(harness.tenant)


async def test_a_failed_build_does_not_advertise_a_partial_index(harness, monkeypatch):
    task = await harness.publish(created_at=_at(10))

    async def _fail(tenant_id: str, task_ids: list[str]) -> None:
        raise redis.exceptions.ConnectionError("redis is down")

    monkeypatch.setattr(harness.manager, "_project_listing_chunk", _fail)

    page = await harness.orchestrator().list_tasks(tenant_id=harness.tenant)

    assert [item.task_id for item in page.items] == [task.task_id]
    assert not await harness.manager.is_task_index_ready(harness.tenant)


async def test_listing_disabled_publishes_no_membership():
    harness = _Harness(enable_task_listing=False)
    await harness.connect()
    try:
        task = await harness.publish(created_at=_at(10))
        members = await harness.redis.zrange(
            tenant_index_key(harness.manager._index_namespace, harness.tenant), 0, -1
        )
        assert members == []

        orchestrator = RayOrchestrator(
            config=RayOrchestratorConfig(redis_url=REDIS_URL),
            converter_manager=_StubConverterManager(),  # type: ignore[arg-type]
        )
        orchestrator.redis_manager = harness.manager
        with pytest.raises(NotImplementedError):
            await orchestrator.list_tasks(tenant_id=harness.tenant)

        # The terminal transaction removes a member even when it was written
        # by another process, so a disabled reader never leaves stale state.
        await harness.redis.zadd(
            tenant_index_key(harness.manager._index_namespace, harness.tenant),
            {task.task_id: index_score(task.created_at)},
        )
        await harness.manager.finalize_task_failure_atomic(
            tenant_id=harness.tenant,
            task_id=task.task_id,
            task_size=1,
            error_message="boom",
            failure=_failure(),
        )
        assert (
            await harness.redis.zrange(
                tenant_index_key(harness.manager._index_namespace, harness.tenant),
                0,
                -1,
            )
            == []
        )
    finally:
        await harness.cleanup()
