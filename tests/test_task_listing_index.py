# SPDX-License-Identifier: MIT

"""Tests for the indexed active-task listing of the RQ backend.

These tests run against a real Redis, because the behaviour under test only
exists when the real commands are issued: the metadata and the projection are
written in one transaction, a cold build reads the queue and the started
registry, and a ready page reads one window of the tenant's index.

Every test owns a queue of its own, so it owns a listing index of its own,
and it removes exactly the keys it created.
"""

import datetime
import json
import os
import time
import uuid
from typing import Any, Optional

import pytest
import redis.asyncio
import redis.exceptions
from rq.job import Job

from docling_jobkit.datamodel.task import Task, validate_task
from docling_jobkit.datamodel.task_listing import tenant_of
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.redis_task_index import (
    build_lock_key,
    generation_key,
    index_score,
    ready_key,
)
from docling_jobkit.orchestrators.rq import orchestrator as rq_orchestrator_module
from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestrator,
    RQOrchestratorConfig,
)

REDIS_URL = (
    os.getenv("DOCLING_TEST_REDIS_URL")
    or os.getenv("REDIS_URL")
    or "redis://127.0.0.1:6379/15"
)

_T0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)


def _at(seconds: int) -> datetime.datetime:
    return _T0 + datetime.timedelta(seconds=seconds)


async def _registry_snapshot(client: Any) -> set[bytes]:
    """Members of the shared RQ queue registry before a test touches it."""
    return set(await client.smembers("rq:queues"))


async def _release_owned_queue(
    client: Any, queue_name: str, baseline: set[bytes]
) -> None:
    """Drop only the queue this test added, never a pre-existing registry entry."""
    queue_key = f"rq:queue:{queue_name}"
    if queue_key.encode("utf-8") in baseline:
        return
    await client.srem("rq:queues", queue_key)
    await client.delete(queue_key)


def _make_task(
    task_id: str,
    *,
    tenant: Optional[str] = "tenant-a",
    status: TaskStatus = TaskStatus.PENDING,
    created_at: Optional[datetime.datetime] = None,
) -> Task:
    metadata: dict[str, Any] = {} if tenant is None else {"tenant_id": tenant}
    task = validate_task(
        {
            "task_id": task_id,
            "task_type": "convert",
            "task_status": status,
            "metadata": metadata,
        }
    )
    task.created_at = created_at or _T0
    return task


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

            def _value() -> Any:
                return attr

            return _value

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

    def hset(self, *args: Any, **kwargs: Any) -> Any:
        return self._write("hset", *args, **kwargs)

    def zadd(self, *args: Any, **kwargs: Any) -> Any:
        return self._write("zadd", *args, **kwargs)

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
    """Publishes tasks into a real Redis and removes what it created."""

    def __init__(self, orchestrator: RQOrchestrator) -> None:
        self.orchestrator = orchestrator
        self._task_ids: list[str] = []
        self._tenants: set[str] = set()
        self._queued: list[str] = []
        self._executions: list[tuple[str, str]] = []

    @property
    def redis(self) -> Any:
        return self.orchestrator._async_redis_conn

    async def publish(
        self,
        task: Task,
        *,
        native_status: Optional[str] = "queued",
        in_queue: bool = False,
        started: bool = False,
    ) -> None:
        """Store a task the way a producer does and give it a native job."""
        self._task_ids.append(task.task_id)
        self._tenants.add(tenant_of(task.metadata))
        await self.orchestrator._store_task_in_redis(task)
        if native_status is not None:
            await self.redis.hset(Job.key_for(task.task_id), "status", native_status)
        if in_queue or started:
            await self.make_job(task, started=started)

    async def make_job(self, task: Task, *, started: bool = False) -> None:
        """Create the native RQ job a listing resolves its status from."""
        self.orchestrator._rq_queue.enqueue(
            "docling_jobkit.orchestrators.rq.worker.docling_task",
            kwargs={"task_data": {}},
            job_id=task.task_id,
        )
        self._queued.append(task.task_id)
        if started:
            self.orchestrator._rq_queue.remove(task.task_id)
            # The started registry holds one execution per running job.
            member = f"{task.task_id}:{uuid.uuid4().hex}"
            key = f"rq:wip:{self.orchestrator.config.queue_name}"
            await self.redis.zadd(key, {member: time.time() + 3600})
            self._executions.append((key, member))

    async def store_metadata_only(self, task: Task) -> None:
        """Store only the task metadata, the way a legacy producer would."""
        self._task_ids.append(task.task_id)
        self._tenants.add(tenant_of(task.metadata))
        data = task.model_dump(mode="json", serialize_as_any=True)
        data["task_status"] = task.task_status.value
        await self.redis.set(
            self.orchestrator._task_metadata_key(task.task_id),
            json.dumps(data),
            ex=3600,
        )

    async def cleanup(self) -> None:
        keys: list[str] = []
        for task_id in self._task_ids:
            keys.append(self.orchestrator._task_metadata_key(task_id))
            keys.append(self.orchestrator._task_record_key(task_id))
            keys.append(Job.key_for(task_id))
        for tenant_id in self._tenants:
            keys.append(self.orchestrator._tenant_index_key(tenant_id))
        keys.append(ready_key(self.orchestrator._index_namespace))
        keys.append(build_lock_key(self.orchestrator._index_namespace))
        keys.append(generation_key(self.orchestrator._index_namespace))
        for task_id in self._queued:
            self.orchestrator._rq_queue.remove(task_id)
        if keys:
            await self.redis.delete(*keys)
        for key, member in self._executions:
            await self.redis.zrem(key, member)


@pytest.fixture
async def harness():
    queue_name = f"listing-{uuid.uuid4().hex}"
    # Snapshot the shared registry before anything can register, so cleanup can
    # remove exactly this test's queue and leave every pre-existing member.
    registry = redis.asyncio.Redis.from_url(REDIS_URL)
    baseline = await _registry_snapshot(registry)
    orchestrator = RQOrchestrator(
        RQOrchestratorConfig(
            redis_url=REDIS_URL,
            queue_name=queue_name,
            enable_task_listing=True,
        )
    )
    instance = _Harness(orchestrator)
    try:
        yield instance
    finally:
        await instance.cleanup()
        await orchestrator.close()
        await _release_owned_queue(registry, queue_name, baseline)
        assert await _registry_snapshot(registry) == baseline
        await registry.aclose()


def _task_id() -> str:
    return f"task-{uuid.uuid4().hex}"


async def test_a_cold_build_indexes_a_job_that_predates_the_index(harness):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.store_metadata_only(task)
    await harness.make_job(task)

    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == [task.task_id]
    assert await harness.redis.exists(ready_key(harness.orchestrator._index_namespace))


async def test_a_cold_build_indexes_a_started_job(harness):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.store_metadata_only(task)
    await harness.make_job(task, started=True)
    await harness.redis.hset(Job.key_for(task.task_id), "status", "started")

    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == [task.task_id]
    assert [item.task_status for item in page.items] == [TaskStatus.STARTED]


async def test_a_ready_page_neither_fetches_a_job_nor_reads_the_metadata(
    harness, monkeypatch
):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.publish(task)
    await harness.redis.delete(harness.orchestrator._task_metadata_key(task.task_id))

    def _fail_fetch(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("a ready page must not fetch a job")

    monkeypatch.setattr(rq_orchestrator_module.Job, "fetch", _fail_fetch)

    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == [task.task_id]


async def test_a_ready_page_reads_only_its_own_window(harness):
    tasks = [
        _make_task(_task_id(), tenant="tenant-a", created_at=_at(index))
        for index in range(50)
    ]
    for task in tasks:
        await harness.publish(task)
    # The first listing builds the index; the measurement is taken on the
    # ready path.
    await harness.orchestrator.list_tasks(tenant_id="tenant-a")

    counting = _CountingRedis(harness.redis)
    harness.orchestrator._async_redis_conn = counting
    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a", limit=5)

    assert len(page.items) == 5
    assert page.has_more is True
    # One window of the index and one compact record plus one native status
    # per member - never a read per task of the tenant.
    assert counting.commands.count("zrange") == 1
    assert counting.commands.count("hmget") == 6
    assert counting.commands.count("hget") == 6

    counting.commands.clear()
    deep = await harness.orchestrator.list_tasks(
        tenant_id="tenant-a", limit=5, offset=10
    )

    assert [item.task_id for item in deep.items] == [
        tasks[index].task_id for index in range(39, 34, -1)
    ]
    assert deep.has_more is True
    # A nonzero offset walks the compact rows before it: the truthful budget
    # is offset + limit + 1 rows, not a page-sized read.
    assert counting.commands.count("zrange") == 1
    assert counting.commands.count("hmget") == 16
    assert counting.commands.count("hget") == 16


async def test_the_growth_of_another_tenant_does_not_change_the_page_reads(
    harness,
):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.publish(task)
    # The first listing builds the index; the measurement is taken on the
    # ready path that every later listing takes.
    await harness.orchestrator.list_tasks(tenant_id="tenant-a")

    counting = _CountingRedis(harness.redis)
    harness.orchestrator._async_redis_conn = counting
    await harness.orchestrator.list_tasks(tenant_id="tenant-a")
    before = len(counting.commands)

    for index in range(20):
        other = _make_task(_task_id(), tenant="tenant-b", created_at=_at(index))
        await harness.publish(other)

    counting.commands.clear()
    await harness.orchestrator.list_tasks(tenant_id="tenant-a")

    assert len(counting.commands) == before


async def test_a_page_keeps_the_order_and_the_tie_break(harness):
    first = _task_id()
    second = _task_id()
    newest = _task_id()
    for task in (
        _make_task(first, tenant="tenant-a", created_at=_at(10)),
        _make_task(second, tenant="tenant-a", created_at=_at(10)),
        _make_task(newest, tenant="tenant-a", created_at=_at(20)),
    ):
        await harness.publish(task)

    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == [
        newest,
        *sorted([first, second]),
    ]


async def test_a_stale_member_is_removed_and_the_page_is_refilled(harness):
    stale = _make_task(_task_id(), tenant="tenant-a", created_at=_at(20))
    fresh = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    for task in (stale, fresh):
        await harness.publish(task)
    await harness.redis.delete(harness.orchestrator._task_record_key(stale.task_id))

    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == [fresh.task_id]
    members = await harness.redis.zrange(
        harness.orchestrator._tenant_index_key("tenant-a"), 0, -1
    )
    assert stale.task_id.encode() not in members


async def _add_stale_members(harness, count: int) -> list[str]:
    """Add *count* index members with no compact record, newest first."""
    index_key = harness.orchestrator._tenant_index_key("tenant-a")
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
    first = _make_task(_task_id(), tenant="tenant-a", created_at=_at(20))
    second = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    for task in (first, second):
        await harness.publish(task)
    stale = await _add_stale_members(harness, 1)
    index_key = harness.orchestrator._tenant_index_key("tenant-a")

    page = await harness.orchestrator.list_tasks(
        tenant_id="tenant-a", limit=1, offset=1
    )

    assert [item.task_id for item in page.items] == [second.task_id]
    assert page.has_more is False
    assert set(await harness.redis.zrange(index_key, 0, -1)) == {
        first.task_id.encode(),
        second.task_id.encode(),
    }
    assert stale[0].encode() not in set(await harness.redis.zrange(index_key, 0, -1))


async def test_a_page_reads_past_more_than_three_stale_members(harness):
    first = _make_task(_task_id(), tenant="tenant-a", created_at=_at(20))
    second = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    for task in (first, second):
        await harness.publish(task)
    stale = await _add_stale_members(harness, 6)
    index_key = harness.orchestrator._tenant_index_key("tenant-a")

    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a", limit=1)

    assert [item.task_id for item in page.items] == [first.task_id]
    assert page.has_more is True
    assert set(await harness.redis.zrange(index_key, 0, -1)) == {
        first.task_id.encode(),
        second.task_id.encode(),
    }
    assert stale  # all six were ahead of the live rows


async def test_cold_projection_does_not_overwrite_a_concurrent_terminal_store(harness):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.publish(task, native_status="started", started=True)
    # The native job is still started even after the terminal metadata write,
    # which is exactly the state that makes the stale projection look active.
    await harness.redis.hset(Job.key_for(task.task_id), "status", "started")
    orchestrator = harness.orchestrator
    real_redis = orchestrator._async_redis_conn
    success = _make_task(task.task_id, tenant="tenant-a", created_at=_at(10))
    success.set_status(TaskStatus.SUCCESS)

    async def _store_terminal() -> None:
        await orchestrator._store_task_in_redis(success)

    racing = _InterleavingRedis(
        real_redis,
        trigger_commands={"hset", "zadd"},
        callback=_store_terminal,
    )
    orchestrator._async_redis_conn = racing  # type: ignore[assignment]
    try:
        page = await orchestrator.list_tasks(tenant_id="tenant-a")
    finally:
        orchestrator._async_redis_conn = real_redis  # type: ignore[assignment]

    assert racing.fired is True
    assert page.items == []
    # The terminal metadata write stays authoritative even though the cold
    # snapshot saw the older pending row.
    stored = await orchestrator._get_listing_task_from_redis(task.task_id)
    assert stored is not None
    assert stored.task_status == TaskStatus.SUCCESS
    index_key = orchestrator._tenant_index_key("tenant-a")
    assert task.task_id.encode() not in set(await real_redis.zrange(index_key, 0, -1))
    assert await real_redis.hget(Job.key_for(task.task_id), "status") == b"started"
    assert (await orchestrator.list_tasks(tenant_id="tenant-a")).items == []


async def test_a_contended_cold_projection_falls_back_without_a_ready_marker(
    harness,
):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.store_metadata_only(task)
    await harness.make_job(task)
    orchestrator = harness.orchestrator
    real_redis = orchestrator._async_redis_conn
    metadata_key = orchestrator._task_metadata_key(task.task_id)
    payload = await real_redis.get(metadata_key)
    assert payload is not None

    async def _touch_metadata() -> None:
        # Any write to the watched primary key aborts the projection, even an
        # idempotent rewrite of the same payload.
        await real_redis.set(metadata_key, payload)

    racing = _InterleavingRedis(
        real_redis,
        trigger_commands={"hset", "zadd"},
        callback=_touch_metadata,
        once=False,
    )
    orchestrator._async_redis_conn = racing  # type: ignore[assignment]
    try:
        page = await orchestrator.list_tasks(tenant_id="tenant-a")
    finally:
        orchestrator._async_redis_conn = real_redis  # type: ignore[assignment]

    # The contention budget is bounded, readiness is not advertised and the
    # already-correct scan path answers the request.
    assert racing.fires == 3
    assert not await real_redis.exists(ready_key(orchestrator._index_namespace))
    assert [item.task_id for item in page.items] == [task.task_id]


async def test_a_failed_writer_cannot_be_masked_by_concurrent_ready_publication(
    harness,
):
    orchestrator = harness.orchestrator
    real_redis = orchestrator._async_redis_conn
    namespace = orchestrator._index_namespace
    new_task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))

    async def _failing_enqueue() -> None:
        # A string at the compact-record key makes the HSET fail at EXEC time.
        # Redis still applies the metadata SET and the ZADD, then the production
        # failure handler invalidates readiness.
        await real_redis.set(
            orchestrator._task_record_key(new_task.task_id), "wrong-type"
        )
        await harness.publish(new_task, in_queue=True)

    racing = _MarkReadyRaceRedis(real_redis, _failing_enqueue)
    orchestrator._async_redis_conn = racing  # type: ignore[assignment]
    try:
        page = await orchestrator.list_tasks(tenant_id="tenant-a")
    finally:
        orchestrator._async_redis_conn = real_redis  # type: ignore[assignment]

    assert racing.fired is True
    # The writer's invalidation wins, so the older builder cannot advertise a
    # ready index that omits the surviving primary task.
    assert not await real_redis.exists(ready_key(namespace))
    stored = await orchestrator._get_listing_task_from_redis(new_task.task_id)
    assert stored is not None
    assert stored.task_status == TaskStatus.PENDING
    assert [item.task_id for item in page.items] == [new_task.task_id]

    # Removing the exact wrong-type record lets the next build repair the index.
    await real_redis.delete(orchestrator._task_record_key(new_task.task_id))
    repaired = await orchestrator.list_tasks(tenant_id="tenant-a")
    assert [item.task_id for item in repaired.items] == [new_task.task_id]
    assert await real_redis.exists(ready_key(namespace))


async def test_a_contended_stale_cleanup_leaves_the_row_for_the_next_query(
    harness,
):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.publish(task)
    orchestrator = harness.orchestrator
    first = await orchestrator.list_tasks(tenant_id="tenant-a")
    assert [item.task_id for item in first.items] == [task.task_id]

    real_redis = orchestrator._async_redis_conn
    record_key = orchestrator._task_record_key(task.task_id)
    index_key = orchestrator._tenant_index_key("tenant-a")
    await real_redis.delete(record_key)

    async def _touch_record() -> None:
        # Keep the row unusable while changing the watched key, so every
        # cleanup attempt loses the WATCH race.
        await real_redis.hset(record_key, "probe", uuid.uuid4().hex)

    racing = _InterleavingRedis(
        real_redis,
        trigger_commands={"zrem"},
        callback=_touch_record,
        once=False,
    )
    orchestrator._async_redis_conn = racing  # type: ignore[assignment]
    try:
        await orchestrator.list_tasks(tenant_id="tenant-a")
    finally:
        orchestrator._async_redis_conn = real_redis  # type: ignore[assignment]

    assert racing.fires == 3
    assert task.task_id.encode() in set(await real_redis.zrange(index_key, 0, -1))


async def test_guarded_cleanup_keeps_a_concurrently_republished_task(harness):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.publish(task)
    orchestrator = harness.orchestrator
    first = await orchestrator.list_tasks(tenant_id="tenant-a")
    assert [item.task_id for item in first.items] == [task.task_id]

    real_redis = orchestrator._async_redis_conn
    record_key = orchestrator._task_record_key(task.task_id)
    index_key = orchestrator._tenant_index_key("tenant-a")
    # The compact row expires while the index member remains.
    await real_redis.delete(record_key)
    started = _make_task(
        task.task_id,
        tenant="tenant-a",
        created_at=_at(10),
        status=TaskStatus.STARTED,
    )

    async def _republish() -> None:
        await orchestrator._store_task_in_redis(started)

    racing = _InterleavingRedis(
        real_redis,
        trigger_commands={"zrem"},
        callback=_republish,
    )
    orchestrator._async_redis_conn = racing  # type: ignore[assignment]
    try:
        await orchestrator.list_tasks(tenant_id="tenant-a")
    finally:
        orchestrator._async_redis_conn = real_redis  # type: ignore[assignment]

    assert racing.fired is True
    # Repair must not delete the membership the concurrent store just wrote.
    assert task.task_id.encode() in set(await real_redis.zrange(index_key, 0, -1))
    second = await orchestrator.list_tasks(tenant_id="tenant-a")
    assert [item.task_id for item in second.items] == [task.task_id]
    assert second.items[0].task_status == TaskStatus.STARTED


async def test_disabled_rq_write_invalidates_ready_and_rebuild_sees_both(harness):
    old = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.publish(old)
    first = await harness.orchestrator.list_tasks(tenant_id="tenant-a")
    assert [item.task_id for item in first.items] == [old.task_id]
    namespace = harness.orchestrator._index_namespace
    assert await harness.redis.exists(ready_key(namespace))

    other_queue = f"listing-{uuid.uuid4().hex}"
    registry = redis.asyncio.Redis.from_url(REDIS_URL)
    registry_baseline = await _registry_snapshot(registry)
    other = RQOrchestrator(
        RQOrchestratorConfig(
            redis_url=REDIS_URL,
            queue_name=other_queue,
            enable_task_listing=True,
        )
    )
    other_harness = _Harness(other)
    other_task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(5))
    disabled = RQOrchestrator(
        RQOrchestratorConfig(
            redis_url=REDIS_URL,
            queue_name=harness.orchestrator.config.queue_name,
            enable_task_listing=False,
        )
    )
    try:
        await other_harness.publish(other_task)
        await other.list_tasks(tenant_id="tenant-a")
        assert await harness.redis.exists(ready_key(other._index_namespace))

        new = _make_task(_task_id(), tenant="tenant-a", created_at=_at(20))
        harness._task_ids.append(new.task_id)
        harness._tenants.add("tenant-a")
        await disabled._store_task_in_redis(new)
        disabled._rq_queue.enqueue(
            "docling_jobkit.orchestrators.rq.worker.docling_task",
            kwargs={"task_data": {}},
            job_id=new.task_id,
        )
        harness._queued.append(new.task_id)

        assert not await harness.redis.exists(ready_key(namespace))
        assert await harness.redis.exists(ready_key(other._index_namespace))

        repaired = await harness.orchestrator.list_tasks(tenant_id="tenant-a")
        assert [item.task_id for item in repaired.items] == [
            new.task_id,
            old.task_id,
        ]
        assert await harness.redis.exists(ready_key(namespace))
    finally:
        await disabled.close()
        await other_harness.cleanup()
        await other.close()
        await _release_owned_queue(registry, other_queue, registry_baseline)
        await registry.aclose()


@pytest.mark.parametrize("action", ["delete", "fetch"])
async def test_disabled_rq_cleanup_invalidates_ready(harness, action):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.publish(task, in_queue=True)
    first = await harness.orchestrator.list_tasks(tenant_id="tenant-a")
    assert [item.task_id for item in first.items] == [task.task_id]
    namespace = harness.orchestrator._index_namespace
    assert await harness.redis.exists(ready_key(namespace))

    disabled = RQOrchestrator(
        RQOrchestratorConfig(
            redis_url=REDIS_URL,
            queue_name=harness.orchestrator.config.queue_name,
            enable_task_listing=False,
        )
    )
    try:
        if action == "delete":
            await disabled.delete_task(task.task_id)
        else:
            await disabled.on_result_fetched(task.task_id)
    finally:
        await disabled.close()

    assert not await harness.redis.exists(ready_key(namespace))
    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a")
    assert page.items == []


async def test_a_never_enabled_rq_write_creates_no_index_state():
    queue_name = f"listing-{uuid.uuid4().hex}"
    manager = RQOrchestrator(
        RQOrchestratorConfig(
            redis_url=REDIS_URL,
            queue_name=queue_name,
            enable_task_listing=False,
        )
    )
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    try:
        await manager._store_task_in_redis(task)
        client = manager._async_redis_conn
        namespace = manager._index_namespace
        assert not await client.exists(ready_key(namespace), generation_key(namespace))
        assert await client.zrange(manager._tenant_index_key("tenant-a"), 0, -1) == []
    finally:
        await manager._async_redis_conn.delete(manager._task_metadata_key(task.task_id))
        await manager.close()


async def test_first_build_establishes_generation_before_enumeration(harness):
    orchestrator = harness.orchestrator
    real_redis = orchestrator._async_redis_conn
    namespace = orchestrator._index_namespace
    generation = generation_key(namespace)
    new_task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    disabled = RQOrchestrator(
        RQOrchestratorConfig(
            redis_url=REDIS_URL,
            queue_name=orchestrator.config.queue_name,
            enable_task_listing=False,
        )
    )

    async def _disabled_write() -> None:
        # The first enabled builder must already have established the fence.
        assert await real_redis.exists(generation)
        await disabled._store_task_in_redis(new_task)
        disabled._rq_queue.enqueue(
            "docling_jobkit.orchestrators.rq.worker.docling_task",
            kwargs={"task_data": {}},
            job_id=new_task.task_id,
        )
        harness._task_ids.append(new_task.task_id)
        harness._tenants.add("tenant-a")
        harness._queued.append(new_task.task_id)

    racing = _MarkReadyRaceRedis(real_redis, _disabled_write)
    orchestrator._async_redis_conn = racing  # type: ignore[assignment]
    try:
        page = await orchestrator.list_tasks(tenant_id="tenant-a")
    finally:
        orchestrator._async_redis_conn = real_redis  # type: ignore[assignment]
        await disabled.close()

    assert racing.fired is True
    assert await real_redis.exists(generation)
    assert await real_redis.get(generation) == b"1"
    assert not await real_redis.exists(ready_key(namespace))
    assert [item.task_id for item in page.items] == [new_task.task_id]

    repaired = await orchestrator.list_tasks(tenant_id="tenant-a")
    assert [item.task_id for item in repaired.items] == [new_task.task_id]
    assert await real_redis.exists(ready_key(namespace))


async def test_a_started_task_does_not_regress_to_pending(harness):
    task = _make_task(
        _task_id(), tenant="tenant-a", status=TaskStatus.STARTED, created_at=_at(10)
    )
    await harness.publish(task, native_status="queued")

    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_status for item in page.items] == [TaskStatus.STARTED]


async def test_a_started_job_is_reported_as_started(harness):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.publish(task, native_status="started")

    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_status for item in page.items] == [TaskStatus.STARTED]


async def test_a_terminal_job_is_excluded(harness):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.publish(task, native_status="finished")

    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a")

    assert page.items == []


async def test_delete_task_removes_the_bytes_keyed_index_member(harness):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.publish(task)
    record_key = harness.orchestrator._task_record_key(task.task_id)
    index_key = harness.orchestrator._tenant_index_key("tenant-a")

    # The shared async client does not decode responses, so this is the real
    # bytes-keyed shape the cleanup path has to understand.
    row = await harness.redis.hgetall(record_key)
    assert row
    assert all(isinstance(field, bytes) for field in row)

    await harness.orchestrator.delete_task(task.task_id)

    assert not await harness.redis.exists(record_key)
    assert await harness.redis.zrange(index_key, 0, -1) == []


async def test_result_fetch_removes_the_bytes_keyed_index_member(harness):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.publish(task)
    record_key = harness.orchestrator._task_record_key(task.task_id)
    index_key = harness.orchestrator._tenant_index_key("tenant-a")
    assert all(
        isinstance(field, bytes) for field in await harness.redis.hgetall(record_key)
    )

    await harness.orchestrator.on_result_fetched(task.task_id)

    assert not await harness.redis.exists(record_key)
    assert await harness.redis.zrange(index_key, 0, -1) == []


async def test_mismatched_and_missing_records_are_removed_without_exposure(harness):
    foreign = _make_task(_task_id(), tenant="tenant-b", created_at=_at(20))
    live = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.publish(foreign)
    await harness.publish(live)
    index_key = harness.orchestrator._tenant_index_key("tenant-a")
    missing = f"missing-{uuid.uuid4().hex}"
    await harness.redis.zadd(
        index_key,
        {
            foreign.task_id: index_score(_at(20)),
            missing: index_score(_at(30)),
        },
    )

    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a", limit=5)

    assert [item.task_id for item in page.items] == [live.task_id]
    assert set(await harness.redis.zrange(index_key, 0, -1)) == {live.task_id.encode()}
    # The foreign tenant still owns the task in its own index.
    foreign_index = harness.orchestrator._tenant_index_key("tenant-b")
    assert foreign.task_id.encode() in set(
        await harness.redis.zrange(foreign_index, 0, -1)
    )


async def test_a_task_of_another_tenant_is_never_listed(harness):
    task = _make_task(_task_id(), tenant="tenant-b", created_at=_at(10))
    await harness.publish(task)

    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a")

    assert page.items == []


async def test_a_failed_projection_invalidates_the_readiness(harness):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.publish(task)
    await harness.orchestrator.list_tasks(tenant_id="tenant-a")
    namespace = harness.orchestrator._index_namespace
    assert await harness.redis.exists(ready_key(namespace))

    class _FailingPipeline:
        def __init__(self, pipe: Any) -> None:
            self._pipe = pipe

        def __getattr__(self, name: str) -> Any:
            attr = getattr(self._pipe, name)
            if not callable(attr):
                return attr

            def _fail(*args: Any, **kwargs: Any) -> Any:
                raise redis.exceptions.ConnectionError("redis is down")

            return _fail

    class _FailingRedis:
        def __init__(self, client: Any) -> None:
            self._client = client

        def pipeline(self, transaction: bool = False) -> Any:
            return _FailingPipeline(self._client.pipeline(transaction=transaction))

        def __getattr__(self, name: str) -> Any:
            return getattr(self._client, name)

    harness.orchestrator._async_redis_conn = _FailingRedis(harness.redis)
    broken = _make_task(_task_id(), tenant="tenant-a", created_at=_at(20))
    await harness.orchestrator._store_task_in_redis(broken)

    assert not await harness.redis.exists(ready_key(namespace))


async def test_a_build_held_by_another_caller_falls_back_to_the_scan(harness):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.store_metadata_only(task)
    await harness.make_job(task)
    await harness.redis.set(
        build_lock_key(harness.orchestrator._index_namespace), "other-caller"
    )

    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == [task.task_id]
    assert not await harness.redis.exists(
        ready_key(harness.orchestrator._index_namespace)
    )


async def test_a_failed_build_does_not_advertise_a_partial_index(harness, monkeypatch):
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    await harness.store_metadata_only(task)
    await harness.make_job(task)

    async def _fail_chunk(chunk: list[str]) -> None:
        raise redis.exceptions.ConnectionError("redis is down")

    monkeypatch.setattr(harness.orchestrator, "_index_candidate_chunk", _fail_chunk)

    page = await harness.orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == [task.task_id]
    assert not await harness.redis.exists(
        ready_key(harness.orchestrator._index_namespace)
    )


async def test_two_queues_keep_two_indexes(harness):
    queue_name = f"listing-{uuid.uuid4().hex}"
    registry = redis.asyncio.Redis.from_url(REDIS_URL)
    baseline = await _registry_snapshot(registry)
    other = RQOrchestrator(
        RQOrchestratorConfig(
            redis_url=REDIS_URL,
            queue_name=queue_name,
            enable_task_listing=True,
        )
    )
    task = _make_task(_task_id(), tenant="tenant-a", created_at=_at(10))
    try:
        await harness.publish(task)

        assert harness.orchestrator._index_namespace != other._index_namespace
        page = await other.list_tasks(tenant_id="tenant-a")
        assert page.items == []
    finally:
        await other._async_redis_conn.delete(
            ready_key(other._index_namespace),
            build_lock_key(other._index_namespace),
            generation_key(other._index_namespace),
        )
        await other.close()
        await _release_owned_queue(registry, queue_name, baseline)
        await registry.aclose()
