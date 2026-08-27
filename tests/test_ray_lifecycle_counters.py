"""Tests for the monotonic per-tenant task-lifecycle counters.

The lifecycle (queued -> dispatched -> started -> terminal) is instrumented with
cumulative counters in a ``tenant:{id}:task_counters`` Redis hash, incremented
atomically at each transition. docling-serve exposes these as Prometheus
counters so transitions that happen between two scrapes are never lost.

Ordinary counter tests use a small in-memory fake. Transaction race tests use
the real Redis service provided by CI.
"""

import asyncio
import os
import uuid

import pytest
import pytest_asyncio

pytest.importorskip("msgpack")
pytest.importorskip("redis")

from docling.datamodel.service.requests import AnyHttpSourceRequest as HttpSource
from docling.datamodel.service.targets import InBodyTarget

from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.ray.redis_helper import (
    _MARK_TASK_STARTED_LUA,
    RedisStateManager,
)


class _FakeRedis:
    """In-memory Redis modelling the commands used by the counter code paths."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.lists: dict[str, list[str]] = {}
        self.sets: dict[str, set[str]] = {}
        self.strings: dict[str, object] = {}

    # --- shared command implementation (used by direct calls and pipelines) ---
    def _apply(self, method: str, args: tuple, kwargs: dict):
        if method == "rpush":
            key, *vals = args
            self.lists.setdefault(key, []).extend(vals)
            return len(self.lists[key])
        if method == "lpop":
            lst = self.lists.get(args[0], [])
            return lst.pop(0) if lst else None
        if method == "lindex":
            key, idx = args
            lst = self.lists.get(key, [])
            return lst[idx] if -len(lst) <= idx < len(lst) else None
        if method == "sadd":
            key, *members = args
            self.sets.setdefault(key, set()).update(members)
            return len(members)
        if method == "srem":
            key, *members = args
            self.sets.setdefault(key, set()).difference_update(members)
            return len(members)
        if method == "sismember":
            key, member = args
            return member in self.sets.get(key, set())
        if method == "scard":
            return len(self.sets.get(args[0], set()))
        if method == "hset":
            key = args[0]
            h = self.hashes.setdefault(key, {})
            mapping = kwargs.get("mapping")
            if mapping is not None:
                h.update({k: str(v) for k, v in mapping.items()})
            else:
                # hset(key, f1, v1, f2, v2, ...)
                rest = args[1:]
                for i in range(0, len(rest), 2):
                    h[rest[i]] = str(rest[i + 1])
            return 1
        if method == "hget":
            v = self.hashes.get(args[0], {}).get(args[1])
            return v.encode("utf-8") if v is not None else None
        if method == "hgetall":
            return {
                k.encode("utf-8"): v.encode("utf-8")
                for k, v in self.hashes.get(args[0], {}).items()
            }
        if method == "hdel":
            h = self.hashes.get(args[0], {})
            for field in args[1:]:
                h.pop(field, None)
            return 1
        if method == "hincrby":
            key, field, amount = args
            h = self.hashes.setdefault(key, {})
            h[field] = str(int(h.get(field, "0")) + int(amount))
            return int(h[field])
        if method == "setex":
            key, _ttl, value = args
            self.strings[key] = value
            return True
        if method == "delete":
            for key in args:
                self.hashes.pop(key, None)
                self.lists.pop(key, None)
                self.sets.pop(key, None)
                self.strings.pop(key, None)
            return 1
        if method == "expire":
            return True
        raise AssertionError(f"unexpected command: {method}")

    # --- direct (awaited) commands ---
    async def lindex(self, key, idx):
        return self._apply("lindex", (key, idx), {})

    async def hget(self, key, field):
        return self._apply("hget", (key, field), {})

    async def hgetall(self, key):
        return self._apply("hgetall", (key,), {})

    async def hset(self, key, *args, **kwargs):
        return self._apply("hset", (key, *args), kwargs)

    async def scard(self, key):
        return self._apply("scard", (key,), {})

    async def eval(self, script, numkeys, *args):
        assert script == _MARK_TASK_STARTED_LUA
        keys = args[:numkeys]
        argv = args[numkeys:]
        return self._eval_mark_started(keys, argv)

    def _eval_mark_started(self, keys, argv) -> int:
        task_key, task_counters_key = keys
        status, timestamp = argv[0], argv[1]
        task = self.hashes.setdefault(task_key, {})
        cur = task.get("status")
        if cur not in ("started", "success", "failure"):
            task["status"] = status
            task["last_update_at"] = timestamp
            task["started_at"] = timestamp
            counters = self.hashes.setdefault(task_counters_key, {})
            counters["tasks_started_total"] = str(
                int(counters.get("tasks_started_total", "0")) + 1
            )
            return 1
        return 0

    def pipeline(self, transaction: bool = True):
        return _FakePipeline(self)


class _FakePipeline:
    """Transactional pipeline: buffers commands after multi(), applies on execute()."""

    def __init__(self, store: _FakeRedis) -> None:
        self._store = store
        self._buffer: list[tuple] = []
        self._multi = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self._buffer = []
        self._multi = False
        return False

    async def watch(self, *keys):
        return True

    async def unwatch(self):
        self._multi = False
        return True

    def multi(self):
        self._multi = True

    async def execute(self):
        results = [self._store._apply(m, a, kw) for (m, a, kw) in self._buffer]
        self._buffer = []
        self._multi = False
        return results

    def __getattr__(self, name):
        def cmd(*args, **kwargs):
            if self._multi:
                self._buffer.append((name, args, kwargs))
                return self
            return self._store._apply(name, args, kwargs)

        return cmd


def _manager() -> tuple[RedisStateManager, _FakeRedis]:
    manager = RedisStateManager(redis_url="redis://localhost:6379/")
    fake = _FakeRedis()
    manager.redis = fake  # type: ignore[assignment]
    return manager, fake


def _task(task_id: str, n_sources: int = 1) -> Task:
    return Task(
        task_id=task_id,
        target=InBodyTarget(),
        sources=[
            HttpSource(url=f"https://example.com/doc-{i}.pdf") for i in range(n_sources)
        ],
    )


@pytest_asyncio.fixture
async def real_redis():
    prefix = uuid.uuid4().hex
    tenant_id = f"test-{prefix}"
    task_id = f"test-{prefix}-task"
    manager = RedisStateManager(
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0")
    )
    await manager.connect()
    redis = manager._ensure_redis()
    try:
        await redis.ping()
    except Exception as exc:
        await manager.disconnect()
        pytest.fail(f"Redis is required for transaction tests: {exc}")

    try:
        yield manager, redis, tenant_id, task_id
    finally:
        await redis.delete(
            f"tenant:{tenant_id}:active_tasks",
            f"tenant:{tenant_id}:tasks",
            f"tenant:{tenant_id}:limits",
            f"tenant:{tenant_id}:task_counters",
            f"task:{task_id}",
            f"task:{task_id}:dispatch",
            f"task:{task_id}:execution",
        )
        await manager.disconnect()


# --- get_tenant_task_counters --------------------------------------------------


@pytest.mark.asyncio
async def test_get_tenant_task_counters_defaults_when_absent() -> None:
    manager, _ = _manager()
    counters = await manager.get_tenant_task_counters("tenant-a")
    assert counters.tasks_enqueued_total == 0
    assert counters.tasks_succeeded_total == 0


@pytest.mark.asyncio
async def test_get_tenant_task_counters_parses_and_ignores_unknown_fields() -> None:
    manager, fake = _manager()
    fake.hashes["tenant:tenant-a:task_counters"] = {
        "tasks_enqueued_total": "4",
        "tasks_succeeded_total": "9",
        "some_future_field": "123",
    }
    counters = await manager.get_tenant_task_counters("tenant-a")
    assert counters.tasks_enqueued_total == 4
    assert counters.tasks_succeeded_total == 9


# --- enqueue --------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_increments_enqueued_counters(monkeypatch) -> None:
    manager, fake = _manager()

    async def _noop_limits(*a, **k):
        return None

    monkeypatch.setattr(manager, "update_tenant_limits", _noop_limits)

    await manager.enqueue_task("tenant-a", _task("t1", n_sources=3))

    assert fake.lists["tenant:tenant-a:tasks"]  # task was pushed
    counters = await manager.get_tenant_task_counters("tenant-a")
    # One task enqueued regardless of how many source specs it carries: the real
    # document count is not known until the coordinator expands the sources.
    assert counters.tasks_enqueued_total == 1


# --- dispatch -------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_increments_dispatched_counters() -> None:
    manager, fake = _manager()
    task = _task("t1")
    fake.lists["tenant:tenant-a:tasks"] = [task.model_dump_json()]

    ok = await manager.dispatch_task_atomic("tenant-a", "t1", task_size=4)

    assert ok is True
    counters = await manager.get_tenant_task_counters("tenant-a")
    assert counters.tasks_dispatched_total == 1
    assert "t1" in fake.sets["tenant:tenant-a:active_tasks"]


@pytest.mark.asyncio
async def test_dispatch_race_does_not_increment() -> None:
    manager, fake = _manager()
    # Front of queue is a different task: simulate a lost race.
    fake.lists["tenant:tenant-a:tasks"] = [_task("other").model_dump_json()]

    ok = await manager.dispatch_task_atomic("tenant-a", "t1", task_size=4)

    assert ok is False
    counters = await manager.get_tenant_task_counters("tenant-a")
    assert counters.tasks_dispatched_total == 0


# --- started --------------------------------------------------------------


@pytest.mark.asyncio
async def test_mark_task_started_is_idempotent() -> None:
    manager, _ = _manager()

    first = await manager.mark_task_started("t1", "tenant-a")
    second = await manager.mark_task_started("t1", "tenant-a")

    assert first is True
    assert second is False
    counters = await manager.get_tenant_task_counters("tenant-a")
    assert counters.tasks_started_total == 1


@pytest.mark.asyncio
async def test_mark_task_started_noop_when_already_terminal() -> None:
    manager, fake = _manager()
    fake.hashes["task:t1"] = {"status": "success"}

    transitioned = await manager.mark_task_started("t1", "tenant-a")

    assert transitioned is False
    counters = await manager.get_tenant_task_counters("tenant-a")
    assert counters.tasks_started_total == 0
    # Status must not be overwritten back to 'started'
    assert fake.hashes["task:t1"]["status"] == "success"


@pytest.mark.asyncio
async def test_mark_task_started_does_not_overwrite_failure_status() -> None:
    manager, fake = _manager()
    fake.hashes["task:t1"] = {"status": "failure"}

    transitioned = await manager.mark_task_started("t1", "tenant-a")

    assert transitioned is False
    counters = await manager.get_tenant_task_counters("tenant-a")
    assert counters.tasks_started_total == 0
    assert fake.hashes["task:t1"]["status"] == "failure"


# --- terminal -------------------------------------------------------------


@pytest.mark.asyncio
async def test_finalize_success_increments_once() -> None:
    manager, fake = _manager()
    fake.hashes["task:t1"] = {"status": "started"}
    fake.sets["tenant:tenant-a:active_tasks"] = {"t1"}

    result = await manager._finalize_task_terminal_state_atomic(
        tenant_id="tenant-a",
        task_id="t1",
        task_size=4,
        terminal_status=TaskStatus.SUCCESS,
    )

    assert result.status_changed is True
    counters = await manager.get_tenant_task_counters("tenant-a")
    assert counters.tasks_succeeded_total == 1
    assert counters.tasks_failed_total == 0


@pytest.mark.asyncio
async def test_finalize_is_exactly_once_first_terminal_wins() -> None:
    manager, fake = _manager()
    fake.hashes["task:t1"] = {"status": "started"}
    fake.sets["tenant:tenant-a:active_tasks"] = {"t1"}

    await manager._finalize_task_terminal_state_atomic(
        tenant_id="tenant-a",
        task_id="t1",
        task_size=4,
        terminal_status=TaskStatus.SUCCESS,
    )
    # A duplicate finalize (e.g. reconciliation) must not double-count.
    second = await manager._finalize_task_terminal_state_atomic(
        tenant_id="tenant-a",
        task_id="t1",
        task_size=4,
        terminal_status=TaskStatus.FAILURE,
        error_message="late",
    )

    assert second.status_changed is False
    counters = await manager.get_tenant_task_counters("tenant-a")
    assert counters.tasks_succeeded_total == 1
    assert counters.tasks_failed_total == 0


# --- bookkeeping race between enqueue and terminalization ----------------------


@pytest.mark.asyncio
async def test_resync_retries_after_concurrent_finalize(
    real_redis, monkeypatch
) -> None:
    manager, redis, tenant_id, task_id = real_redis
    active_key = f"tenant:{tenant_id}:active_tasks"
    limits_key = f"tenant:{tenant_id}:limits"
    manager.max_concurrent_tasks = 1

    await redis.hset(
        limits_key,
        mapping={
            "max_concurrent_tasks": "1",
            "active_tasks": "1",
            "queued_tasks": "0",
        },
    )
    await manager.update_tenant_limits(tenant_id, delta_queued_tasks=1)
    assert await redis.hmget(limits_key, "active_tasks", "queued_tasks") == [b"1", b"1"]
    await redis.sadd(active_key, task_id)

    attempts = 0
    get_ids = manager.get_tenant_active_task_ids

    async def finalize_after_read(current_tenant_id):
        nonlocal attempts
        members = await get_ids(current_tenant_id)
        attempts += 1
        if attempts == 1:
            await manager._finalize_task_terminal_state_atomic(
                tenant_id=tenant_id,
                task_id=task_id,
                task_size=1,
                terminal_status=TaskStatus.SUCCESS,
            )
        return members

    monkeypatch.setattr(manager, "get_tenant_active_task_ids", finalize_after_read)

    await manager.resync_tenant_limits(tenant_id)

    assert attempts == 2
    assert await redis.scard(active_key) == 0
    assert int(await redis.hget(limits_key, "active_tasks")) == 0
    can_process, reason = await manager.check_tenant_can_process(tenant_id, 1)
    assert can_process, reason


@pytest.mark.asyncio
async def test_resync_defers_after_three_conflicts(
    real_redis, monkeypatch, caplog
) -> None:
    manager, redis, tenant_id, _ = real_redis
    limits_key = f"tenant:{tenant_id}:limits"
    await redis.hset(
        limits_key, mapping={"max_concurrent_tasks": "5", "active_tasks": "9"}
    )

    attempts = 0
    get_ids = manager.get_tenant_active_task_ids

    async def contend_after_read(current_tenant_id):
        nonlocal attempts
        members = await get_ids(current_tenant_id)
        attempts += 1
        await redis.hset(limits_key, "contention", attempts)
        return members

    with monkeypatch.context() as patch:
        patch.setattr(manager, "get_tenant_active_task_ids", contend_after_read)
        limits = await asyncio.wait_for(
            manager.resync_tenant_limits(tenant_id), timeout=1.0
        )

    assert attempts == 3
    assert limits.active_tasks == 9
    assert await redis.hget(limits_key, "active_tasks") == b"9"
    assert "Tenant limits resync deferred after repeated contention" in caplog.text

    assert (await manager.resync_tenant_limits(tenant_id)).active_tasks == 0
