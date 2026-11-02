# SPDX-License-Identifier: MIT

"""Tests for the tenant-scoped active-task listing shared by all backends.

Every backend is exercised without a live service: the Local backend runs
against its real process-local bookkeeping, while RQ and Ray run against small
in-memory doubles that stand in for Redis/RQ. That keeps the listing suite
hermetic while still driving each backend's own enumeration path.
"""

import datetime
import json
from typing import Any, Optional

import pytest
import redis.exceptions
from rq.job import Job

from docling.datamodel.service.tasks import TaskType

from docling_jobkit.datamodel.task import Task, validate_task
from docling_jobkit.datamodel.task_listing import (
    ACTIVE_TASK_STATUSES,
    LISTING_CHUNK_SIZE,
    ActiveTaskItem,
    CompactTask,
    build_active_task_page,
    resolve_tenant_id,
    tenant_of,
)
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.base_orchestrator import BaseOrchestrator
from docling_jobkit.orchestrators.local.orchestrator import (
    LocalOrchestrator,
    LocalOrchestratorConfig,
)
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.orchestrator import RayOrchestrator
from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager
from docling_jobkit.orchestrators.redis_task_index import index_score, ready_key
from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestrator,
    RQOrchestratorConfig,
    _decode_stored_task,
)

_T0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)


def _at(seconds: int) -> datetime.datetime:
    return _T0 + datetime.timedelta(seconds=seconds)


def _make_task(
    task_id: str,
    *,
    tenant: Optional[str] = "tenant-a",
    status: TaskStatus = TaskStatus.PENDING,
    created_at: Optional[datetime.datetime] = None,
    task_type: TaskType = TaskType.CONVERT,
) -> Task:
    """Build a tracked task without any sources, targets or options."""
    metadata: dict[str, Any] = {} if tenant is None else {"tenant_id": tenant}
    task = validate_task(
        {
            "task_id": task_id,
            "task_type": task_type,
            "task_status": status,
            "metadata": metadata,
        }
    )
    task.created_at = created_at or _T0
    return task


def _item(
    task_id: str,
    *,
    created_at: Optional[datetime.datetime] = None,
    status: TaskStatus = TaskStatus.PENDING,
) -> ActiveTaskItem:
    return ActiveTaskItem(
        task_id=task_id,
        task_type=TaskType.CONVERT,
        task_status=status,
        created_at=created_at or _T0,
    )


def _apply_index_script(
    strings: dict[str, str], script: str, numkeys: int, *args: Any
) -> int:
    """Model the index generation scripts used by the listing helpers."""
    if numkeys == 1:
        if len(args) == 1:
            key = args[0]
            if strings.get(key) is None:
                strings[key] = "0"
            return strings[key]
        key, token = args[0], args[1]
        if strings.get(key) == token:
            del strings[key]
            return 1
        return 0
    generation_key, ready_key = args[0], args[1]
    if "EXISTS" in script:
        if strings.get(generation_key) is None and strings.get(ready_key) is None:
            return 0
        generation = int(strings.get(generation_key, "0")) + 1
        strings[generation_key] = str(generation)
        strings.pop(ready_key, None)
        return 1
    if "INCR" in script:
        generation = int(strings.get(generation_key, "0")) + 1
        strings[generation_key] = str(generation)
        strings.pop(ready_key, None)
        return generation
    if len(args) == 3:
        expected = args[2]
        if strings.get(generation_key, "0") != expected:
            return 0
        strings[ready_key] = expected
        return 1
    ready = strings.get(ready_key)
    if ready is None:
        return 0
    return int(ready == strings.get(generation_key, "0"))


# ---------------------------------------------------------------------------
# Shared paging contract
# ---------------------------------------------------------------------------


def test_page_orders_newest_first_and_breaks_ties_by_task_id():
    page = build_active_task_page(
        [
            _item("b", created_at=_at(10)),
            _item("a", created_at=_at(10)),
            _item("c", created_at=_at(30)),
        ],
        limit=10,
        offset=0,
    )

    assert [item.task_id for item in page.items] == ["c", "a", "b"]
    assert page.has_more is False


def test_page_deduplicates_a_task_reported_by_two_sources():
    page = build_active_task_page(
        [_item("a", created_at=_at(10)), _item("a", created_at=_at(10))],
        limit=10,
        offset=0,
    )

    assert [item.task_id for item in page.items] == ["a"]


def test_page_treats_naive_timestamps_as_utc_instead_of_raising():
    aware = _item("aware", created_at=_at(10))
    naive = _item("naive", created_at=datetime.datetime(2026, 1, 1, 0, 0, 10))

    page = build_active_task_page([naive, aware], limit=10, offset=0)

    assert [item.task_id for item in page.items] == ["aware", "naive"]


@pytest.mark.parametrize("limit, offset", [(0, 0), (101, 0), (-1, 0), (20, -1)])
def test_page_rejects_window_outside_the_supported_bounds(limit, offset):
    with pytest.raises(ValueError):
        build_active_task_page([_item("a")], limit=limit, offset=offset)


def test_page_beyond_the_last_entry_is_empty_and_final():
    page = build_active_task_page([_item("a")], limit=20, offset=50)

    assert page.items == []
    assert page.has_more is False


def test_compact_item_carries_no_payload_options_or_results():
    dumped = _item("a").model_dump()

    assert set(dumped) == {"task_id", "task_type", "task_status", "created_at"}


def test_compact_record_decodes_an_undecoded_redis_hash():
    record = CompactTask(
        task_id="a1",
        task_type=TaskType.CONVERT,
        task_status=TaskStatus.PENDING,
        created_at=_at(10),
        tenant_id="tenant-a",
    )
    raw = {
        field.encode("utf-8"): value.encode("utf-8")
        for field, value in record.to_redis_mapping().items()
    }

    assert CompactTask.from_redis_mapping(raw) == record

    raw.pop(b"tenant_id")
    assert CompactTask.from_redis_mapping(raw) is None


def test_tenant_helpers_treat_missing_and_empty_tenant_as_default():
    assert resolve_tenant_id(None) == "default"
    assert resolve_tenant_id("") == "default"
    assert tenant_of({}) == "default"
    assert tenant_of({"tenant_id": ""}) == "default"
    assert tenant_of({"tenant_id": "tenant-a"}) == "tenant-a"


# ---------------------------------------------------------------------------
# Local backend
# ---------------------------------------------------------------------------


def _local_orchestrator(tmp_path) -> LocalOrchestrator:
    return LocalOrchestrator(
        config=LocalOrchestratorConfig(scratch_dir=tmp_path, enable_task_listing=True),
        converter_manager=None,  # type: ignore[arg-type]  # never used by listing
    )


async def test_local_listing_is_disabled_by_default(tmp_path):
    orchestrator = LocalOrchestrator(
        config=LocalOrchestratorConfig(scratch_dir=tmp_path),
        converter_manager=None,  # type: ignore[arg-type]  # never used by listing
    )

    with pytest.raises(NotImplementedError):
        await orchestrator.list_tasks(tenant_id="tenant-a")


async def test_local_lists_only_the_tenant_active_tasks(tmp_path):
    orchestrator = _local_orchestrator(tmp_path)
    orchestrator.tasks = {
        "a1": _make_task("a1", tenant="tenant-a", created_at=_at(10)),
        "a2": _make_task(
            "a2", tenant="tenant-a", status=TaskStatus.STARTED, created_at=_at(20)
        ),
        "a3": _make_task("a3", tenant="tenant-a", status=TaskStatus.SUCCESS),
        "a4": _make_task("a4", tenant="tenant-a", status=TaskStatus.FAILURE),
        "b1": _make_task("b1", tenant="tenant-b", created_at=_at(30)),
    }

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a2", "a1"]
    assert [item.task_status for item in page.items] == [
        TaskStatus.STARTED,
        TaskStatus.PENDING,
    ]
    assert page.has_more is False


async def test_local_resolves_default_tenant_from_task_metadata(tmp_path):
    orchestrator = _local_orchestrator(tmp_path)
    orchestrator.tasks = {
        "no-metadata": _make_task("no-metadata", tenant=None),
        "empty-tenant": _make_task("empty-tenant", tenant=""),
        "named": _make_task("named", tenant="tenant-a"),
    }

    default_page = await orchestrator.list_tasks(tenant_id="default")
    empty_request = await orchestrator.list_tasks(tenant_id="")

    assert {item.task_id for item in default_page.items} == {
        "no-metadata",
        "empty-tenant",
    }
    assert {item.task_id for item in empty_request.items} == {
        "no-metadata",
        "empty-tenant",
    }


async def test_local_filters_by_tenant_before_paging(tmp_path):
    orchestrator = _local_orchestrator(tmp_path)
    orchestrator.tasks = {
        "a1": _make_task("a1", tenant="tenant-a", created_at=_at(10)),
        "a2": _make_task("a2", tenant="tenant-a", created_at=_at(20)),
        "b1": _make_task("b1", tenant="tenant-b", created_at=_at(30)),
        "b2": _make_task("b2", tenant="tenant-b", created_at=_at(40)),
    }

    first = await orchestrator.list_tasks(tenant_id="tenant-a", limit=1)
    second = await orchestrator.list_tasks(tenant_id="tenant-a", limit=1, offset=1)
    drained = await orchestrator.list_tasks(tenant_id="tenant-a", limit=1, offset=2)

    assert [item.task_id for item in first.items] == ["a2"]
    assert first.has_more is True
    assert [item.task_id for item in second.items] == ["a1"]
    assert second.has_more is False
    assert drained.items == []
    assert drained.has_more is False


async def test_local_page_is_empty_for_a_peer_process(tmp_path):
    peer = _local_orchestrator(tmp_path)
    orchestrator = _local_orchestrator(tmp_path)
    orchestrator.tasks = {"a1": _make_task("a1", tenant="tenant-a")}

    page = await peer.list_tasks(tenant_id="tenant-a")

    assert page.items == []
    assert page.has_more is False


async def test_local_rejects_window_outside_the_supported_bounds(tmp_path):
    orchestrator = _local_orchestrator(tmp_path)

    with pytest.raises(ValueError):
        await orchestrator.list_tasks(tenant_id="tenant-a", limit=0)


async def test_listing_stays_non_abstract_for_external_subclasses():
    class ExternalOrchestrator(BaseOrchestrator):
        async def enqueue(self, *args, **kwargs):  # pragma: no cover - not called
            raise NotImplementedError

        async def queue_size(self) -> int:
            return 0

        async def get_queue_position(self, task_id: str):
            return None

        async def process_queue(self):
            raise NotImplementedError

        async def warm_up_caches(self):
            raise NotImplementedError

        async def clear_converters(self):
            raise NotImplementedError

        async def check_connection(self):
            raise NotImplementedError

        async def task_result(self, task_id: str):
            return None

    # Instantiating must not fail on a new abstract member, and an engine that
    # does not implement listing must say so instead of reporting no tasks.
    orchestrator = ExternalOrchestrator()
    with pytest.raises(NotImplementedError):
        await orchestrator.list_tasks(tenant_id="tenant-a")


# ---------------------------------------------------------------------------
# RQ backend
# ---------------------------------------------------------------------------


class _FakeIndexPipeline:
    """Collects the commands of a pipeline and replays them on the store."""

    def __init__(self, store: "_FakeIndexRedis") -> None:
        self._store = store
        self._queued: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self._watching = False

    async def __aenter__(self) -> "_FakeIndexPipeline":
        return self

    async def __aexit__(self, *exc_info: Any) -> bool:
        self._queued.clear()
        self._watching = False
        return False

    def _queue(self, name: str, *args: Any, **kwargs: Any) -> "_FakeIndexPipeline":
        self._queued.append((name, args, kwargs))
        return self

    def _read(self, name: str, *args: Any, **kwargs: Any) -> Any:
        # A watched pipeline returns command results immediately instead of
        # queueing them for EXEC.
        if not self._watching:
            return self._queue(name, *args, **kwargs)

        async def _immediate() -> Any:
            return self._store._apply(name, *args, **kwargs)

        return _immediate()

    async def watch(self, *keys: str) -> None:
        del keys
        self._watching = True
        self._queued.clear()

    def multi(self) -> "_FakeIndexPipeline":
        self._watching = False
        return self

    async def reset(self) -> None:
        self._watching = False
        self._queued.clear()

    def get(self, key: str) -> Any:
        return self._read("get", key)

    def set(
        self, key: str, value: str, ex: Optional[int] = None
    ) -> "_FakeIndexPipeline":
        return self._queue("set", key, value, ex=ex, nx=False)

    def hget(self, key: str, field: str) -> Any:
        return self._read("hget", key, field)

    def hmget(self, key: str, *fields: str) -> Any:
        return self._read("hmget", key, *fields)

    def hgetall(self, key: str) -> "_FakeIndexPipeline":
        return self._queue("hgetall", key)

    def hset(
        self, key: str, mapping: Optional[dict[str, str]] = None
    ) -> "_FakeIndexPipeline":
        return self._queue("hset", key, mapping)

    def expire(self, key: str, ttl: int) -> "_FakeIndexPipeline":
        return self._queue("expire", key, ttl)

    def zadd(self, key: str, mapping: dict[str, int]) -> "_FakeIndexPipeline":
        return self._queue("zadd", key, mapping)

    def zrem(self, key: str, *members: str) -> "_FakeIndexPipeline":
        return self._queue("zrem", key, *members)

    def delete(self, *keys: str) -> "_FakeIndexPipeline":
        return self._queue("delete", *keys)

    def eval(self, script: str, numkeys: int, *args: Any) -> "_FakeIndexPipeline":
        return self._queue("eval", script, numkeys, *args)

    async def execute(self) -> list[Any]:
        return [
            self._store._apply(name, *args, **kwargs)
            for name, args, kwargs in self._queued
        ]


class _FakeIndexRedis:
    """In-memory Redis double covering the commands the RQ listing uses."""

    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.sorted_sets: dict[str, dict[str, int]] = {}
        self.commands: list[str] = []
        self.failure: Optional[Exception] = None
        self.failure_command: Optional[str] = None

    # -- command implementations ---------------------------------------
    def _do_get(self, key: str) -> Optional[str]:
        return self.strings.get(key)

    def _do_set(
        self, key: str, value: str, ex: Optional[int] = None, nx: bool = False
    ) -> bool:
        if nx and key in self.strings:
            return False
        self.strings[key] = value
        return True

    def _do_exists(self, key: str) -> int:
        return int(key in self.strings)

    def _do_delete(self, *keys: str) -> int:
        removed = 0
        for key in keys:
            removed += int(self.strings.pop(key, None) is not None)
            removed += int(self.hashes.pop(key, None) is not None)
        return removed

    def _do_hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    def _do_hget(self, key: str, field: str) -> Optional[str]:
        return self.hashes.get(key, {}).get(field)

    def _do_hmget(self, key: str, *fields: str) -> list[Optional[str]]:
        row = self.hashes.get(key, {})
        return [row.get(field) for field in fields]

    def _do_hset(self, key: str, mapping: Optional[dict[str, str]] = None) -> int:
        row = self.hashes.setdefault(key, {})
        row.update(mapping or {})
        return len(mapping or {})

    def _do_expire(self, key: str, ttl: int) -> int:
        return 1

    def _do_zadd(self, key: str, mapping: dict[str, int]) -> int:
        members = self.sorted_sets.setdefault(key, {})
        for member, score in mapping.items():
            members[member] = score
        return len(mapping)

    def _do_zrem(self, key: str, *members: str) -> int:
        existing = self.sorted_sets.get(key, {})
        removed = 0
        for member in members:
            removed += int(existing.pop(member, None) is not None)
        return removed

    def _do_zrange(self, key: str, start: int, end: int) -> list[str]:
        members = self.sorted_sets.get(key, {})
        ordered = sorted(members.items(), key=lambda item: (item[1], item[0]))
        window = ordered[start : end + 1] if end >= 0 else ordered[start:]
        return [member for member, _ in window]

    def _do_eval(self, script: str, numkeys: int, *args: Any) -> int:
        return _apply_index_script(self.strings, script, numkeys, *args)

    def _apply(self, name: str, *args: Any, **kwargs: Any) -> Any:
        self.commands.append(name)
        if self.failure is not None:
            raise self.failure
        if self.failure_command == name:
            raise redis.exceptions.ConnectionError(f"{name} is down")
        return getattr(self, f"_do_{name}")(*args, **kwargs)

    # -- public commands ------------------------------------------------
    async def get(self, key: str) -> Optional[str]:
        return self._apply("get", key)

    async def set(
        self, key: str, value: str, ex: Optional[int] = None, nx: bool = False
    ) -> bool:
        return self._apply("set", key, value, ex=ex, nx=nx)

    async def exists(self, key: str) -> int:
        return self._apply("exists", key)

    async def delete(self, *keys: str) -> int:
        return self._apply("delete", *keys)

    async def hgetall(self, key: str) -> dict[str, str]:
        return self._apply("hgetall", key)

    async def hget(self, key: str, field: str) -> Optional[str]:
        return self._apply("hget", key, field)

    async def hmget(self, key: str, *fields: str) -> list[Optional[str]]:
        return self._apply("hmget", key, *fields)

    async def hset(self, key: str, mapping: Optional[dict[str, str]] = None) -> int:
        return self._apply("hset", key, mapping)

    async def expire(self, key: str, ttl: int) -> int:
        return self._apply("expire", key, ttl)

    async def zadd(self, key: str, mapping: dict[str, int]) -> int:
        return self._apply("zadd", key, mapping)

    async def zrem(self, key: str, *members: str) -> int:
        return self._apply("zrem", key, *members)

    async def zrange(self, key: str, start: int, end: int) -> list[str]:
        return self._apply("zrange", key, start, end)

    async def eval(self, script: str, numkeys: int, *args: Any) -> int:
        return self._apply("eval", script, numkeys, *args)

    def pipeline(self, transaction: bool = False) -> _FakeIndexPipeline:
        return _FakeIndexPipeline(self)

    # -- seeding helpers ------------------------------------------------
    def seed_indexed_task(
        self,
        orchestrator: RQOrchestrator,
        task: Task,
        *,
        native_status: Optional[str] = "queued",
    ) -> None:
        """Publish a task into the index the way a producer would.

        A native status of ``None`` publishes the index entry without the job,
        which is how an expired job looks to a listing.
        """
        record = CompactTask.from_task(task)
        self.hashes[orchestrator._task_record_key(task.task_id)] = (
            record.to_redis_mapping()
        )
        self.sorted_sets.setdefault(
            orchestrator._tenant_index_key(record.tenant_id), {}
        )[task.task_id] = index_score(task.created_at)
        if native_status is not None:
            self.hashes.setdefault(Job.key_for(task.task_id), {})["status"] = (
                native_status
            )

    def seed_index_ready(self, orchestrator: RQOrchestrator) -> None:
        """Mark the index as built, as a completed cold build would."""
        self.strings[ready_key(orchestrator._index_namespace)] = "0"


def _rq_orchestrator(
    *,
    tasks: Optional[list[Task]] = None,
    native_statuses: Optional[dict[str, str]] = None,
    ready: bool = True,
) -> tuple[RQOrchestrator, _FakeIndexRedis]:
    """Build an RQ orchestrator whose Redis reads and writes are in-memory."""
    orchestrator = RQOrchestrator(
        RQOrchestratorConfig(redis_url="redis://127.0.0.1:1/", enable_task_listing=True)
    )
    store = _FakeIndexRedis()
    orchestrator._async_redis_conn = store  # type: ignore[assignment]
    statuses = native_statuses or {}
    for task in tasks or []:
        store.seed_indexed_task(
            orchestrator,
            task,
            native_status=statuses.get(task.task_id, "queued"),
        )
    if ready:
        store.seed_index_ready(orchestrator)
    return orchestrator, store


async def test_rq_listing_is_disabled_by_default():
    orchestrator = RQOrchestrator(
        RQOrchestratorConfig(redis_url="redis://127.0.0.1:1/")
    )

    with pytest.raises(NotImplementedError):
        await orchestrator.list_tasks(tenant_id="tenant-a")


async def test_rq_lists_queued_and_started_jobs_of_the_tenant():
    orchestrator, _ = _rq_orchestrator(
        tasks=[
            _make_task("a1", tenant="tenant-a", created_at=_at(10)),
            _make_task("a2", tenant="tenant-a", created_at=_at(20)),
            _make_task("b1", tenant="tenant-b", created_at=_at(30)),
        ],
        native_statuses={"a1": "queued", "a2": "started", "b1": "queued"},
    )

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a2", "a1"]
    assert [item.task_status for item in page.items] == [
        TaskStatus.STARTED,
        TaskStatus.PENDING,
    ]


async def test_rq_lists_shared_state_with_an_empty_local_task_cache():
    orchestrator, _ = _rq_orchestrator(
        tasks=[_make_task("a1", tenant="tenant-a")],
    )

    assert orchestrator.tasks == {}
    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a1"]


async def test_rq_reports_a_task_reported_by_both_sources_once():
    orchestrator, _ = _rq_orchestrator(
        tasks=[_make_task("a1", tenant="tenant-a", created_at=_at(10))],
    )

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a1"]


async def test_rq_omits_terminal_jobs_and_expired_entries():
    orchestrator, _ = _rq_orchestrator(
        tasks=[
            _make_task("a-finished", tenant="tenant-a"),
            _make_task("a-failed", tenant="tenant-a"),
            _make_task("a-expired", tenant="tenant-a"),
        ],
        native_statuses={
            "a-finished": "finished",
            "a-failed": "failed",
            "a-expired": None,
        },
    )

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert page.items == []
    assert page.has_more is False


async def test_rq_drops_a_task_that_reached_a_terminal_status():
    orchestrator, store = _rq_orchestrator(
        tasks=[_make_task("a1", tenant="tenant-a", created_at=_at(10))],
        native_statuses={"a1": "started"},
    )

    before = await orchestrator.list_tasks(tenant_id="tenant-a")
    store.hashes[Job.key_for("a1")]["status"] = "finished"
    after = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in before.items] == ["a1"]
    assert after.items == []


async def test_rq_omits_metadata_already_marked_terminal():
    orchestrator, _ = _rq_orchestrator(
        tasks=[
            _make_task("a1", tenant="tenant-a", status=TaskStatus.SUCCESS),
        ],
    )

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert page.items == []


async def test_rq_maps_a_task_without_tenant_metadata_to_default():
    orchestrator, _ = _rq_orchestrator(
        tasks=[_make_task("a1", tenant=None)],
    )

    default_page = await orchestrator.list_tasks(tenant_id="default")
    named_page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in default_page.items] == ["a1"]
    assert named_page.items == []


async def test_rq_propagates_a_backend_failure():
    orchestrator, store = _rq_orchestrator(
        tasks=[_make_task("a1", tenant="tenant-a")],
    )
    store.failure = redis.exceptions.ConnectionError("redis is down")

    with pytest.raises(redis.exceptions.ConnectionError):
        await orchestrator.list_tasks(tenant_id="tenant-a")


@pytest.mark.parametrize("stage", ["members", "records", "status"])
async def test_rq_propagates_an_index_read_failure(stage):
    orchestrator, store = _rq_orchestrator(
        tasks=[_make_task("a1", tenant="tenant-a")],
    )
    store.failure_command = {"members": "zrange", "records": "hmget", "status": "hget"}[
        stage
    ]

    with pytest.raises(redis.exceptions.ConnectionError):
        await orchestrator.list_tasks(tenant_id="tenant-a")


async def test_rq_pages_after_filtering_and_validates_the_window():
    orchestrator, _ = _rq_orchestrator(
        tasks=[
            _make_task("a1", tenant="tenant-a", created_at=_at(10)),
            _make_task("b1", tenant="tenant-b", created_at=_at(40)),
            _make_task("a2", tenant="tenant-a", created_at=_at(20)),
            _make_task("a3", tenant="tenant-a", created_at=_at(30)),
        ],
    )

    first = await orchestrator.list_tasks(tenant_id="tenant-a", limit=2)
    second = await orchestrator.list_tasks(tenant_id="tenant-a", limit=2, offset=2)

    assert [item.task_id for item in first.items] == ["a3", "a2"]
    assert first.has_more is True
    assert [item.task_id for item in second.items] == ["a1"]
    assert second.has_more is False

    with pytest.raises(ValueError):
        await orchestrator.list_tasks(
            tenant_id="tenant-a", limit=LISTING_CHUNK_SIZE + 1
        )


async def test_rq_listing_reads_only_the_requested_window_of_a_deep_index():
    count = LISTING_CHUNK_SIZE * 2 + 5
    orchestrator, store = _rq_orchestrator(
        tasks=[
            _make_task(f"a{index:03}", tenant="tenant-a", created_at=_at(index))
            for index in range(count)
        ],
    )

    page = await orchestrator.list_tasks(tenant_id="tenant-a", limit=10)

    assert [item.task_id for item in page.items] == [
        f"a{index:03}" for index in range(count - 1, count - 11, -1)
    ]
    assert page.has_more is True
    # The page reads its own window and nothing else: one range command and
    # one compact record plus one native status per member.
    assert store.commands.count("zrange") == 1
    assert store.commands.count("hmget") == 11
    assert store.commands.count("hget") == 11


async def test_rq_listing_removes_a_stale_member_and_refills_the_page():
    orchestrator, store = _rq_orchestrator(
        tasks=[
            _make_task("a1", tenant="tenant-a", created_at=_at(10)),
            _make_task("a2", tenant="tenant-a", created_at=_at(20)),
        ],
    )
    # The record of the newest task expired, so the member is stale.
    store.hashes.pop(orchestrator._task_record_key("a2"))

    page = await orchestrator.list_tasks(tenant_id="tenant-a", limit=5)

    assert [item.task_id for item in page.items] == ["a1"]
    assert "a2" not in store.sorted_sets[orchestrator._tenant_index_key("tenant-a")]


@pytest.mark.parametrize(
    "record",
    [
        None,
        {"task_id": "a1"},
        {"task_id": "a1", "task_type": "convert", "task_status": "teleported"},
        {
            "task_id": "a1",
            "task_type": "convert",
            "task_status": "pending",
            "created_at": "not a date",
            "tenant_id": "tenant-a",
        },
    ],
)
async def test_rq_listing_omits_an_absent_or_corrupt_index_record(record):
    orchestrator, store = _rq_orchestrator()
    if record is not None:
        store.hashes[orchestrator._task_record_key("a1")] = record
    store.sorted_sets.setdefault(orchestrator._tenant_index_key("tenant-a"), {})[
        "a1"
    ] = index_score(_T0)

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert page.items == []
    assert page.has_more is False


async def test_rq_listing_reports_a_stored_task_with_its_tenant_and_time():
    orchestrator, store = _rq_orchestrator()
    task = _make_task("a1", tenant="tenant-a", created_at=_at(10))
    store.hashes.setdefault(Job.key_for("a1"), {})["status"] = "started"

    await orchestrator._store_task_in_redis(task)
    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a1"]
    assert page.items[0].task_status == TaskStatus.STARTED
    assert page.items[0].created_at == _at(10)
    assert page.has_more is False


async def test_rq_listing_reports_the_stored_task_type():
    orchestrator, store = _rq_orchestrator()
    task = _make_task("c1", tenant="tenant-a", task_type=TaskType.CHUNK)
    store.hashes.setdefault(Job.key_for("c1"), {})["status"] = "queued"

    await orchestrator._store_task_in_redis(task)
    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["c1"]
    assert page.items[0].task_type == TaskType.CHUNK


async def test_rq_listing_drops_a_stored_task_that_reached_a_terminal_status():
    orchestrator, store = _rq_orchestrator()
    task = _make_task("a1", tenant="tenant-a", created_at=_at(10))
    store.hashes.setdefault(Job.key_for("a1"), {})["status"] = "queued"

    await orchestrator._store_task_in_redis(task)
    task.set_status(TaskStatus.SUCCESS)
    store.hashes[Job.key_for("a1")]["status"] = "finished"
    await orchestrator._store_task_in_redis(task)
    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert page.items == []


async def test_rq_listing_drops_a_deleted_task():
    orchestrator, store = _rq_orchestrator()
    task = _make_task("a1", tenant="tenant-a", created_at=_at(10))
    store.hashes.setdefault(Job.key_for("a1"), {})["status"] = "queued"

    await orchestrator._store_task_in_redis(task)
    await orchestrator.delete_task("a1")
    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert page.items == []


# ---------------------------------------------------------------------------
# Ray backend
# ---------------------------------------------------------------------------


class _FakePipe:
    """Collects the commands of a pipeline and replays them on the store."""

    def __init__(self, store: "_FakeRedis") -> None:
        self._store = store
        self._queued: list[tuple[str, tuple[Any, ...]]] = []
        self._watching = False

    async def __aenter__(self) -> "_FakePipe":
        return self

    async def __aexit__(self, *exc_info: Any) -> bool:
        self._queued.clear()
        self._watching = False
        return False

    def _queue(self, name: str, *args: Any) -> "_FakePipe":
        self._queued.append((name, args))
        return self

    def _read(self, name: str, *args: Any) -> Any:
        # A watched pipeline returns command results immediately instead of
        # queueing them for EXEC.
        if not self._watching:
            return self._queue(name, *args)

        async def _immediate() -> Any:
            return self._store._apply(name, *args)

        return _immediate()

    async def watch(self, *keys: str) -> None:
        del keys
        self._watching = True
        self._queued.clear()

    def multi(self) -> "_FakePipe":
        self._watching = False
        return self

    async def reset(self) -> None:
        self._watching = False
        self._queued.clear()

    def hgetall(self, key: str) -> "_FakePipe":
        return self._queue("hgetall", key)

    def hmget(self, key: str, *fields: str) -> Any:
        return self._read("hmget", key, *fields)

    def zadd(self, key: str, mapping: dict[str, int]) -> "_FakePipe":
        return self._queue("zadd", key, mapping)

    def zrem(self, key: str, *members: str) -> "_FakePipe":
        return self._queue("zrem", key, *members)

    def eval(self, script: str, numkeys: int, *args: Any) -> "_FakePipe":
        return self._queue("eval", script, numkeys, *args)

    async def execute(self) -> list[Any]:
        self._store.pipe_batches.append(len(self._queued))
        return [self._store._apply(name, *args) for name, args in self._queued]


class _FakeRedis:
    """Redis double covering only the commands a listing performs."""

    def __init__(self) -> None:
        self.lists: dict[str, list[bytes]] = {}
        self.sets: dict[str, set[bytes]] = {}
        self.hashes: dict[str, dict[bytes, bytes]] = {}
        self.sorted_sets: dict[str, dict[str, int]] = {}
        self.strings: dict[str, str] = {}
        self.touched: list[str] = []
        self.lrange_calls: list[tuple[int, int]] = []
        self.pipe_batches: list[int] = []
        self.commands: list[str] = []
        self.failure: Optional[Exception] = None
        self.failure_command: Optional[str] = None

    def pipeline(self, transaction: bool = False) -> _FakePipe:
        return _FakePipe(self)

    # --- command implementations ---------------------------------------
    def _do_hgetall(self, key: str) -> dict[bytes, bytes]:
        return self.hashes.get(key, {})

    def _do_hmget(self, key: str, *fields: str) -> list[Optional[bytes]]:
        row = self.hashes.get(key, {})
        return [row.get(field.encode("utf-8")) for field in fields]

    def _do_zadd(self, key: str, mapping: dict[str, int]) -> int:
        members = self.sorted_sets.setdefault(key, {})
        for member, score in mapping.items():
            members[member] = score
        return len(mapping)

    def _do_zrem(self, key: str, *members: str) -> int:
        existing = self.sorted_sets.get(key, {})
        removed = 0
        for member in members:
            removed += int(existing.pop(member, None) is not None)
        return removed

    def _do_zrange(self, key: str, start: int, end: int) -> list[bytes]:
        members = self.sorted_sets.get(key, {})
        ordered = sorted(members.items(), key=lambda item: (item[1], item[0]))
        window = ordered[start : end + 1] if end >= 0 else ordered[start:]
        return [member.encode("utf-8") for member, _ in window]

    def _do_get(self, key: str) -> Optional[str]:
        return self.strings.get(key)

    def _do_set(
        self, key: str, value: str, ex: Optional[int] = None, nx: bool = False
    ) -> bool:
        if nx and key in self.strings:
            return False
        self.strings[key] = value
        return True

    def _do_exists(self, key: str) -> int:
        return int(key in self.strings)

    def _do_delete(self, *keys: str) -> int:
        return sum(int(self.strings.pop(key, None) is not None) for key in keys)

    def _do_eval(self, script: str, numkeys: int, *args: Any) -> int:
        return _apply_index_script(self.strings, script, numkeys, *args)

    def _apply(self, name: str, *args: Any, **kwargs: Any) -> Any:
        self.commands.append(name)
        if self.failure is not None:
            raise self.failure
        if self.failure_command == name:
            raise redis.exceptions.ConnectionError(f"{name} is down")
        return getattr(self, f"_do_{name}")(*args, **kwargs)

    # --- public commands ------------------------------------------------
    def _check(self) -> None:
        if self.failure is not None:
            raise self.failure

    async def lrange(self, key: str, start: int, end: int) -> list[bytes]:
        self._check()
        self.touched.append(key)
        self.lrange_calls.append((start, end))
        rows = self.lists.get(key, [])
        return rows[start : end + 1] if end >= 0 else rows[start:]

    async def smembers(self, key: str) -> set[bytes]:
        self._check()
        self.touched.append(key)
        return self.sets.get(key, set())

    def hgetall(self, key: str) -> dict[bytes, bytes]:
        self._check()
        return self.hashes.get(key, {})

    async def get(self, key: str) -> Optional[str]:
        return self._apply("get", key)

    async def zrange(self, key: str, start: int, end: int) -> list[bytes]:
        return self._apply("zrange", key, start, end)

    async def zadd(self, key: str, mapping: dict[str, int]) -> int:
        return self._apply("zadd", key, mapping)

    async def zrem(self, key: str, *members: str) -> int:
        return self._apply("zrem", key, *members)

    async def set(
        self, key: str, value: str, ex: Optional[int] = None, nx: bool = False
    ) -> bool:
        return self._apply("set", key, value, ex=ex, nx=nx)

    async def exists(self, key: str) -> int:
        return self._apply("exists", key)

    async def delete(self, *keys: str) -> int:
        return self._apply("delete", *keys)

    async def eval(self, script: str, numkeys: int, *args: Any) -> int:
        return self._apply("eval", script, numkeys, *args)

    # --- seeding helpers -------------------------------------------------
    def seed_queue(self, tenant_id: str, task_ids: list[str]) -> None:
        # Production stores full Task JSON per queue row; seed minimal but
        # valid Task JSON so the listing validation path is exercised.
        rows = [
            json.dumps(
                {
                    "task_id": task_id,
                    "task_type": "convert",
                    "task_status": "pending",
                }
            ).encode("utf-8")
            for task_id in task_ids
        ]
        self.lists[f"tenant:{tenant_id}:tasks"] = rows

    def seed_raw_queue(self, tenant_id: str, rows: list[bytes]) -> None:
        self.lists[f"tenant:{tenant_id}:tasks"] = list(rows)

    def seed_active(self, tenant_id: str, task_ids: list[str]) -> None:
        self.sets[f"tenant:{tenant_id}:active_tasks"] = {
            task_id.encode("utf-8") for task_id in task_ids
        }

    def seed_task(self, task_id: str, **fields: str) -> None:
        mapping = {
            "task_id": task_id,
            "task_type": "convert",
            "task_size": "1",
            "created_at": _at(0).isoformat(),
            "last_update_at": _at(0).isoformat(),
        }
        mapping.update(fields)
        self.hashes[f"task:{task_id}"] = {
            key.encode("utf-8"): value.encode("utf-8") for key, value in mapping.items()
        }


class _StubConverterManager:
    # Mirrors the attribute DoclingConverterManagerConfig exposes here.
    class config:
        allow_external_plugins = False


def _ray_orchestrator(store: _FakeRedis) -> RayOrchestrator:
    orchestrator = RayOrchestrator(
        config=RayOrchestratorConfig(
            redis_url="redis://127.0.0.1:1/", enable_task_listing=True
        ),
        converter_manager=_StubConverterManager(),  # type: ignore[arg-type]
    )
    manager = RedisStateManager(
        redis_url="redis://127.0.0.1:1/", enable_task_listing=True
    )
    manager.redis = store  # type: ignore[assignment]
    manager.pool = object()  # type: ignore[assignment]
    orchestrator.redis_manager = manager
    return orchestrator


def _seed_indexed_task(
    store: _FakeRedis,
    orchestrator: RayOrchestrator,
    tenant_id: str,
    task_id: str,
    *,
    status: TaskStatus = TaskStatus.PENDING,
    created_at: Optional[datetime.datetime] = None,
) -> None:
    """Publish a task into the fake listing index the way a producer would."""
    created = created_at or _at(0)
    store.seed_task(
        task_id,
        tenant_id=tenant_id,
        status=status.value,
        created_at=created.isoformat(),
    )
    store.sorted_sets.setdefault(
        orchestrator.redis_manager._task_index_key(tenant_id), {}
    )[task_id] = index_score(created)


def _seed_index_ready(
    store: _FakeRedis, orchestrator: RayOrchestrator, tenant_id: str
) -> None:
    store.strings[orchestrator.redis_manager._task_index_ready_key(tenant_id)] = "0"


async def test_ray_listing_is_disabled_by_default():
    orchestrator = RayOrchestrator(
        config=RayOrchestratorConfig(redis_url="redis://127.0.0.1:1/"),
        converter_manager=_StubConverterManager(),  # type: ignore[arg-type]
    )

    with pytest.raises(NotImplementedError):
        await orchestrator.list_tasks(tenant_id="tenant-a")


async def test_ray_lists_queued_and_active_tasks_of_the_tenant():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    _seed_indexed_task(store, orchestrator, "tenant-a", "a-queued", created_at=_at(10))
    _seed_indexed_task(
        store,
        orchestrator,
        "tenant-a",
        "a-started",
        status=TaskStatus.STARTED,
        created_at=_at(20),
    )
    _seed_index_ready(store, orchestrator, "tenant-a")

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a-started", "a-queued"]
    assert [item.task_status for item in page.items] == [
        TaskStatus.STARTED,
        TaskStatus.PENDING,
    ]


async def test_ray_reports_an_indexed_task_once():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    _seed_indexed_task(store, orchestrator, "tenant-a", "a1")
    _seed_index_ready(store, orchestrator, "tenant-a")

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a1"]


async def test_ray_omits_terminal_stale_and_corrupt_entries():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    _seed_indexed_task(store, orchestrator, "tenant-a", "a-live")
    _seed_indexed_task(
        store, orchestrator, "tenant-a", "a-done", status=TaskStatus.SUCCESS
    )
    _seed_indexed_task(store, orchestrator, "tenant-a", "a-corrupt")
    store.hashes["task:a-corrupt"][b"status"] = b"teleported"
    # a-stale is indexed but has no task hash at all.
    store.sorted_sets[orchestrator.redis_manager._task_index_key("tenant-a")][
        "a-stale"
    ] = index_score(_at(0))
    _seed_index_ready(store, orchestrator, "tenant-a")

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a-live"]
    assert set(
        store.sorted_sets[orchestrator.redis_manager._task_index_key("tenant-a")]
    ) == {"a-live"}


async def test_ray_omits_a_task_that_reached_a_terminal_status():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    _seed_indexed_task(store, orchestrator, "tenant-a", "a1", status=TaskStatus.STARTED)
    _seed_index_ready(store, orchestrator, "tenant-a")

    before = await orchestrator.list_tasks(tenant_id="tenant-a")
    store.seed_task("a1", tenant_id="tenant-a", status=TaskStatus.FAILURE.value)
    after = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in before.items] == ["a1"]
    assert after.items == []


async def test_ray_lists_the_default_tenant_queue():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    _seed_indexed_task(store, orchestrator, "default", "d1")
    _seed_index_ready(store, orchestrator, "default")

    page = await orchestrator.list_tasks(tenant_id="")

    assert [item.task_id for item in page.items] == ["d1"]


async def test_ray_ready_page_reads_only_the_index_and_its_window():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    for index in range(50):
        _seed_indexed_task(
            store, orchestrator, "tenant-a", f"a{index:02}", created_at=_at(index)
        )
    for index in range(20):
        _seed_indexed_task(
            store, orchestrator, "tenant-b", f"b{index:02}", created_at=_at(index)
        )
    _seed_index_ready(store, orchestrator, "tenant-a")

    page = await orchestrator.list_tasks(tenant_id="tenant-a", limit=5)

    assert [item.task_id for item in page.items] == [
        "a49",
        "a48",
        "a47",
        "a46",
        "a45",
    ]
    assert page.has_more is True
    # One window of the index and one compact read per member; the queued Task
    # JSON is never read on a ready page.
    assert store.commands.count("zrange") == 1
    assert store.commands.count("hmget") == 6
    assert store.lrange_calls == []
    assert not any(key.startswith("tenant:") for key in store.touched)


async def test_ray_pages_after_filtering_and_validates_the_window():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    for task_id, created in (("a1", 10), ("a2", 20), ("a3", 30)):
        _seed_indexed_task(
            store, orchestrator, "tenant-a", task_id, created_at=_at(created)
        )
    _seed_indexed_task(store, orchestrator, "tenant-b", "b1", created_at=_at(40))
    _seed_index_ready(store, orchestrator, "tenant-a")

    first = await orchestrator.list_tasks(tenant_id="tenant-a", limit=2)
    second = await orchestrator.list_tasks(tenant_id="tenant-a", limit=2, offset=2)
    drained = await orchestrator.list_tasks(tenant_id="tenant-a", limit=2, offset=3)

    assert [item.task_id for item in first.items] == ["a3", "a2"]
    assert first.has_more is True
    assert [item.task_id for item in second.items] == ["a1"]
    assert second.has_more is False
    assert drained.items == []
    assert drained.has_more is False

    with pytest.raises(ValueError):
        await orchestrator.list_tasks(tenant_id="tenant-a", offset=-1)


@pytest.mark.parametrize("stage", ["members", "records"])
async def test_ray_propagates_an_index_read_failure(stage):
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    _seed_indexed_task(store, orchestrator, "tenant-a", "a1")
    _seed_index_ready(store, orchestrator, "tenant-a")
    store.failure_command = {"members": "zrange", "records": "hmget"}[stage]

    with pytest.raises(redis.exceptions.ConnectionError):
        await orchestrator.list_tasks(tenant_id="tenant-a")


async def test_ray_lists_shared_state_with_an_empty_local_task_cache():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    _seed_indexed_task(store, orchestrator, "tenant-a", "a1", status=TaskStatus.STARTED)
    _seed_index_ready(store, orchestrator, "tenant-a")

    assert orchestrator.tasks == {}
    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a1"]


def test_active_statuses_cover_only_non_terminal_states():
    assert ACTIVE_TASK_STATUSES == {TaskStatus.PENDING, TaskStatus.STARTED}


async def test_ray_cold_build_indexes_the_tenant_queue_and_active_set():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    store.seed_queue("tenant-a", ["a-queued"])
    store.seed_active("tenant-a", ["a-started"])
    store.seed_task(
        "a-queued",
        tenant_id="tenant-a",
        status=TaskStatus.PENDING.value,
        created_at=_at(10).isoformat(),
    )
    store.seed_task(
        "a-started",
        tenant_id="tenant-a",
        status=TaskStatus.STARTED.value,
        created_at=_at(20).isoformat(),
    )

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a-started", "a-queued"]
    assert await orchestrator.redis_manager.is_task_index_ready("tenant-a")


async def test_ray_cold_build_omits_a_corrupt_or_terminal_candidate():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    store.seed_raw_queue(
        "tenant-a",
        [
            b"not json at all",
            json.dumps(["a1"]).encode("utf-8"),
            json.dumps({"task_id": ""}).encode("utf-8"),
            json.dumps({"task_id": "a1"}).encode("utf-8"),
            json.dumps({"task_id": "a-done"}).encode("utf-8"),
        ],
    )
    store.seed_task("a1", tenant_id="tenant-a", status=TaskStatus.PENDING.value)
    store.seed_task("a-done", tenant_id="tenant-a", status=TaskStatus.SUCCESS.value)

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a1"]


async def test_rq_listing_pages_through_a_deep_index():
    ids = [f"a{index:03}" for index in range(LISTING_CHUNK_SIZE * 2 + 5)]
    orchestrator, _ = _rq_orchestrator(
        tasks=[
            _make_task(task_id, created_at=_at(index))
            for index, task_id in enumerate(ids)
        ],
    )

    page = await orchestrator.list_tasks(tenant_id="tenant-a", limit=1)
    assert [item.task_id for item in page.items] == [ids[-1]]
    assert page.has_more is True
    final = await orchestrator.list_tasks(
        tenant_id="tenant-a", limit=1, offset=len(ids) - 1
    )
    assert [item.task_id for item in final.items] == [ids[0]]
    assert final.has_more is False


@pytest.mark.parametrize("tenant", [None, ""])
async def test_ray_listing_defaults_missing_or_empty_persisted_tenant(tenant):
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    store.seed_queue("default", ["d1"])
    fields = {} if tenant is None else {"tenant_id": tenant}
    store.seed_task("d1", status=TaskStatus.PENDING.value, **fields)
    page = await orchestrator.list_tasks(tenant_id="default")
    assert [item.task_id for item in page.items] == ["d1"]


async def test_rq_listing_propagates_metadata_read_failure(monkeypatch):
    orchestrator, _ = _rq_orchestrator()
    monkeypatch.setattr(
        orchestrator,
        "_get_listing_task_from_redis",
        RQOrchestrator._get_listing_task_from_redis.__get__(orchestrator),
    )

    class FailingMetadataRedis:
        async def get(self, key):
            raise redis.exceptions.ConnectionError("metadata redis is down")

    orchestrator._async_redis_conn = FailingMetadataRedis()
    with pytest.raises(redis.exceptions.ConnectionError):
        await orchestrator._get_listing_task_from_redis("a1")


async def test_rq_listing_decodes_chunk_tasks_via_the_index():
    orchestrator, store = _rq_orchestrator(
        tasks=[
            _make_task(
                "c1", tenant="tenant-a", task_type=TaskType.CHUNK, created_at=_at(10)
            ),
            _make_task(
                "c2", tenant="tenant-a", task_type=TaskType.CHUNK, created_at=_at(20)
            ),
        ],
        native_statuses={"c1": "queued", "c2": "started"},
    )
    store.seed_index_ready(orchestrator)

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["c2", "c1"]
    assert all(item.task_type == TaskType.CHUNK for item in page.items)
    assert [item.task_status for item in page.items] == [
        TaskStatus.STARTED,
        TaskStatus.PENDING,
    ]
    assert set(page.items[0].model_dump()) == {
        "task_id",
        "task_type",
        "task_status",
        "created_at",
    }


def test_rq_decoder_handles_legacy_defaults_and_plugin_equivalence():
    data: dict[str, Any] = {
        "task_id": "a1",
        "task_type": "convert",
        "task_status": "pending",
    }
    legacy = _decode_stored_task(dict(data), allow_external_plugins=False)
    assert legacy.task_id == "a1"
    assert legacy.processing_meta is not None
    assert legacy.targets, "legacy rows must gain the default target"

    equivalent = _decode_stored_task(dict(data), allow_external_plugins=True)
    assert equivalent.model_dump(
        exclude={"created_at", "last_update_at"}
    ) == legacy.model_dump(exclude={"created_at", "last_update_at"})


@pytest.mark.parametrize(
    "loader", ["_get_task_from_redis", "_get_listing_task_from_redis"]
)
@pytest.mark.parametrize("row", [None, b"not json", b"[]", "legacy"])
async def test_rq_loaders_share_absent_corrupt_and_legacy_semantics(
    monkeypatch, loader, row
):
    orchestrator, _ = _rq_orchestrator()
    data = {
        "task_id": "a1",
        "task_type": "convert",
        "task_status": "pending",
        "created_at": _T0.isoformat(),
        "last_update_at": _T0.isoformat(),
    }
    payload = json.dumps(data).encode() if row == "legacy" else row

    class MetadataRedis:
        async def get(self, key):
            assert key == "docling:tasks:a1:metadata"
            return payload

    orchestrator._async_redis_conn = MetadataRedis()
    method = getattr(RQOrchestrator, loader).__get__(orchestrator)
    task = await method("a1")
    if row == "legacy":
        expected = _decode_stored_task(data, allow_external_plugins=False)
        assert task.model_dump() == expected.model_dump()
        assert task.targets[0].kind == "inbody"
        assert task.processing_meta.num_docs == 0
    else:
        assert task is None


async def test_rq_loaders_differ_only_on_transport_error(monkeypatch):
    orchestrator, _ = _rq_orchestrator()
    monkeypatch.setattr(
        orchestrator,
        "_get_task_from_redis",
        RQOrchestrator._get_task_from_redis.__get__(orchestrator),
    )
    monkeypatch.setattr(
        orchestrator,
        "_get_listing_task_from_redis",
        RQOrchestrator._get_listing_task_from_redis.__get__(orchestrator),
    )

    class FailingMetadataRedis:
        async def get(self, key):
            raise redis.exceptions.ConnectionError("metadata redis is down")

    orchestrator._async_redis_conn = FailingMetadataRedis()
    assert await orchestrator._get_task_from_redis("a1") is None
    with pytest.raises(redis.exceptions.ConnectionError):
        await orchestrator._get_listing_task_from_redis("a1")


@pytest.mark.parametrize("failure", [False, True])
async def test_ray_listing_handles_immediate_lrange_results(monkeypatch, failure):
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    ids = [f"a{index:03}" for index in range(LISTING_CHUNK_SIZE * 2 + 5)]
    store.seed_queue("tenant-a", ids)
    for index, task_id in enumerate(ids):
        store.seed_task(
            task_id,
            tenant_id="tenant-a",
            status=TaskStatus.PENDING.value,
            created_at=_at(index).isoformat(),
        )

    def immediate_lrange(key, start, end):
        if failure:
            raise redis.exceptions.ConnectionError("immediate LRANGE failure")
        store.touched.append(key)
        store.lrange_calls.append((start, end))
        return store.lists.get(key, [])[start : end + 1]

    monkeypatch.setattr(store, "lrange", immediate_lrange)
    if failure:
        with pytest.raises(
            redis.exceptions.ConnectionError, match="immediate LRANGE failure"
        ):
            await orchestrator.list_tasks(tenant_id="tenant-a")
        return

    page = await orchestrator.list_tasks(tenant_id="tenant-a", limit=1)
    assert [item.task_id for item in page.items] == [ids[-1]]
    assert page.has_more is True
    assert store.lrange_calls == [(0, 99), (100, 199), (200, 299)]
