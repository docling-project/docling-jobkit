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
from rq.exceptions import NoSuchJobError
from rq.job import JobStatus

from docling.datamodel.service.tasks import TaskType

from docling_jobkit.datamodel.task import Task, validate_task, validate_task_json
from docling_jobkit.datamodel.task_listing import (
    ACTIVE_TASK_STATUSES,
    LISTING_CHUNK_SIZE,
    ActiveTaskItem,
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
from docling_jobkit.orchestrators.rq import orchestrator as rq_orchestrator_module
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
        config=LocalOrchestratorConfig(scratch_dir=tmp_path),
        converter_manager=None,  # type: ignore[arg-type]  # never used by listing
    )


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


class _FakeQueue:
    def __init__(self, job_ids: list[str]) -> None:
        self.job_ids = job_ids
        self.windows: list[tuple[int, int]] = []

    def get_job_ids(self, offset: int = 0, length: int = -1) -> list[str]:
        self.windows.append((offset, length))
        if length < 0:
            return list(self.job_ids[offset:])
        return list(self.job_ids[offset : offset + length])


class _FakeStartedJobRegistry:
    def __init__(self, job_ids: list[str], **_: Any) -> None:
        self.job_ids = job_ids
        self.windows: list[tuple[int, int]] = []

    def get_job_ids(self, start: int = 0, end: int = -1, **_: Any) -> list[str]:
        self.windows.append((start, end))
        if end < 0:
            return list(self.job_ids[start:])
        return list(self.job_ids[start : end + 1])


class _FakeJob:
    def __init__(self, status: JobStatus) -> None:
        self._status = status

    def get_status(self) -> JobStatus:
        return self._status


def _rq_orchestrator(
    monkeypatch,
    *,
    queue_ids: Optional[list[str]] = None,
    started_ids: Optional[list[str]] = None,
    job_statuses: Optional[dict[str, JobStatus]] = None,
    metadata: Optional[dict[str, Task]] = None,
    fetch_error: Optional[Exception] = None,
) -> RQOrchestrator:
    """Build an RQ orchestrator whose Redis/RQ reads are in-memory doubles."""
    orchestrator = RQOrchestrator(
        RQOrchestratorConfig(redis_url="redis://127.0.0.1:1/")
    )
    orchestrator._rq_queue = _FakeQueue(list(queue_ids or []))
    registries: list[_FakeStartedJobRegistry] = []

    def _registry_factory(**kwargs: Any) -> _FakeStartedJobRegistry:
        registry = _FakeStartedJobRegistry(list(started_ids or []), **kwargs)
        registries.append(registry)
        return registry

    monkeypatch.setattr(
        rq_orchestrator_module,
        "StartedJobRegistry",
        _registry_factory,
    )
    # Exposed for bounded-read assertions in deep-source tests.
    orchestrator._test_registries = registries  # type: ignore[attr-defined]

    jobs = job_statuses if job_statuses is not None else {}

    class _JobFactory:
        @staticmethod
        def fetch(task_id: str, connection: Any = None) -> _FakeJob:
            if fetch_error is not None:
                raise fetch_error
            status = jobs.get(task_id)
            if status is None:
                raise NoSuchJobError("no such job")
            return _FakeJob(status)

    monkeypatch.setattr(rq_orchestrator_module, "Job", _JobFactory)

    stored = dict(metadata or {})

    async def _load(task_id: str) -> Optional[Task]:
        return stored.get(task_id)

    monkeypatch.setattr(orchestrator, "_get_listing_task_from_redis", _load)
    return orchestrator


async def test_rq_lists_queued_and_started_jobs_of_the_tenant(monkeypatch):
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=["a1", "b1"],
        started_ids=["a2"],
        job_statuses={
            "a1": JobStatus.QUEUED,
            "a2": JobStatus.STARTED,
            "b1": JobStatus.QUEUED,
        },
        metadata={
            "a1": _make_task("a1", tenant="tenant-a", created_at=_at(10)),
            "a2": _make_task("a2", tenant="tenant-a", created_at=_at(20)),
            "b1": _make_task("b1", tenant="tenant-b", created_at=_at(30)),
        },
    )

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a2", "a1"]
    assert [item.task_status for item in page.items] == [
        TaskStatus.STARTED,
        TaskStatus.PENDING,
    ]


async def test_rq_lists_shared_state_with_an_empty_local_task_cache(monkeypatch):
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=["a1"],
        job_statuses={"a1": JobStatus.QUEUED},
        metadata={"a1": _make_task("a1", tenant="tenant-a")},
    )

    assert orchestrator.tasks == {}
    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a1"]


async def test_rq_reports_a_task_reported_by_both_sources_once(monkeypatch):
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=["a1"],
        started_ids=["a1"],
        job_statuses={"a1": JobStatus.QUEUED},
        metadata={"a1": _make_task("a1", tenant="tenant-a")},
    )

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a1"]


async def test_rq_omits_terminal_jobs_and_expired_entries(monkeypatch):
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=["a-finished", "a-failed", "a-expired"],
        # a-expired is no longer a job in Redis at all: its id survives in the
        # queue but Job.fetch has nothing to return for it.
        job_statuses={
            "a-finished": JobStatus.FINISHED,
            "a-failed": JobStatus.FAILED,
        },
        metadata={
            "a-finished": _make_task("a-finished", tenant="tenant-a"),
            "a-failed": _make_task("a-failed", tenant="tenant-a"),
            "a-expired": _make_task("a-expired", tenant="tenant-a"),
        },
    )

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert page.items == []
    assert page.has_more is False


async def test_rq_drops_a_task_that_reached_a_terminal_status(monkeypatch):
    job_statuses = {"a1": JobStatus.STARTED}
    metadata = {
        "a1": _make_task(
            "a1", tenant="tenant-a", status=TaskStatus.STARTED, created_at=_at(10)
        )
    }
    orchestrator = _rq_orchestrator(
        monkeypatch,
        started_ids=["a1"],
        job_statuses=job_statuses,
        metadata=metadata,
    )

    before = await orchestrator.list_tasks(tenant_id="tenant-a")
    job_statuses["a1"] = JobStatus.FINISHED
    after = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in before.items] == ["a1"]
    assert after.items == []


async def test_rq_omits_metadata_already_marked_terminal(monkeypatch):
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=["a1"],
        job_statuses={"a1": JobStatus.QUEUED},
        metadata={"a1": _make_task("a1", tenant="tenant-a", status=TaskStatus.SUCCESS)},
    )

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert page.items == []


async def test_rq_maps_a_task_without_tenant_metadata_to_default(monkeypatch):
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=["a1"],
        job_statuses={"a1": JobStatus.QUEUED},
        metadata={"a1": _make_task("a1", tenant=None)},
    )

    default_page = await orchestrator.list_tasks(tenant_id="default")
    named_page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in default_page.items] == ["a1"]
    assert named_page.items == []


async def test_rq_propagates_a_backend_failure(monkeypatch):
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=["a1"],
        metadata={"a1": _make_task("a1", tenant="tenant-a")},
        fetch_error=redis.exceptions.ConnectionError("redis is down"),
    )

    with pytest.raises(redis.exceptions.ConnectionError):
        await orchestrator.list_tasks(tenant_id="tenant-a")


@pytest.mark.parametrize("source", ["queue", "registry"])
async def test_rq_propagates_candidate_source_read_failure(monkeypatch, source):
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=["a1"],
        started_ids=["a1"],
        job_statuses={"a1": JobStatus.QUEUED},
        metadata={"a1": _make_task("a1", tenant="tenant-a")},
    )
    failure = redis.exceptions.ConnectionError("redis is down")
    if source == "queue":

        def _fail_queue(offset: int = 0, length: int = -1) -> list[str]:
            raise failure

        monkeypatch.setattr(orchestrator._rq_queue, "get_job_ids", _fail_queue)
    else:

        class _FailingRegistry:
            def get_job_ids(self, start: int = 0, end: int = -1, **_: Any) -> list[str]:
                raise failure

        monkeypatch.setattr(
            rq_orchestrator_module, "StartedJobRegistry", lambda **_: _FailingRegistry()
        )

    with pytest.raises(redis.exceptions.ConnectionError):
        await orchestrator.list_tasks(tenant_id="tenant-a")


async def test_rq_pages_after_filtering_and_validates_the_window(monkeypatch):
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=["a1", "b1", "a2", "a3"],
        job_statuses={
            "a1": JobStatus.QUEUED,
            "b1": JobStatus.QUEUED,
            "a2": JobStatus.QUEUED,
            "a3": JobStatus.QUEUED,
        },
        metadata={
            "a1": _make_task("a1", tenant="tenant-a", created_at=_at(10)),
            "b1": _make_task("b1", tenant="tenant-b", created_at=_at(40)),
            "a2": _make_task("a2", tenant="tenant-a", created_at=_at(20)),
            "a3": _make_task("a3", tenant="tenant-a", created_at=_at(30)),
        },
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


# ---------------------------------------------------------------------------
# Ray backend
# ---------------------------------------------------------------------------


class _FakePipe:
    def __init__(self, store: "_FakeRedis") -> None:
        self._store = store
        self._keys: list[str] = []

    async def __aenter__(self) -> "_FakePipe":
        return self

    async def __aexit__(self, *exc_info: Any) -> bool:
        return False

    def hgetall(self, key: str) -> "_FakePipe":
        self._keys.append(key)
        self._store.touched.append(key)
        return self

    async def execute(self) -> list[Any]:
        self._store.pipe_batches.append(len(self._keys))
        return [self._store.hgetall(key) for key in self._keys]


class _FakeRedis:
    """Redis double covering only the commands a listing performs."""

    def __init__(self) -> None:
        self.lists: dict[str, list[bytes]] = {}
        self.sets: dict[str, set[bytes]] = {}
        self.hashes: dict[str, dict[bytes, bytes]] = {}
        self.touched: list[str] = []
        self.lrange_calls: list[tuple[int, int]] = []
        self.pipe_batches: list[int] = []
        self.failure: Optional[Exception] = None

    def pipeline(self, transaction: bool = False) -> _FakePipe:
        return _FakePipe(self)

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
        config=RayOrchestratorConfig(redis_url="redis://127.0.0.1:1/"),
        converter_manager=_StubConverterManager(),  # type: ignore[arg-type]
    )
    manager = RedisStateManager(redis_url="redis://127.0.0.1:1/")
    manager.redis = store  # type: ignore[assignment]
    manager.pool = object()  # type: ignore[assignment]
    orchestrator.redis_manager = manager
    return orchestrator


async def test_ray_lists_queued_and_active_tasks_of_the_tenant():
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
    assert [item.task_status for item in page.items] == [
        TaskStatus.STARTED,
        TaskStatus.PENDING,
    ]


async def test_ray_reports_a_task_present_in_queue_and_active_set_once():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    store.seed_queue("tenant-a", ["a1"])
    store.seed_active("tenant-a", ["a1"])
    store.seed_task("a1", tenant_id="tenant-a", status=TaskStatus.PENDING.value)

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a1"]


async def test_ray_omits_terminal_stale_and_corrupt_entries():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    store.seed_queue("tenant-a", ["a-live", "a-done", "a-stale", "a-corrupt"])
    store.seed_active("tenant-a", ["a-done", "a-stale", "a-corrupt"])
    store.seed_task("a-live", tenant_id="tenant-a", status=TaskStatus.PENDING.value)
    store.seed_task("a-done", tenant_id="tenant-a", status=TaskStatus.SUCCESS.value)
    # a-stale has no metadata hash at all; a-corrupt carries an unknown status.
    store.seed_task("a-corrupt", tenant_id="tenant-a", status="teleported")
    store.seed_task(
        "a-elsewhere", tenant_id="tenant-b", status=TaskStatus.PENDING.value
    )

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a-live"]


async def test_ray_omits_a_task_that_reached_a_terminal_status():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    store.seed_active("tenant-a", ["a1"])
    store.seed_task("a1", tenant_id="tenant-a", status=TaskStatus.STARTED.value)

    before = await orchestrator.list_tasks(tenant_id="tenant-a")
    store.seed_task("a1", tenant_id="tenant-a", status=TaskStatus.FAILURE.value)
    after = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in before.items] == ["a1"]
    assert after.items == []


async def test_ray_omits_a_corrupt_queue_row():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    store.seed_raw_queue(
        "tenant-a",
        [
            b"not json at all",
            json.dumps(["a1"]).encode("utf-8"),
            json.dumps({"task_id": ""}).encode("utf-8"),
            json.dumps({"task_id": "a1"}).encode("utf-8"),
        ],
    )
    store.seed_task("a1", tenant_id="tenant-a", status=TaskStatus.PENDING.value)

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a1"]


async def test_ray_lists_the_default_tenant_queue():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    store.seed_queue("default", ["d1"])
    store.seed_task("d1", tenant_id="default", status=TaskStatus.PENDING.value)

    page = await orchestrator.list_tasks(tenant_id="")

    assert [item.task_id for item in page.items] == ["d1"]


async def test_ray_reads_only_the_requested_tenant_keys():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    store.seed_queue("tenant-a", ["a1"])
    store.seed_queue("tenant-b", ["b1"])
    store.seed_active("tenant-b", ["b1"])
    store.seed_task("a1", tenant_id="tenant-a", status=TaskStatus.PENDING.value)
    store.seed_task("b1", tenant_id="tenant-b", status=TaskStatus.PENDING.value)

    await orchestrator.list_tasks(tenant_id="tenant-a")

    # The other tenant's queue, active set and task hash are never touched.
    assert set(store.touched) == {
        "tenant:tenant-a:tasks",
        "tenant:tenant-a:active_tasks",
        "task:a1",
    }


async def test_ray_reads_a_deep_queue_in_bounded_chunks():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    queued = [f"a{index}" for index in range(LISTING_CHUNK_SIZE * 2 + 5)]
    store.seed_queue("tenant-a", queued)
    for task_id in queued:
        store.seed_task(task_id, tenant_id="tenant-a", status=TaskStatus.PENDING.value)

    page = await orchestrator.list_tasks(tenant_id="tenant-a", limit=LISTING_CHUNK_SIZE)

    assert len(page.items) == LISTING_CHUNK_SIZE
    assert page.has_more is True
    assert all(
        end - start + 1 <= LISTING_CHUNK_SIZE for start, end in store.lrange_calls
    )
    assert len(store.lrange_calls) == 3
    assert store.pipe_batches, "metadata reads must be batched"
    assert all(0 < batch <= LISTING_CHUNK_SIZE for batch in store.pipe_batches)


async def test_ray_pages_after_filtering_and_validates_the_window():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    store.seed_queue("tenant-a", ["a1", "b1", "a2", "a3"])
    for task_id, created in (("a1", 10), ("b1", 40), ("a2", 20), ("a3", 30)):
        store.seed_task(
            task_id,
            tenant_id="tenant-b" if task_id == "b1" else "tenant-a",
            status=TaskStatus.PENDING.value,
            created_at=_at(created).isoformat(),
        )

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


@pytest.mark.parametrize("stage", ["queue", "active", "metadata"])
async def test_ray_propagates_a_backend_failure(stage, monkeypatch):
    store = _FakeRedis()
    store.seed_queue("tenant-a", ["a1"])
    store.seed_task("a1", tenant_id="tenant-a", status=TaskStatus.PENDING.value)
    orchestrator = _ray_orchestrator(store)
    failure = redis.exceptions.ConnectionError("redis is down")

    if stage == "queue":

        async def _fail_lrange(key: str, start: int, end: int) -> list[bytes]:
            raise failure

        monkeypatch.setattr(store, "lrange", _fail_lrange)
    elif stage == "active":

        async def _fail_smembers(key: str) -> set[bytes]:
            raise failure

        monkeypatch.setattr(store, "smembers", _fail_smembers)
    else:
        pipe = store.pipeline()

        async def _fail_execute() -> list[Any]:
            raise failure

        monkeypatch.setattr(pipe, "execute", _fail_execute)
        monkeypatch.setattr(store, "pipeline", lambda transaction=False: pipe)

    with pytest.raises(redis.exceptions.ConnectionError):
        await orchestrator.list_tasks(tenant_id="tenant-a")


async def test_ray_lists_shared_state_with_an_empty_local_task_cache():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    store.seed_active("tenant-a", ["a1"])
    store.seed_task("a1", tenant_id="tenant-a", status=TaskStatus.STARTED.value)

    assert orchestrator.tasks == {}
    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert [item.task_id for item in page.items] == ["a1"]


def test_active_statuses_cover_only_non_terminal_states():
    assert ACTIVE_TASK_STATUSES == {TaskStatus.PENDING, TaskStatus.STARTED}


async def test_ray_listing_omits_invalid_task_json_with_valid_metadata():
    store = _FakeRedis()
    orchestrator = _ray_orchestrator(store)
    row = json.dumps({"task_id": "a1", "task_status": "teleported"}).encode()
    with pytest.raises(ValueError):
        validate_task_json(row)
    store.seed_raw_queue("tenant-a", [row])
    store.seed_task("a1", tenant_id="tenant-a", status=TaskStatus.PENDING.value)

    page = await orchestrator.list_tasks(tenant_id="tenant-a")

    assert page.items == []


async def test_rq_listing_exhausts_both_deep_candidate_sources(monkeypatch):
    ids = [f"a{index:03}" for index in range(LISTING_CHUNK_SIZE * 2 + 5)]
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=ids,
        started_ids=ids,
        job_statuses=dict.fromkeys(ids, JobStatus.STARTED),
        metadata={
            task_id: _make_task(task_id, created_at=_at(index))
            for index, task_id in enumerate(ids)
        },
    )
    page = await orchestrator.list_tasks(tenant_id="tenant-a", limit=1)
    assert [item.task_id for item in page.items] == [ids[-1]]
    assert page.has_more is True
    final = await orchestrator.list_tasks(
        tenant_id="tenant-a", limit=1, offset=len(ids) - 1
    )
    assert [item.task_id for item in final.items] == [ids[0]]
    assert final.has_more is False

    queue = orchestrator._rq_queue
    assert queue.windows, "queue reads must be recorded"
    assert all(0 < length <= LISTING_CHUNK_SIZE for _, length in queue.windows)
    registries = orchestrator._test_registries  # type: ignore[attr-defined]
    assert registries, "started registry reads must be recorded"
    for registry in registries:
        assert registry.windows, "registry reads must be recorded"
        assert all(
            0 < end - start + 1 <= LISTING_CHUNK_SIZE for start, end in registry.windows
        )


@pytest.mark.parametrize("row", [None, b"not json", b"[]", b'{"task_id":"a1"}'])
async def test_rq_listing_omits_absent_or_corrupt_real_metadata(monkeypatch, row):
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=["a1"],
        job_statuses={"a1": JobStatus.QUEUED},
    )
    monkeypatch.setattr(
        orchestrator,
        "_get_listing_task_from_redis",
        RQOrchestrator._get_listing_task_from_redis.__get__(orchestrator),
    )

    class MetadataRedis:
        async def get(self, key):
            return row

    orchestrator._async_redis_conn = MetadataRedis()
    page = await orchestrator.list_tasks(tenant_id="tenant-a")
    assert page.items == []
    assert page.has_more is False


@pytest.mark.parametrize("tenant", [None, "", "tenant-a"])
async def test_rq_listing_real_metadata_roundtrip(monkeypatch, tenant):
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=["a1"],
        job_statuses={"a1": JobStatus.STARTED},
    )
    monkeypatch.setattr(
        orchestrator,
        "_get_listing_task_from_redis",
        RQOrchestrator._get_listing_task_from_redis.__get__(orchestrator),
    )
    task = _make_task("a1", tenant=tenant, created_at=_at(10))

    class MetadataRedis:
        async def get(self, key):
            assert key == "docling:tasks:a1:metadata"
            return task.model_dump_json().encode()

    orchestrator._async_redis_conn = MetadataRedis()
    page = await orchestrator.list_tasks(tenant_id=tenant or "default")
    assert [item.task_id for item in page.items] == ["a1"]
    assert page.items[0].task_status == TaskStatus.STARTED
    assert page.items[0].created_at == _at(10)
    assert page.has_more is False


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
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=["a1"],
        job_statuses={"a1": JobStatus.QUEUED},
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
    with pytest.raises(redis.exceptions.ConnectionError):
        await orchestrator.list_tasks(tenant_id="tenant-a")


async def test_rq_listing_decodes_chunk_task_via_real_metadata(monkeypatch):
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=["c1", "c2"],
        job_statuses={"c1": JobStatus.QUEUED, "c2": JobStatus.STARTED},
    )
    monkeypatch.setattr(
        orchestrator,
        "_get_listing_task_from_redis",
        RQOrchestrator._get_listing_task_from_redis.__get__(orchestrator),
    )
    first = _make_task(
        "c1", tenant="tenant-a", task_type=TaskType.CHUNK, created_at=_at(10)
    )
    second = _make_task(
        "c2", tenant="tenant-a", task_type=TaskType.CHUNK, created_at=_at(20)
    )
    payloads = {
        "docling:tasks:c1:metadata": first.model_dump_json().encode(),
        "docling:tasks:c2:metadata": second.model_dump_json().encode(),
    }

    class MetadataRedis:
        async def get(self, key):
            return payloads.get(key)

    orchestrator._async_redis_conn = MetadataRedis()
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
    orchestrator = _rq_orchestrator(monkeypatch)
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


async def test_rq_listing_propagates_job_status_read_failure(monkeypatch):
    orchestrator = _rq_orchestrator(monkeypatch, queue_ids=["a1"])

    class FailingJob:
        @staticmethod
        def fetch(task_id, connection):
            return FailingJob()

        def get_status(self):
            raise redis.exceptions.ConnectionError("job status redis is down")

    monkeypatch.setattr(rq_orchestrator_module, "Job", FailingJob)
    with pytest.raises(
        redis.exceptions.ConnectionError, match="job status redis is down"
    ):
        await orchestrator.list_tasks(tenant_id="tenant-a")


async def test_rq_loaders_differ_only_on_transport_error(monkeypatch):
    orchestrator = _rq_orchestrator(
        monkeypatch,
        queue_ids=["a1"],
        job_statuses={"a1": JobStatus.QUEUED},
    )
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
