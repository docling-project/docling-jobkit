import asyncio
import datetime
import os
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytest.importorskip("ray")
if os.getenv("CI"):
    pytest.skip("Skipping Ray tests in CI", allow_module_level=True)
from ray.serve.schema import ApplicationStatus

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.service.responses import (
    FailureCategory,
    FailurePhase,
    PublicFailureInfo,
)
from docling.datamodel.service.targets import InBodyTarget
from docling.datamodel.service.tasks import TaskType

from docling_jobkit.datamodel.result import (
    DoclingTaskResult,
    DocumentResultItem,
    ExportDocumentResponse,
)
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators._redis_gate import RedisCallerGate
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.dispatcher import RayTaskDispatcher
from docling_jobkit.orchestrators.ray.models import (
    ConverterFailureResult,
    RedisTaskMetadata,
    TenantLimits,
)
from docling_jobkit.orchestrators.ray.orchestrator import (
    DispatcherUnavailableError,
    RayOrchestrator,
)
from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager
from docling_jobkit.orchestrators.ray.serve_deployment import (
    DoclingProcessorConverterDeployment,
    DoclingProcessorCoordinatorDeployment,
)


def _make_orchestrator(
    config: RayOrchestratorConfig | None = None,
) -> RayOrchestrator:
    config = config or RayOrchestratorConfig(redis_url="redis://localhost:6379/")
    with patch.object(RayOrchestrator, "__init__", lambda self, **kw: None):
        orchestrator = object.__new__(RayOrchestrator)

    orchestrator.config = config
    orchestrator.tasks = {}
    orchestrator.notifier = None
    orchestrator.cm = MagicMock()
    orchestrator.cm.config = object()
    orchestrator.redis_manager = AsyncMock()
    orchestrator._redis_gate = RedisCallerGate(config.redis_gate_concurrency or 1)
    orchestrator._pubsub_task = None
    orchestrator._dispatcher_supervisor_task = None
    orchestrator.dispatcher = None
    orchestrator.dispatcher_name = "docling_task_dispatcher"
    orchestrator.serve_app_name = "docling_processor"
    orchestrator.deployment_handle = object()
    orchestrator._unhealthy_since = None
    orchestrator._ray_session_needs_restart = False
    orchestrator._ray_admin_executor = None
    return orchestrator


def _make_dispatcher(
    config: RayOrchestratorConfig | None = None,
) -> RayTaskDispatcher:
    config = config or RayOrchestratorConfig(redis_url="redis://localhost:6379/")
    dispatcher_class = RayTaskDispatcher.__ray_actor_class__
    dispatcher = dispatcher_class(config, object())
    dispatcher.redis_manager.disconnect = AsyncMock()
    dispatcher.redis_manager.connect = AsyncMock()
    return dispatcher


def _success_result() -> DoclingTaskResult:
    return DoclingTaskResult(
        result=DocumentResultItem(
            document=ExportDocumentResponse(filename="doc.md", md_content="hello"),
            status=ConversionStatus.SUCCESS,
        ),
        processing_time=0.1,
        num_converted=1,
        num_succeeded=1,
        num_partially_succeeded=0,
        num_failed=0,
    )


class _FakeReplicaContext:
    def __init__(self, replica_id) -> None:
        self.replica_id = replica_id


class _FakeReplicaID:
    def __str__(self) -> str:
        return "Replica(id='abc123', deployment='converter', app='docling_processor')"


def test_ray_orchestrator_init_is_ray_free() -> None:
    config = RayOrchestratorConfig(redis_url="redis://localhost:6379/")
    converter_manager = MagicMock()

    with (
        patch(
            "docling_jobkit.orchestrators.ray.orchestrator.ray.is_initialized",
            side_effect=AssertionError("__init__ should not touch Ray"),
        ),
        patch(
            "docling_jobkit.orchestrators.ray.orchestrator.ray.init"
        ) as mock_ray_init,
        patch(
            "docling_jobkit.orchestrators.ray.orchestrator.serve.start"
        ) as mock_serve_start,
        patch(
            "docling_jobkit.orchestrators.ray.orchestrator.deploy_processor"
        ) as mock_deploy_processor,
        patch.object(
            RayOrchestrator,
            "_bind_dispatcher",
            side_effect=AssertionError("__init__ should not bind the dispatcher"),
        ),
    ):
        orchestrator = RayOrchestrator(
            config=config, converter_manager=converter_manager
        )

    assert orchestrator.deployment_handle is None
    assert orchestrator.dispatcher is None
    mock_ray_init.assert_not_called()
    mock_serve_start.assert_not_called()
    mock_deploy_processor.assert_not_called()


def test_converter_deployment_stringifies_replica_id() -> None:
    config = RayOrchestratorConfig(redis_url="redis://localhost:6379/")
    converter_manager_config = MagicMock()
    replica_id = _FakeReplicaID()

    with (
        patch(
            "docling_jobkit.orchestrators.ray.serve_deployment.serve.get_replica_context",
            return_value=_FakeReplicaContext(replica_id),
        ),
        patch(
            "docling_jobkit.orchestrators.ray.serve_deployment.DoclingConverterManager"
        ),
        patch(
            "docling_jobkit.orchestrators.ray.serve_deployment.tempfile.mkdtemp",
            return_value="/tmp/docling_serve_replica",
        ),
    ):
        deployment_cls = DoclingProcessorConverterDeployment.func_or_class
        deployment = deployment_cls(
            converter_manager_config=converter_manager_config,
            config=config,
        )

    assert deployment.replica_id == str(replica_id)
    assert isinstance(deployment.replica_id, str)


@pytest.mark.asyncio
async def test_run_ray_admin_reuses_single_thread() -> None:
    orchestrator = _make_orchestrator()

    def _thread_ident() -> int:
        return threading.get_ident()

    first_ident = await orchestrator._run_ray_admin(_thread_ident)
    second_ident = await orchestrator._run_ray_admin(_thread_ident)

    assert first_ident == second_ident
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_initialize_ray_runtime_calls_ray_startup_lazily() -> None:
    """Fresh Ray init with no existing Serve app deploys the processor."""
    orchestrator = _make_orchestrator()
    orchestrator.deployment_handle = None
    orchestrator.dispatcher = None
    serve_status = MagicMock()
    serve_status.applications = {}

    calls: list[str] = []

    async def _fake_run_ray_admin(func, *args, **kwargs):
        del args, kwargs
        calls.append(func.__name__)
        if func.__name__ == "is_initialized":
            return False
        if func.__name__ in {"init", "start"}:
            return None
        if func.__name__ == "status":
            return serve_status
        if func.__name__ == "deploy_processor":
            return "deployment-handle"
        if func.__name__ == "_bind_dispatcher":
            return "dispatcher-handle"
        raise AssertionError(f"Unexpected admin call: {func.__name__}")

    with (
        patch(
            "docling_jobkit.orchestrators.ray.orchestrator.asyncio.to_thread",
            side_effect=lambda func, *args, **kwargs: func(*args, **kwargs),
        ),
        patch.object(
            orchestrator,
            "_run_ray_admin",
            side_effect=_fake_run_ray_admin,
        ),
        patch.object(
            orchestrator,
            "_build_ray_init_kwargs",
            return_value={"address": None, "namespace": "docling", "runtime_env": None},
        ) as mock_build_init_kwargs,
    ):
        await orchestrator._initialize_ray_runtime()

    mock_build_init_kwargs.assert_called_once_with()
    assert orchestrator.deployment_handle == "deployment-handle"
    assert orchestrator.dispatcher == "dispatcher-handle"
    assert calls == [
        "is_initialized",
        "init",
        "start",
        "status",
        "deploy_processor",
        "_bind_dispatcher",
    ]


@pytest.mark.asyncio
async def test_initialize_ray_runtime_restarts_session_on_same_admin_path() -> None:
    orchestrator = _make_orchestrator()
    orchestrator.deployment_handle = None
    orchestrator.dispatcher = "stale-dispatcher"
    orchestrator._ray_session_needs_restart = True
    calls: list[str] = []

    serve_status = MagicMock()
    serve_status.applications = {
        "docling_processor": MagicMock(status=ApplicationStatus.RUNNING)
    }

    def _name_for(func) -> str:
        if getattr(func, "__self__", None) is orchestrator:
            return func.__func__.__name__
        return func.__name__

    async def _fake_run_ray_admin(func, *args, **kwargs):
        del args, kwargs
        name = _name_for(func)
        calls.append(name)
        if name == "is_initialized":
            return len([call for call in calls if call == "is_initialized"]) == 1
        if name in {
            "shutdown",
            "init",
            "start",
        }:
            return None
        if name == "status":
            return serve_status
        if name == "get_app_handle":
            return "fresh-handle"
        if name == "_bind_dispatcher":
            return "fresh-dispatcher"
        if name == "get_dashboard_url":
            return None
        raise AssertionError(f"Unexpected admin call: {name}")

    with (
        patch.object(
            orchestrator,
            "_run_ray_admin",
            side_effect=_fake_run_ray_admin,
        ),
        patch.object(
            orchestrator,
            "_build_ray_init_kwargs",
            return_value={"address": None, "namespace": "docling", "runtime_env": None},
        ),
    ):
        await orchestrator._initialize_ray_runtime()

    assert orchestrator._ray_session_needs_restart is False
    assert orchestrator.deployment_handle == "fresh-handle"
    assert orchestrator.dispatcher == "fresh-dispatcher"
    assert calls == [
        "shutdown",
        "is_initialized",
        "start",
        "status",
        "get_app_handle",
        "_bind_dispatcher",
    ]


@pytest.mark.asyncio
async def test_initialize_ray_runtime_shutdowns_broken_client_even_if_not_initialized() -> (
    None
):
    orchestrator = _make_orchestrator()
    orchestrator.deployment_handle = None
    orchestrator.dispatcher = None
    orchestrator._ray_session_needs_restart = True
    calls: list[str] = []

    serve_status = MagicMock()
    serve_status.applications = {
        "docling_processor": MagicMock(status=ApplicationStatus.RUNNING)
    }

    def _name_for(func) -> str:
        if getattr(func, "__self__", None) is orchestrator:
            return func.__func__.__name__
        return func.__name__

    async def _fake_run_ray_admin(func, *args, **kwargs):
        del args, kwargs
        name = _name_for(func)
        calls.append(name)
        if name == "is_initialized":
            return False
        if name in {"shutdown", "init", "start"}:
            return None
        if name == "status":
            return serve_status
        if name == "get_app_handle":
            return "fresh-handle"
        if name == "_bind_dispatcher":
            return "fresh-dispatcher"
        raise AssertionError(f"Unexpected admin call: {name}")

    with (
        patch.object(
            orchestrator,
            "_run_ray_admin",
            side_effect=_fake_run_ray_admin,
        ),
        patch.object(
            orchestrator,
            "_build_ray_init_kwargs",
            return_value={"address": None, "namespace": "docling", "runtime_env": None},
        ),
    ):
        await orchestrator._initialize_ray_runtime()

    assert orchestrator._ray_session_needs_restart is False
    assert orchestrator.deployment_handle == "fresh-handle"
    assert orchestrator.dispatcher == "fresh-dispatcher"
    assert calls == [
        "shutdown",
        "is_initialized",
        "init",
        "start",
        "status",
        "get_app_handle",
        "_bind_dispatcher",
    ]


class _TerminalizationPipeline:
    def __init__(self, state: dict[str, object]) -> None:
        self.state = state
        self.ops: list[tuple] = []

    async def __aenter__(self) -> "_TerminalizationPipeline":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    async def watch(self, *keys: str) -> None:
        del keys

    def sismember(self, key: str, value: str) -> bool:
        del key
        active_members = self.state["active_members"]
        assert isinstance(active_members, set)
        return value in active_members

    def multi(self) -> None:
        self.ops = []

    def setex(self, key: str, ttl: int, value: bytes) -> None:
        self.ops.append(("setex", key, ttl, value))

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        self.ops.append(("hset", key, mapping))

    def hdel(self, key: str, *fields: str) -> None:
        self.ops.append(("hdel", key, fields))

    def delete(self, *keys: str) -> None:
        for key in keys:
            self.ops.append(("delete", key))

    def srem(self, key: str, value: str) -> None:
        self.ops.append(("srem", key, value))

    def hincrby(self, key: str, field: str, amount: int) -> None:
        self.ops.append(("hincrby", key, field, amount))

    async def execute(self) -> list[int]:
        task_fields = self.state["task_fields"]
        result_store = self.state["result_store"]
        deleted_keys = self.state["deleted_keys"]
        active_members = self.state["active_members"]
        limits = self.state["limits"]
        assert isinstance(task_fields, dict)
        assert isinstance(result_store, dict)
        assert isinstance(deleted_keys, list)
        assert isinstance(active_members, set)
        assert isinstance(limits, dict)

        results: list[int] = []
        for op in self.ops:
            if op[0] == "setex":
                result_store[op[1]] = {"ttl": op[2], "value": op[3]}
                results.append(1)
            elif op[0] == "hset":
                task_fields.update(op[2])
                results.append(1)
            elif op[0] == "hdel":
                removed = 0
                for field in op[2]:
                    if field in task_fields:
                        removed += 1
                        del task_fields[field]
                results.append(removed)
            elif op[0] == "delete":
                deleted_keys.append(op[1])
                results.append(1)
            elif op[0] == "srem":
                removed = 1 if op[2] in active_members else 0
                if removed:
                    active_members.remove(op[2])
                results.append(removed)
            elif op[0] == "hincrby":
                limits[op[2]] = int(limits.get(op[2], 0)) + op[3]
                results.append(int(limits[op[2]]))
            else:
                raise AssertionError(f"Unexpected op {op[0]}")

        return results


class _FakeTerminalRedis:
    def __init__(self, state: dict[str, object]) -> None:
        self.state = state

    async def hget(self, key: str, field: str) -> bytes | None:
        # Terminalization reads the held converter-unit count off the execution
        # hash to release it in the same transaction.
        if key == "task:task-1:execution":
            assert field == "converter_units"
            held = self.state.get("execution_converter_units")
            if held is None:
                return None
            return str(held).encode("utf-8")
        assert key == "task:task-1"
        assert field == "status"
        task_fields = self.state["task_fields"]
        assert isinstance(task_fields, dict)
        value = task_fields.get(field)
        if value is None:
            return None
        return str(value).encode("utf-8")

    def pipeline(self, transaction: bool = True) -> _TerminalizationPipeline:
        assert transaction is True
        return _TerminalizationPipeline(self.state)


class _StatusGuardPipeline:
    """Minimal WATCH/MULTI pipeline backed by a shared task-fields dict."""

    def __init__(self, store: dict[str, dict[str, str]]) -> None:
        self.store = store
        self._mapping: dict[str, str] | None = None

    async def __aenter__(self) -> "_StatusGuardPipeline":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    async def watch(self, *keys: str) -> None:
        del keys

    async def unwatch(self) -> None:
        return None

    def multi(self) -> None:
        self._mapping = None

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        del key
        self._mapping = mapping

    async def execute(self) -> list[int]:
        assert self._mapping is not None
        self.store["fields"].update(self._mapping)
        return [1]


class _StatusGuardRedis:
    """Fake redis supporting plain hset (metadata) and a guarded status pipeline."""

    def __init__(self, store: dict[str, dict[str, str]]) -> None:
        self.store = store

    async def hget(self, key: str, field: str) -> bytes | None:
        del key
        value = self.store["fields"].get(field)
        return value.encode("utf-8") if value is not None else None

    async def hset(self, key: str, mapping: dict[str, str]) -> None:
        del key
        self.store["fields"].update(mapping)

    def pipeline(self, transaction: bool = True) -> _StatusGuardPipeline:
        assert transaction is True
        return _StatusGuardPipeline(self.store)


@pytest.mark.asyncio
async def test_set_task_metadata_and_update_status_store_real_timestamps() -> None:
    store: dict[str, dict[str, str]] = {"fields": {}}
    manager = RedisStateManager(redis_url="redis://localhost:6379/")
    manager.redis = _StatusGuardRedis(store)  # type: ignore[assignment]

    await manager.set_task_metadata(
        task_id="task-1",
        tenant_id="tenant-a",
        task_type=TaskType.CHUNK,
        task_size=3,
        status=TaskStatus.PENDING,
    )
    fields = store["fields"]
    assert fields["created_at"] != "null"
    assert fields["task_type"] == TaskType.CHUNK.value
    assert fields["task_size"] == "3"
    assert fields["status"] == TaskStatus.PENDING.value

    await manager.update_task_status("task-1", TaskStatus.STARTED)

    assert fields["status"] == TaskStatus.STARTED.value
    assert fields["last_update_at"] != "null"
    assert fields["started_at"] != "null"

    datetime.datetime.fromisoformat(fields["created_at"])
    datetime.datetime.fromisoformat(fields["last_update_at"])
    datetime.datetime.fromisoformat(fields["started_at"])


@pytest.mark.asyncio
async def test_finalize_task_success_atomic_is_idempotent() -> None:
    manager = RedisStateManager(
        redis_url="redis://localhost:6379/",
        max_documents=10,
    )
    state: dict[str, object] = {
        "task_fields": {
            "status": TaskStatus.STARTED.value,
            "error_message": "old error",
        },
        "active_members": {"task-1"},
        "limits": {"active_tasks": 1, "active_documents": 2},
        "deleted_keys": [],
        "result_store": {},
    }
    manager.redis = _FakeTerminalRedis(state)  # type: ignore[assignment]

    result = _success_result()
    outcome_1 = await manager.finalize_task_success_atomic(
        tenant_id="tenant-a",
        task_id="task-1",
        task_size=2,
        result=result,
    )
    outcome_2 = await manager.finalize_task_success_atomic(
        tenant_id="tenant-a",
        task_id="task-1",
        task_size=2,
        result=result,
    )

    task_fields = state["task_fields"]
    limits = state["limits"]
    deleted_keys = state["deleted_keys"]
    result_store = state["result_store"]
    active_members = state["active_members"]
    assert isinstance(task_fields, dict)
    assert isinstance(limits, dict)
    assert isinstance(deleted_keys, list)
    assert isinstance(result_store, dict)
    assert isinstance(active_members, set)

    assert outcome_1.final_status == TaskStatus.SUCCESS
    assert outcome_1.status_changed is True
    assert outcome_1.capacity_released is True
    assert outcome_2.final_status == TaskStatus.SUCCESS
    assert outcome_2.status_changed is False
    assert outcome_2.capacity_released is False
    assert task_fields["status"] == TaskStatus.SUCCESS.value
    assert "error_message" not in task_fields
    assert limits["active_tasks"] == 0
    assert limits["active_documents"] == 0
    assert "task-1" not in active_members
    assert deleted_keys.count("task:task-1:dispatch") == 2
    assert "docling:ray:results:task:task-1:result" in result_store


@pytest.mark.asyncio
async def test_finalize_task_failure_atomic_preserves_existing_success() -> None:
    manager = RedisStateManager(
        redis_url="redis://localhost:6379/",
        max_documents=10,
    )
    state: dict[str, object] = {
        "task_fields": {
            "status": TaskStatus.SUCCESS.value,
        },
        "active_members": {"task-1"},
        "limits": {"active_tasks": 1, "active_documents": 2},
        "deleted_keys": [],
        "result_store": {},
    }
    manager.redis = _FakeTerminalRedis(state)  # type: ignore[assignment]

    outcome = await manager.finalize_task_failure_atomic(
        tenant_id="tenant-a",
        task_id="task-1",
        task_size=2,
        error_message="dispatcher died",
        failure=PublicFailureInfo(
            category=FailureCategory.INTERNAL,
            message="Internal processing error.",
            retryable=False,
            phase=FailurePhase.ORCHESTRATION,
        ),
    )

    task_fields = state["task_fields"]
    limits = state["limits"]
    active_members = state["active_members"]
    assert isinstance(task_fields, dict)
    assert isinstance(limits, dict)
    assert isinstance(active_members, set)

    assert outcome.final_status == TaskStatus.SUCCESS
    assert outcome.status_changed is False
    assert outcome.capacity_released is True
    assert task_fields["status"] == TaskStatus.SUCCESS.value
    assert limits["active_tasks"] == 0
    assert limits["active_documents"] == 0
    assert "task-1" not in active_members


def test_config_accepts_heartbeat_interval_without_stale_age() -> None:
    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/",
        heartbeat_interval=60.0,
    )

    assert config.heartbeat_interval == 60.0


def test_config_rejects_non_positive_heartbeat_interval() -> None:
    with pytest.raises(ValueError, match="heartbeat_interval must be > 0"):
        RayOrchestratorConfig(
            redis_url="redis://localhost:6379/",
            heartbeat_interval=0.0,
        )


def test_config_maps_deprecated_ray_resource_aliases(caplog) -> None:
    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/",
        ray_num_cpus_per_actor=3.0,
        ray_memory_limit_per_actor="8Gi",
        coordinator_num_cpus=0.75,
        coordinator_memory_limit="512Mi",
    )

    assert config.converter_actor_num_cpus == 3.0
    assert config.converter_actor_memory_request == "8Gi"
    assert config.coordinator_actor_num_cpus == 0.75
    assert config.coordinator_actor_memory_request == "512Mi"
    assert "ray_num_cpus_per_actor is deprecated" in caplog.text
    assert "ray_memory_limit_per_actor is deprecated" in caplog.text


def test_new_ray_resource_settings_override_deprecated_aliases() -> None:
    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/",
        converter_actor_num_cpus=4.0,
        ray_num_cpus_per_actor=2.0,
        converter_actor_memory_request="10Gi",
        ray_memory_limit_per_actor="8Gi",
        coordinator_actor_num_cpus=1.0,
        coordinator_num_cpus=0.5,
        coordinator_actor_memory_request="1Gi",
        coordinator_memory_limit="512Mi",
    )

    assert config.converter_actor_num_cpus == 4.0
    assert config.converter_actor_memory_request == "10Gi"
    assert config.coordinator_actor_num_cpus == 1.0
    assert config.coordinator_actor_memory_request == "1Gi"


@pytest.mark.asyncio
async def test_serve_replica_finalizes_expected_converter_failure_without_raising(
    tmp_path,
) -> None:
    deployment_cls = DoclingProcessorCoordinatorDeployment.func_or_class
    deployment = object.__new__(deployment_cls)
    deployment.config = MagicMock(enable_oom_protection=False)
    deployment.replica_id = "replica-1"
    deployment.redis_manager = AsyncMock()
    deployment.redis_manager.write_task_execution_lease = AsyncMock(
        side_effect=RuntimeError("skip heartbeat")
    )
    deployment.redis_manager.finalize_task_failure_atomic = AsyncMock(
        return_value=MagicMock(
            final_status=TaskStatus.FAILURE,
            status_changed=True,
            result_key=None,
        )
    )
    deployment.redis_manager.publish_update = AsyncMock()
    deployment.redis_manager.update_tenant_stats = AsyncMock()
    deployment.scratch_dir = tmp_path
    deployment.tasks_processed = 0
    deployment.documents_processed = 0
    deployment.last_task_time = None
    deployment._process_task = AsyncMock(
        return_value=ConverterFailureResult(
            failure=PublicFailureInfo(
                category=FailureCategory.POLICY,
                message="404 Client Error: Not Found for url: https://example.com/missing.pdf",
                retryable=False,
                phase=FailurePhase.SOURCE_ENUMERATION,
                details={"source_kind": "http"},
            )
        )
    )

    task = Task(
        task_id="task-404",
        sources=[],
        target=InBodyTarget(),
        task_type=TaskType.CONVERT,
        metadata={"tenant_id": "tenant-a"},
    )

    result = await deployment_cls.process_task(deployment, task)

    assert result.num_failed == 0
    deployment.redis_manager.finalize_task_failure_atomic.assert_awaited_once()
    deployment.redis_manager.publish_update.assert_awaited_once()
    deployment.redis_manager.update_tenant_stats.assert_awaited_once()


@pytest.mark.asyncio
async def test_task_status_reconstructs_from_redis_metadata() -> None:
    orchestrator = _make_orchestrator()
    metadata = RedisTaskMetadata(
        task_id="task-1",
        tenant_id="tenant-a",
        status=TaskStatus.FAILURE,
        task_type=TaskType.CHUNK,
        task_size=2,
        created_at=datetime.datetime.now(datetime.timezone.utc),
        last_update_at=datetime.datetime.now(datetime.timezone.utc),
        error_message="dispatcher failed",
    )
    orchestrator.redis_manager.get_task_metadata_model = AsyncMock(
        return_value=metadata
    )

    task = await orchestrator.task_status("task-1")

    assert task.task_id == "task-1"
    assert task.task_status == TaskStatus.FAILURE
    assert task.task_type == TaskType.CHUNK
    assert task.error_message == "dispatcher failed"
    assert orchestrator.tasks["task-1"] is task


@pytest.mark.asyncio
async def test_task_status_checks_memory_only_once_after_redis_miss() -> None:
    orchestrator = _make_orchestrator()
    cached_task = Task(task_id="task-cached", sources=[], target=InBodyTarget())
    await orchestrator.init_task_tracking(cached_task)
    orchestrator.redis_manager.get_task_metadata_model = AsyncMock(return_value=None)

    task = await orchestrator.task_status(cached_task.task_id)

    assert task is cached_task
    orchestrator.redis_manager.get_task_metadata_model.assert_awaited_once_with(
        cached_task.task_id
    )


@pytest.mark.asyncio
async def test_ensure_dispatcher_ready_does_not_refresh_runtime() -> None:
    orchestrator = _make_orchestrator()
    dispatcher = MagicMock()
    dispatcher.get_health.remote = AsyncMock(return_value=True)
    dispatcher.refresh_runtime.remote = AsyncMock()
    orchestrator.dispatcher = dispatcher

    await orchestrator.ensure_dispatcher_ready()

    dispatcher.get_health.remote.assert_awaited_once()
    dispatcher.refresh_runtime.remote.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_dispatcher_ready_times_out_without_dropping_binding() -> None:
    orchestrator = _make_orchestrator(
        RayOrchestratorConfig(
            redis_url="redis://localhost:6379/",
            dispatcher_rpc_timeout=0.01,
        )
    )

    async def _hang() -> bool:
        await asyncio.sleep(60)
        return True

    dispatcher = MagicMock()
    dispatcher.get_health.remote = AsyncMock(side_effect=_hang)
    orchestrator.dispatcher = dispatcher

    with pytest.raises(DispatcherUnavailableError, match="timed out"):
        await orchestrator.ensure_dispatcher_ready()

    # The probe is side-effect-free: handle repair is owned by the supervisor,
    # which nulls the handle in its own exception path.
    assert orchestrator.dispatcher is dispatcher


@pytest.mark.asyncio
async def test_ensure_dispatcher_ready_wraps_baseexception_without_dropping_binding() -> (
    None
):
    orchestrator = _make_orchestrator()
    dispatcher = MagicMock()
    dispatcher.get_health.remote = AsyncMock(side_effect=SystemExit(15))
    orchestrator.dispatcher = dispatcher

    with pytest.raises(DispatcherUnavailableError, match="15"):
        await orchestrator.ensure_dispatcher_ready()

    assert orchestrator.dispatcher is dispatcher


@pytest.mark.asyncio
async def test_ensure_dispatcher_ready_preserves_cancelled_error() -> None:
    orchestrator = _make_orchestrator()
    dispatcher = MagicMock()
    dispatcher.get_health.remote = AsyncMock(side_effect=asyncio.CancelledError())
    orchestrator.dispatcher = dispatcher

    with pytest.raises(asyncio.CancelledError):
        await orchestrator.ensure_dispatcher_ready()


@pytest.mark.asyncio
def test_is_liveness_healthy_uses_continuous_unhealthy_deadline() -> None:
    orchestrator = _make_orchestrator(
        RayOrchestratorConfig(
            redis_url="redis://localhost:6379/",
            liveness_fail_after=90.0,
        )
    )

    assert orchestrator.is_liveness_healthy() is True

    orchestrator._unhealthy_since = 100.0
    with patch(
        "docling_jobkit.orchestrators.ray.orchestrator.time.monotonic",
        return_value=150.0,
    ):
        assert orchestrator.is_liveness_healthy() is True
    with patch(
        "docling_jobkit.orchestrators.ray.orchestrator.time.monotonic",
        return_value=190.0,
    ):
        assert orchestrator.is_liveness_healthy() is False


@pytest.mark.asyncio
async def test_shutdown_is_local_only() -> None:
    orchestrator = _make_orchestrator()
    orchestrator.redis_manager.disconnect = AsyncMock()
    dispatcher = MagicMock()
    dispatcher.stop_dispatching.remote = AsyncMock()
    orchestrator.dispatcher = dispatcher
    orchestrator._pubsub_task = asyncio.create_task(asyncio.sleep(60))
    orchestrator._dispatcher_supervisor_task = asyncio.create_task(asyncio.sleep(60))

    with patch(
        "docling_jobkit.orchestrators.ray.orchestrator.serve.delete"
    ) as mock_delete:
        await orchestrator.shutdown()

    dispatcher.stop_dispatching.remote.assert_not_called()
    mock_delete.assert_not_called()


async def test_reconcile_missing_dispatch_hash_marks_failure_and_resyncs() -> None:
    dispatcher = _make_dispatcher()
    metadata = RedisTaskMetadata(
        task_id="task-1",
        tenant_id="tenant-a",
        status=TaskStatus.STARTED,
        task_type=TaskType.CONVERT,
        task_size=4,
        created_at=datetime.datetime.now(datetime.timezone.utc),
        last_update_at=datetime.datetime.now(datetime.timezone.utc),
    )
    dispatcher.redis_manager.get_tenant_active_task_ids = AsyncMock(
        return_value=["task-1"]
    )
    dispatcher.redis_manager.get_task_metadata_model = AsyncMock(return_value=metadata)
    dispatcher.redis_manager.get_task_dispatch_hash = AsyncMock(return_value={})
    dispatcher.redis_manager.finalize_task_failure_atomic = AsyncMock(
        return_value=MagicMock(
            final_status=TaskStatus.FAILURE,
            status_changed=True,
        )
    )
    dispatcher.redis_manager.publish_update = AsyncMock()
    dispatcher.redis_manager.resync_tenant_limits = AsyncMock()

    await dispatcher._reconcile_tenant_active_tasks("tenant-a")

    dispatcher.redis_manager.finalize_task_failure_atomic.assert_awaited_once()
    failure_call = dispatcher.redis_manager.finalize_task_failure_atomic.await_args
    assert failure_call.kwargs["tenant_id"] == "tenant-a"
    assert failure_call.kwargs["task_id"] == "task-1"
    assert failure_call.kwargs["task_size"] == 4
    assert failure_call.kwargs["error_message"] == "Internal processing error."
    failure = failure_call.kwargs["failure"]
    assert failure.category == FailureCategory.INTERNAL
    assert failure.phase == FailurePhase.ORCHESTRATION
    assert failure.details == {"task_size": "4"}
    dispatcher.redis_manager.publish_update.assert_awaited_once()
    dispatcher.redis_manager.resync_tenant_limits.assert_awaited_once_with("tenant-a")


@pytest.mark.asyncio
async def test_reconcile_dispatched_pending_task_within_claim_grace_is_unresolved() -> (
    None
):
    dispatcher = _make_dispatcher()
    metadata = RedisTaskMetadata(
        task_id="task-2",
        tenant_id="tenant-a",
        status=TaskStatus.PENDING,
        task_type=TaskType.CONVERT,
        task_size=3,
        created_at=datetime.datetime.now(datetime.timezone.utc),
        last_update_at=datetime.datetime.now(datetime.timezone.utc),
    )
    recent_dispatch = datetime.datetime.now(datetime.timezone.utc).timestamp() - 5.0
    dispatcher.redis_manager.get_tenant_active_task_ids = AsyncMock(
        return_value=["task-2"]
    )
    dispatcher.redis_manager.get_task_metadata_model = AsyncMock(return_value=metadata)
    dispatcher.redis_manager.get_task_dispatch_hash = AsyncMock(
        return_value={"dispatched_at": str(recent_dispatch), "task_size": "3"}
    )
    dispatcher.redis_manager.finalize_task_failure_atomic = AsyncMock()
    dispatcher.redis_manager.publish_update = AsyncMock()
    dispatcher.redis_manager.resync_tenant_limits = AsyncMock()

    await dispatcher._reconcile_tenant_active_tasks("tenant-a")

    dispatcher.redis_manager.finalize_task_failure_atomic.assert_not_called()
    dispatcher.redis_manager.publish_update.assert_not_called()
    dispatcher.redis_manager.resync_tenant_limits.assert_awaited_once_with("tenant-a")


@pytest.mark.asyncio
async def test_reconcile_stale_execution_lease_marks_failure() -> None:
    dispatcher = _make_dispatcher(
        RayOrchestratorConfig(
            redis_url="redis://localhost:6379/",
            heartbeat_interval=30.0,
        )
    )
    metadata = RedisTaskMetadata(
        task_id="task-stale",
        tenant_id="tenant-a",
        status=TaskStatus.STARTED,
        task_type=TaskType.CONVERT,
        task_size=3,
        created_at=datetime.datetime.now(datetime.timezone.utc),
        last_update_at=datetime.datetime.now(datetime.timezone.utc),
    )
    old_heartbeat = datetime.datetime.now(datetime.timezone.utc).timestamp() - 300.0
    dispatcher.redis_manager.get_tenant_active_task_ids = AsyncMock(
        return_value=["task-stale"]
    )
    dispatcher.redis_manager.get_task_metadata_model = AsyncMock(return_value=metadata)
    dispatcher.redis_manager.get_task_dispatch_hash = AsyncMock(
        return_value={"dispatched_at": "0"}
    )
    dispatcher.redis_manager.get_task_execution_lease = AsyncMock(
        return_value={"replica_id": "replica-1", "heartbeat_at": str(old_heartbeat)}
    )
    dispatcher.redis_manager.finalize_task_failure_atomic = AsyncMock(
        return_value=MagicMock(
            final_status=TaskStatus.FAILURE,
            status_changed=True,
        )
    )
    dispatcher.redis_manager.publish_update = AsyncMock()
    dispatcher.redis_manager.resync_tenant_limits = AsyncMock()

    await dispatcher._reconcile_tenant_active_tasks("tenant-a")

    dispatcher.redis_manager.finalize_task_failure_atomic.assert_awaited_once()
    dispatcher.redis_manager.publish_update.assert_awaited_once()
    dispatcher.redis_manager.resync_tenant_limits.assert_awaited_once_with("tenant-a")


@pytest.mark.asyncio
async def test_reconcile_live_execution_lease_is_left_unresolved() -> None:
    dispatcher = _make_dispatcher(
        RayOrchestratorConfig(
            redis_url="redis://localhost:6379/",
            heartbeat_interval=30.0,
        )
    )
    metadata = RedisTaskMetadata(
        task_id="task-live",
        tenant_id="tenant-a",
        status=TaskStatus.STARTED,
        task_type=TaskType.CONVERT,
        task_size=3,
        created_at=datetime.datetime.now(datetime.timezone.utc),
        last_update_at=datetime.datetime.now(datetime.timezone.utc),
    )
    fresh_heartbeat = datetime.datetime.now(datetime.timezone.utc).timestamp() - 5.0
    dispatcher.redis_manager.get_tenant_active_task_ids = AsyncMock(
        return_value=["task-live"]
    )
    dispatcher.redis_manager.get_task_metadata_model = AsyncMock(return_value=metadata)
    dispatcher.redis_manager.get_task_dispatch_hash = AsyncMock(
        return_value={"dispatched_at": "0"}
    )
    dispatcher.redis_manager.get_task_execution_lease = AsyncMock(
        return_value={"replica_id": "replica-1", "heartbeat_at": str(fresh_heartbeat)}
    )
    dispatcher.redis_manager.finalize_task_failure_atomic = AsyncMock()
    dispatcher.redis_manager.publish_update = AsyncMock()
    dispatcher.redis_manager.resync_tenant_limits = AsyncMock()

    await dispatcher._reconcile_tenant_active_tasks("tenant-a")

    dispatcher.redis_manager.finalize_task_failure_atomic.assert_not_called()
    dispatcher.redis_manager.publish_update.assert_not_called()
    dispatcher.redis_manager.resync_tenant_limits.assert_awaited_once_with("tenant-a")


@pytest.mark.asyncio
async def test_reconcile_started_task_without_lease_within_grace_is_unresolved() -> (
    None
):
    """Narrow window: STARTED set but lease not yet written, still within grace."""
    dispatcher = _make_dispatcher()
    metadata = RedisTaskMetadata(
        task_id="task-no-lease",
        tenant_id="tenant-a",
        status=TaskStatus.STARTED,
        task_type=TaskType.CONVERT,
        task_size=3,
        created_at=datetime.datetime.now(datetime.timezone.utc),
        last_update_at=datetime.datetime.now(datetime.timezone.utc),
    )
    recent_dispatch = datetime.datetime.now(datetime.timezone.utc).timestamp() - 5.0
    dispatcher.redis_manager.get_tenant_active_task_ids = AsyncMock(
        return_value=["task-no-lease"]
    )
    dispatcher.redis_manager.get_task_metadata_model = AsyncMock(return_value=metadata)
    dispatcher.redis_manager.get_task_dispatch_hash = AsyncMock(
        return_value={"dispatched_at": str(recent_dispatch), "task_size": "3"}
    )
    dispatcher.redis_manager.get_task_execution_lease = AsyncMock(return_value=None)
    dispatcher.redis_manager.finalize_task_failure_atomic = AsyncMock()
    dispatcher.redis_manager.publish_update = AsyncMock()
    dispatcher.redis_manager.resync_tenant_limits = AsyncMock()

    await dispatcher._reconcile_tenant_active_tasks("tenant-a")

    dispatcher.redis_manager.finalize_task_failure_atomic.assert_not_called()
    dispatcher.redis_manager.publish_update.assert_not_called()
    dispatcher.redis_manager.resync_tenant_limits.assert_awaited_once_with("tenant-a")


@pytest.mark.asyncio
async def test_resync_tenant_limits_rebuilds_counters_from_canonical_structures() -> (
    None
):
    manager = RedisStateManager(redis_url="redis://localhost:6379/")
    manager.redis = AsyncMock()
    manager.get_tenant_active_task_ids = AsyncMock(return_value=["task-1", "task-2"])
    manager.get_task_metadata_model = AsyncMock(
        side_effect=[
            RedisTaskMetadata(
                task_id="task-1",
                tenant_id="tenant-a",
                status=TaskStatus.STARTED,
                task_type=TaskType.CONVERT,
                task_size=2,
                created_at=datetime.datetime.now(datetime.timezone.utc),
                last_update_at=datetime.datetime.now(datetime.timezone.utc),
            ),
            None,
        ]
    )
    manager.get_tenant_limits = AsyncMock(
        return_value=TenantLimits(max_concurrent_tasks=5)
    )
    manager.get_tenant_queue_size = AsyncMock(return_value=3)
    manager.get_task_execution_lease = AsyncMock(
        side_effect=[
            {"replica_id": "r1", "converter_units": "2"},
            None,
        ]
    )

    limits = await manager.resync_tenant_limits("tenant-a")

    assert limits.active_tasks == 2
    assert limits.queued_tasks == 3
    assert limits.active_documents == 3
    assert limits.converter_units == 2
    manager.redis.hset.assert_awaited_once()


@pytest.mark.asyncio
async def test_refresh_runtime_and_get_health_do_not_race_loop_startup() -> None:
    dispatcher = _make_dispatcher()
    dispatcher.redis_manager.connect = AsyncMock()
    dispatcher.redis_manager.disconnect = AsyncMock()
    release_loop = asyncio.Event()
    loop_start_count = 0

    async def fake_run_dispatch_loop() -> None:
        nonlocal loop_start_count
        loop_start_count += 1
        await release_loop.wait()

    dispatcher._run_dispatch_loop = fake_run_dispatch_loop
    refreshed_config = dispatcher.config.model_copy(update={"dispatcher_interval": 5.0})

    refresh_result, loop_running = await asyncio.gather(
        dispatcher.refresh_runtime("deployment-v2", refreshed_config),
        dispatcher.get_health(),
    )

    assert refresh_result is None
    assert loop_running is True
    assert loop_start_count == 1
    assert dispatcher.deployment_handle == "deployment-v2"
    assert dispatcher.config.dispatcher_interval == 5.0

    release_loop.set()
    await dispatcher.stop_dispatching()


@pytest.mark.asyncio
async def test_dispatcher_stats_preserve_existing_shape() -> None:
    dispatcher = _make_dispatcher()
    dispatcher.last_heartbeat = datetime.datetime(
        2026, 4, 14, 12, 0, tzinfo=datetime.timezone.utc
    )
    dispatcher.redis_manager.get_all_tenants_with_tasks = AsyncMock(
        return_value=["tenant-a"]
    )
    dispatcher.redis_manager.get_tenant_active_task_count = AsyncMock(return_value=2)
    dispatcher.redis_manager.get_tenant_limits = AsyncMock(
        return_value=TenantLimits(max_concurrent_tasks=5)
    )
    dispatcher.redis_manager.get_tenant_queue_size = AsyncMock(return_value=3)

    stats = await dispatcher.get_stats()

    assert stats["active"] is False
    assert stats["last_heartbeat"] == "2026-04-14T12:00:00+00:00"
    assert stats["tenants_with_tasks"] == 1
    assert stats["total_active_tasks"] == 2
    assert stats["total_queued_tasks"] == 3
    assert stats["total_capacity_available"] == 3
    assert stats["tenant_details"] == [
        {
            "tenant_id": "tenant-a",
            "active_tasks": 2,
            "max_concurrent_tasks": 5,
            "queued_tasks": 3,
            "capacity_available": 3,
            "utilization_pct": 40.0,
        }
    ]
    assert stats["ray_serve_deployment"] == {
        "min_replicas": dispatcher.config.min_actors,
        "max_replicas": dispatcher.config.max_actors,
        "target_requests_per_replica": dispatcher.config.target_requests_per_replica,
    }
    assert stats["config"] == {
        "dispatcher_interval": dispatcher.config.dispatcher_interval,
        "max_concurrent_tasks": dispatcher.config.max_concurrent_tasks,
        "max_queued_tasks": dispatcher.config.max_queued_tasks,
    }
