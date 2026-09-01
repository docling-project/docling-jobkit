"""Terminal task progress callbacks in the Ray orchestrator.

Tests pin at-most-once scheduling by the durable terminalization winner and keep
task outcomes independent from document outcomes.
"""

import asyncio
import datetime
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

# These drive the deployment/actor classes in-process with fakes: no Ray cluster
# and no Redis, so they run in CI like the other unit-level Ray tests.
pytest.importorskip("ray")

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.service.callbacks import (
    CallbackSpec,
    ProcessedDocsItem,
    ProgressKind,
    ProgressUpdateProcessed,
)
from docling.datamodel.service.requests import (
    FileSourceRequest as FileSource,
    HttpSourceRequest as HttpSource,
)
from docling.datamodel.service.responses import (
    DoclingTaskResult,
    FailureCategory,
    FailurePhase,
    PublicFailureInfo,
    RemoteTargetResult,
)
from docling.datamodel.service.targets import InBodyTarget, PresignedUrlTarget
from docling.datamodel.service.tasks import TaskType

from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.dispatcher import RayTaskDispatcher
from docling_jobkit.orchestrators.ray.models import (
    ConverterTaskResult,
    RedisTaskMetadata,
    TaskTerminalizationResult,
)
from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager
from docling_jobkit.orchestrators.ray.serve_deployment import (
    DoclingProcessorCoordinatorDeployment,
)
from docling_jobkit.orchestrators.serialization import dump_model_with_secrets


class _RecordingInvoker:
    """Captures invoke_callbacks_async calls instead of firing HTTP requests."""

    calls: list[dict[str, Any]] = []

    def invoke_callbacks_async(self, **kwargs: Any) -> None:
        _RecordingInvoker.calls.append(kwargs)


@pytest.fixture
def recorded_callbacks(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    _RecordingInvoker.calls = []
    monkeypatch.setattr(
        "docling_jobkit.orchestrators.completion_callbacks.CallbackInvoker",
        _RecordingInvoker,
    )
    return _RecordingInvoker.calls


class _FakeRedisManager:
    """Minimal RedisStateManager stand-in for the terminal-failure paths."""

    def __init__(self, *, status_changed: bool = True) -> None:
        self._status_changed = status_changed
        self.published: list[Any] = []

    async def mark_task_started(self, **kwargs: Any) -> None:
        return None

    async def write_task_execution_lease(self, **kwargs: Any) -> None:
        return None

    async def update_task_execution_heartbeat(self, task_id: str) -> bool:
        return True

    async def get_tenant_limits(self, tenant_id: str) -> SimpleNamespace:
        return SimpleNamespace(max_concurrent_tasks=4)

    async def acquire_converter_unit(
        self, tenant_id: str, task_id: str, ceiling: int
    ) -> int:
        return 1

    async def release_converter_units(
        self, tenant_id: str, task_id: str, count: int
    ) -> int:
        return count

    async def finalize_task_failure_atomic(
        self, **kwargs: Any
    ) -> TaskTerminalizationResult:
        return TaskTerminalizationResult(
            final_status=TaskStatus.FAILURE,
            status_changed=self._status_changed,
            capacity_released=True,
        )

    async def finalize_task_success_atomic(
        self, **kwargs: Any
    ) -> TaskTerminalizationResult:
        return TaskTerminalizationResult(
            final_status=TaskStatus.SUCCESS,
            status_changed=self._status_changed,
            capacity_released=True,
            result_key="task:t-callback:result",
        )

    async def publish_update(self, update: Any) -> None:
        self.published.append(update)

    async def update_tenant_stats(self, tenant_id: str, **kwargs: Any) -> None:
        return None


def _task_with_callback(
    *, sources: list[Any] | None = None, target: Any | None = None
) -> Task:
    return Task(
        task_id="t-callback",
        sources=sources or [FileSource(base64_string="ZHVtbXk=", filename="doc.md")],
        target=target or InBodyTarget(),
        callbacks=[CallbackSpec(url="https://example.invalid/hook")],
        metadata={"tenant_id": "tenant-a"},
    )


def _make_coordinator(
    config: RayOrchestratorConfig,
    converter_handle: object,
    manager: _FakeRedisManager,
    monkeypatch: pytest.MonkeyPatch,
) -> Any:
    monkeypatch.setattr(
        "docling_jobkit.orchestrators.ray.serve_deployment.serve.get_replica_context",
        lambda: type("ReplicaContext", (), {"replica_id": "coordinator-1"})(),
    )
    # The passthrough path pre-counts documents by opening a source processor;
    # short-circuit that so the test never touches connector I/O.
    monkeypatch.setattr(
        "docling_jobkit.orchestrators.ray.serve_deployment._count_documents_for_task",
        lambda task, allow_external_plugins=False: len(task.sources),
    )
    deployment_cls = getattr(
        DoclingProcessorCoordinatorDeployment, "func_or_class", None
    )
    assert deployment_cls is not None
    converter_manager_config = MagicMock()
    converter_manager_config.allow_external_plugins = False
    deployment = deployment_cls(
        converter_manager_config=converter_manager_config,
        config=config,
        redis_url=config.redis_url,
        converter_handle=converter_handle,
    )
    deployment.redis_manager = manager  # type: ignore[assignment]
    return deployment


def _make_dispatcher(manager: _FakeRedisManager) -> Any:
    # Unwrap the @ray.remote decorator and skip __init__ (it would open Redis);
    # only the terminal-failure branches are under test.
    dispatcher_cls = getattr(
        RayTaskDispatcher, "__ray_actor_class__", RayTaskDispatcher
    )
    dispatcher = dispatcher_cls.__new__(dispatcher_cls)
    dispatcher.redis_manager = manager
    dispatcher.config = RayOrchestratorConfig(redis_url="redis://localhost:6379/")
    dispatcher._wake_event = asyncio.Event()
    return dispatcher


def _only_task_completed(calls: list[dict[str, Any]]) -> Any:
    assert len(calls) == 1, f"expected exactly one callback, got {len(calls)}"
    progress = calls[0]["progress"]
    assert progress.kind == ProgressKind.TASK_COMPLETED
    return progress


@pytest.mark.asyncio
async def test_converter_exception_emits_terminal_failure_callback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorded_callbacks: list[dict[str, Any]],
) -> None:
    """The reported bug: conversion raises, so no callback ever fired."""

    class ExplodingConverter:
        async def remote(self, request: object, **_: object) -> Any:
            raise FileNotFoundError(
                "Model 'HuggingFaceTB/SmolVLM-256M-Instruct' not found in artifacts_path."
            )

    manager = _FakeRedisManager()
    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/", scratch_dir=tmp_path
    )
    deployment = _make_coordinator(
        config,
        SimpleNamespace(process_converter_request=ExplodingConverter()),
        manager,
        monkeypatch,
    )

    with pytest.raises(FileNotFoundError):
        await deployment.process_task(
            _task_with_callback(
                sources=[HttpSource(url="https://example.invalid/doc.pdf")],
                target=PresignedUrlTarget(),
            )
        )

    progress = _only_task_completed(recorded_callbacks)
    assert recorded_callbacks[0]["task_id"] == "t-callback"
    assert progress.task_status == "failure"
    assert progress.failure.category == FailureCategory.INTERNAL
    assert progress.failure.message == "Internal processing error."
    assert not hasattr(progress, "docs")
    # The durable update still went out — the callback does not replace it.
    assert manager.published and manager.published[0].task_status == TaskStatus.FAILURE


@pytest.mark.parametrize(
    ("num_succeeded", "num_failed"),
    [(2, 0), (1, 1), (0, 2)],
)
@pytest.mark.asyncio
async def test_complete_document_results_emit_task_success(
    num_succeeded: int,
    num_failed: int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorded_callbacks: list[dict[str, Any]],
) -> None:
    class CompletedConverter:
        async def remote(self, request: object, **_: object) -> ConverterTaskResult:
            return ConverterTaskResult(
                task_result=DoclingTaskResult(
                    result=RemoteTargetResult(),
                    processing_time=0.1,
                    num_converted=2,
                    num_succeeded=num_succeeded,
                    num_partially_succeeded=0,
                    num_failed=num_failed,
                )
            )

    manager = _FakeRedisManager()
    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/", scratch_dir=tmp_path
    )
    deployment = _make_coordinator(
        config,
        SimpleNamespace(process_converter_request=CompletedConverter()),
        manager,
        monkeypatch,
    )
    task = _task_with_callback(
        sources=[
            FileSource(base64_string="Zmlyc3Q=", filename="first.md"),
            FileSource(base64_string="c2Vjb25k", filename="second.md"),
        ]
    )

    result = await deployment.process_task(task)

    assert (result.num_succeeded, result.num_failed) == (num_succeeded, num_failed)
    progress = _only_task_completed(recorded_callbacks)
    assert progress.task_status == "success"
    assert progress.failure is None


@pytest.mark.asyncio
async def test_partial_document_progress_then_abort_keeps_only_known_outcomes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorded_callbacks: list[dict[str, Any]],
) -> None:
    document_progress = ProgressUpdateProcessed(
        num_processed=1,
        num_succeeded=1,
        num_partially_succeeded=0,
        num_failed=0,
        docs=[
            ProcessedDocsItem(
                source="known.md",
                status=ConversionStatus.SUCCESS,
            )
        ],
    )
    recorded_callbacks.append({"task_id": "t-callback", "progress": document_progress})

    class ExplodingConverter:
        async def remote(self, request: object, **_: object) -> Any:
            raise RuntimeError("converter crashed after partial progress")

    manager = _FakeRedisManager()
    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/", scratch_dir=tmp_path
    )
    deployment = _make_coordinator(
        config,
        SimpleNamespace(process_converter_request=ExplodingConverter()),
        manager,
        monkeypatch,
    )

    with pytest.raises(RuntimeError):
        await deployment.process_task(_task_with_callback())

    assert recorded_callbacks[0]["progress"] is document_progress
    terminal = recorded_callbacks[1]["progress"]
    assert terminal.kind == ProgressKind.TASK_COMPLETED
    assert terminal.task_status == "failure"
    assert not hasattr(terminal, "docs")


@pytest.mark.asyncio
async def test_no_callback_when_task_was_already_terminalized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorded_callbacks: list[dict[str, Any]],
) -> None:
    """Emission is gated on winning the atomic terminalization, so it fires once."""

    class ExplodingConverter:
        async def remote(self, request: object, **_: object) -> Any:
            raise RuntimeError("converter crashed")

    manager = _FakeRedisManager(status_changed=False)
    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/", scratch_dir=tmp_path
    )
    deployment = _make_coordinator(
        config,
        SimpleNamespace(process_converter_request=ExplodingConverter()),
        manager,
        monkeypatch,
    )

    with pytest.raises(RuntimeError):
        await deployment.process_task(_task_with_callback())

    assert recorded_callbacks == []


@pytest.mark.asyncio
async def test_client_actionable_failure_emits_terminal_callback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorded_callbacks: list[dict[str, Any]],
) -> None:
    manager = _FakeRedisManager()
    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/", scratch_dir=tmp_path
    )
    deployment = _make_coordinator(config, SimpleNamespace(), manager, monkeypatch)

    await deployment._finalize_client_actionable_task_failure(
        task=_task_with_callback(),
        tenant_id="tenant-a",
        task_size=1,
        failure=PublicFailureInfo(
            category=FailureCategory.SOURCE_UNAVAILABLE,
            message="Source document could not be reached.",
            retryable=True,
            phase=FailurePhase.SOURCE_ENUMERATION,
        ),
    )

    progress = _only_task_completed(recorded_callbacks)
    assert progress.task_status == "failure"
    assert progress.failure.message == "Source document could not be reached."


@pytest.mark.asyncio
async def test_dispatcher_emits_terminal_callback_when_it_terminalizes(
    monkeypatch: pytest.MonkeyPatch,
    recorded_callbacks: list[dict[str, Any]],
) -> None:
    """Covers failures the replica could not report itself (timeout, replica death)."""
    manager = _FakeRedisManager()
    dispatcher = _make_dispatcher(manager)

    class DeadReplicaCall:
        def remote(self, task: Task) -> Any:
            raise TimeoutError("replica did not respond")

    dispatcher.deployment_handle = SimpleNamespace(  # type: ignore[attr-defined]
        process_task=DeadReplicaCall()
    )

    await dispatcher._process_task_async(_task_with_callback(), "tenant-a")

    progress = _only_task_completed(recorded_callbacks)
    assert progress.task_status == "failure"
    assert progress.failure.category == FailureCategory.TIMEOUT
    assert manager.published and manager.published[0].task_status == TaskStatus.FAILURE


def _started_metadata(task_size: int = 3) -> RedisTaskMetadata:
    now = datetime.datetime.now(datetime.timezone.utc)
    return RedisTaskMetadata(
        task_id="t-callback",
        tenant_id="tenant-a",
        status=TaskStatus.STARTED,
        task_type=TaskType.CONVERT,
        task_size=task_size,
        created_at=now,
        last_update_at=now,
        started_at=now,
    )


@pytest.mark.asyncio
async def test_reconciled_orphan_notifies_with_persisted_callbacks(
    recorded_callbacks: list[dict[str, Any]],
) -> None:
    """A task whose replica died is only ever reported by reconciliation."""
    manager = _FakeRedisManager()
    dispatcher = _make_dispatcher(manager)

    await dispatcher._fail_reconciled_task(
        tenant_id="tenant-a",
        task_id="t-callback",
        metadata=_started_metadata(task_size=3),
        callbacks=[CallbackSpec(url="https://example.invalid/hook")],
        error_message="Task orphaned: replica execution lease stale during reconciliation",
    )

    progress = _only_task_completed(recorded_callbacks)
    assert progress.task_status == "failure"
    assert progress.failure.phase == FailurePhase.ORCHESTRATION


@pytest.mark.asyncio
async def test_reconciled_orphan_without_persisted_callbacks_is_silent(
    recorded_callbacks: list[dict[str, Any]],
) -> None:
    """No dispatch hash means no callback specs survived — terminalize only."""
    manager = _FakeRedisManager()
    dispatcher = _make_dispatcher(manager)

    await dispatcher._fail_reconciled_task(
        tenant_id="tenant-a",
        task_id="t-callback",
        metadata=_started_metadata(),
        callbacks=[],
        error_message="Task orphaned: processing state missing during reconciliation",
    )

    assert recorded_callbacks == []
    assert manager.published and manager.published[0].task_status == TaskStatus.FAILURE


class _CapturingPipeline:
    """Fake transactional pipeline recording the dispatch-hash write."""

    def __init__(self, captured: dict[str, Any]) -> None:
        self.captured = captured

    async def __aenter__(self) -> "_CapturingPipeline":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def watch(self, *keys: str) -> None:
        return None

    async def unwatch(self) -> None:
        return None

    def multi(self) -> None:
        return None

    def lpop(self, key: str) -> None:
        return None

    def sadd(self, key: str, value: str) -> None:
        return None

    def hincrby(self, key: str, field: str, amount: int) -> None:
        return None

    def hset(self, key: str, mapping: dict[str, str]) -> None:
        self.captured[key] = mapping

    def expire(self, key: str, ttl: int) -> None:
        self.captured.setdefault("ttls", {})[key] = ttl

    async def execute(self) -> list[Any]:
        return []


class _CapturingRedis:
    def __init__(self, queued_task_json: str, captured: dict[str, Any]) -> None:
        self.queued_task_json = queued_task_json
        self.captured = captured

    async def lindex(self, key: str, index: int) -> str:
        return self.queued_task_json

    def pipeline(self, transaction: bool = True) -> _CapturingPipeline:
        assert transaction is True
        return _CapturingPipeline(self.captured)


@pytest.mark.asyncio
async def test_dispatch_persists_callbacks_for_reconciliation() -> None:
    """The queue entry is consumed at dispatch; the dispatch hash must carry the specs."""
    task = _task_with_callback()
    task.callbacks = [
        CallbackSpec(
            url="https://example.invalid/hook",
            headers={"Authorization": "Bearer token"},
        )
    ]
    captured: dict[str, Any] = {}
    manager = RedisStateManager(redis_url="redis://localhost:6379/")
    manager.redis = _CapturingRedis(  # type: ignore[assignment]
        json.dumps(dump_model_with_secrets(task)), captured
    )

    dispatched = await manager.dispatch_task_atomic("tenant-a", "t-callback", 1)

    assert dispatched is True
    dispatch_state = captured["task:t-callback:dispatch"]
    decoded = RedisStateManager.decode_dispatch_callbacks(dispatch_state)
    assert [str(spec.url) for spec in decoded] == ["https://example.invalid/hook"]
    assert decoded[0].headers == {"Authorization": "Bearer token"}
    # Credentials live only under the TTL-bounded key that terminalization deletes.
    assert captured["ttls"]["task:t-callback:dispatch"] == manager.processing_ttl


def test_decode_dispatch_callbacks_tolerates_legacy_and_corrupt_state() -> None:
    assert RedisStateManager.decode_dispatch_callbacks({}) == []
    assert RedisStateManager.decode_dispatch_callbacks({"callbacks": "not-json"}) == []
    assert RedisStateManager.decode_dispatch_callbacks({"callbacks": "[{}]"}) == []


@pytest.mark.asyncio
async def test_target_write_error_in_build_task_result_returns_converter_failure_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TargetWriteError raised inside _build_task_result must be caught by the
    converter's process_converter_request and returned as a ConverterFailureResult,
    not propagate as an unhandled exception that Ray logs as 'Request failed'."""
    from unittest.mock import patch

    from docling_jobkit.orchestrators.ray.models import (
        ConverterFailureResult,
        PassthroughTaskRequest,
    )
    from docling_jobkit.orchestrators.ray.serve_deployment import (
        DoclingProcessorConverterDeployment,
    )
    from docling_jobkit.public_errors import TargetWriteError

    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/", scratch_dir=tmp_path
    )
    monkeypatch.setattr(
        "docling_jobkit.orchestrators.ray.serve_deployment.serve.get_replica_context",
        lambda: type("ReplicaContext", (), {"replica_id": "converter-1"})(),
    )
    converter_manager_config = MagicMock()
    converter_manager_config.allow_external_plugins = False
    converter_cls = getattr(DoclingProcessorConverterDeployment, "func_or_class", None)
    assert converter_cls is not None

    with patch(
        "docling_jobkit.orchestrators.ray.serve_deployment.DoclingConverterManager"
    ):
        converter = converter_cls(
            converter_manager_config=converter_manager_config,
            config=config,
        )

    task = _task_with_callback(
        sources=[HttpSource(url="https://example.invalid/doc.pdf")],
        target=PresignedUrlTarget(),
    )
    request = PassthroughTaskRequest(task=task)

    kafka_error = TargetWriteError(
        "Could not fetch Kafka metadata for topic 'docling.chunks': "
        "Local: Broker transport failure"
    )

    # Patch _build_task_result on the instance to simulate target write failing
    # after conversion succeeds — the path that was previously unhandled.
    with (
        patch.object(converter, "_run_with_retry", return_value=MagicMock()),
        patch.object(converter, "_build_task_result", side_effect=kafka_error),
        patch(
            "docling_jobkit.orchestrators.ray.serve_deployment._to_exportable_documents",
            return_value=[],
        ),
    ):
        result = await converter.process_converter_request(request, tenant_id="default")

    assert isinstance(result, ConverterFailureResult)
    assert result.failure.category.value == "target_unavailable"
    assert result.failure.retryable is False
    assert (
        result.failure.message == "Result could not be written to the requested target."
    )
    assert result.failure.details.get("target_kind") == PresignedUrlTarget().kind


@pytest.mark.asyncio
async def test_target_write_error_details_contain_correct_target_kind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorded_callbacks: list[dict[str, Any]],
) -> None:
    """failure.details['target_kind'] must reflect the actual target, not 'NoneType'.

    task.target is always None at runtime (normalised into task.targets by the
    model validator), so the old getattr(task.target, ...) lookup always produced
    'NoneType'.  The fix reads task.targets[0] instead.

    The coordinator re-raises after emitting the callback, so pytest.raises is used
    and the published TaskUpdate is inspected for the correct target_kind detail.
    """
    from docling_jobkit.public_errors import TargetWriteError

    class TargetWriteConverter:
        async def remote(self, request: object, **_: object) -> Any:
            raise TargetWriteError("Broker transport failure")

    manager = _FakeRedisManager()
    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/", scratch_dir=tmp_path
    )
    task = _task_with_callback(
        sources=[HttpSource(url="https://example.invalid/doc.pdf")],
        target=PresignedUrlTarget(),
    )
    deployment = _make_coordinator(
        config,
        SimpleNamespace(process_converter_request=TargetWriteConverter()),
        manager,
        monkeypatch,
    )

    with pytest.raises(TargetWriteError):
        await deployment.process_task(task)

    progress = _only_task_completed(recorded_callbacks)
    assert progress.failure.category == FailureCategory.TARGET_UNAVAILABLE
    assert progress.failure.details.get("target_kind") == PresignedUrlTarget().kind
