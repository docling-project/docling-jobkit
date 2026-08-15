"""Terminal-failure progress callbacks in the Ray orchestrator.

A task that dies before or inside conversion never reaches the result-processing
path, which is the only place the Ray orchestrator emits progress callbacks. These
tests pin the contract that such a task still notifies its callbacks exactly once,
from whichever component actually terminalized it durably.
"""

import asyncio
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
    ProgressKind,
)
from docling.datamodel.service.requests import FileSourceRequest as FileSource
from docling.datamodel.service.responses import (
    FailureCategory,
    FailurePhase,
    PublicFailureInfo,
)
from docling.datamodel.service.targets import InBodyTarget

from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.dispatcher import RayTaskDispatcher
from docling_jobkit.orchestrators.ray.models import TaskTerminalizationResult
from docling_jobkit.orchestrators.ray.serve_deployment import (
    DoclingProcessorCoordinatorDeployment,
)


class _RecordingInvoker:
    """Captures invoke_callbacks_async calls instead of firing HTTP requests."""

    calls: list[dict[str, Any]] = []

    def invoke_callbacks_async(self, **kwargs: Any) -> None:
        _RecordingInvoker.calls.append(kwargs)


@pytest.fixture
def recorded_callbacks(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    _RecordingInvoker.calls = []
    monkeypatch.setattr(
        "docling_jobkit.orchestrators.failure_callbacks.CallbackInvoker",
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

    async def publish_update(self, update: Any) -> None:
        self.published.append(update)

    async def update_tenant_stats(self, tenant_id: str, **kwargs: Any) -> None:
        return None


def _task_with_callback() -> Task:
    return Task(
        task_id="t-callback",
        sources=[FileSource(base64_string="ZHVtbXk=", filename="doc.md")],
        target=InBodyTarget(),
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
        lambda task, allow_external_plugins=False: 1,
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


def _only_update_processed(calls: list[dict[str, Any]]) -> Any:
    assert len(calls) == 1, f"expected exactly one callback, got {len(calls)}"
    progress = calls[0]["progress"]
    assert progress.kind == ProgressKind.UPDATE_PROCESSED
    return progress


@pytest.mark.asyncio
async def test_converter_exception_emits_terminal_failure_callback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorded_callbacks: list[dict[str, Any]],
) -> None:
    """The reported bug: conversion raises, so no callback ever fired."""

    class ExplodingConverter:
        async def remote(self, request: object) -> Any:
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
        await deployment.process_task(_task_with_callback())

    progress = _only_update_processed(recorded_callbacks)
    assert recorded_callbacks[0]["task_id"] == "t-callback"
    assert progress.num_processed == 1
    assert progress.num_failed == 1
    assert progress.num_succeeded == 0
    assert [doc.status for doc in progress.docs] == [ConversionStatus.FAILURE]
    assert progress.docs[0].source == "doc.md"
    # The durable update still went out — the callback does not replace it.
    assert manager.published and manager.published[0].task_status == TaskStatus.FAILURE


@pytest.mark.asyncio
async def test_no_callback_when_task_was_already_terminalized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recorded_callbacks: list[dict[str, Any]],
) -> None:
    """Emission is gated on winning the atomic terminalization, so it fires once."""

    class ExplodingConverter:
        async def remote(self, request: object) -> Any:
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

    progress = _only_update_processed(recorded_callbacks)
    assert progress.num_failed == 1
    assert progress.docs[0].error == "Source document could not be reached."


@pytest.mark.asyncio
async def test_dispatcher_emits_terminal_callback_when_it_terminalizes(
    monkeypatch: pytest.MonkeyPatch,
    recorded_callbacks: list[dict[str, Any]],
) -> None:
    """Covers failures the replica could not report itself (timeout, replica death)."""
    # Unwrap the @ray.remote decorator and skip __init__ (it would open Redis);
    # only the failure branch of _process_task_async is under test.
    dispatcher_cls = getattr(
        RayTaskDispatcher, "__ray_actor_class__", RayTaskDispatcher
    )
    dispatcher = dispatcher_cls.__new__(dispatcher_cls)
    manager = _FakeRedisManager()
    dispatcher.redis_manager = manager  # type: ignore[attr-defined]
    dispatcher.config = RayOrchestratorConfig(redis_url="redis://localhost:6379/")
    dispatcher._wake_event = asyncio.Event()

    class DeadReplicaCall:
        def remote(self, task: Task) -> Any:
            raise TimeoutError("replica did not respond")

    dispatcher.deployment_handle = SimpleNamespace(  # type: ignore[attr-defined]
        process_task=DeadReplicaCall()
    )

    await dispatcher._process_task_async(_task_with_callback(), "tenant-a")

    progress = _only_update_processed(recorded_callbacks)
    assert progress.num_failed == 1
    assert manager.published and manager.published[0].task_status == TaskStatus.FAILURE
