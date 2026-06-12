"""Coordinator fan-out loop tests for converter-unit gating.

These drive the *real* async fan-out loops in
`DoclingProcessorCoordinatorDeployment` (`_process_s3_fanout_task`,
`_run_slice_plan`) with a fake `redis_manager` that faithfully tracks in-flight
converter units and records peak concurrency. They prove the highest-risk
behavior the in-memory Lua tests cannot: that the loops never exceed the tenant
ceiling, drain-and-refill to process every item, and never leak a unit across
success, per-child failure, and terminalization/cancellation.
"""

import asyncio
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("ray")

from docling.datamodel.base_models import ConversionStatus, OutputFormat
from docling.datamodel.service.callbacks import ProcessedDocsItem
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.sources import FileSource, S3Coordinates
from docling.datamodel.service.targets import InBodyTarget, S3Target

from docling_jobkit.connectors.source_processor import DocumentChunk, SourceDocumentRef
from docling_jobkit.datamodel.result import DoclingTaskResult, RemoteTargetResult
from docling_jobkit.datamodel.task import Task
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.models import ConverterTaskResult
from docling_jobkit.orchestrators.ray.serve_deployment import (
    DoclingProcessorCoordinatorDeployment,
    _build_slice_plan,
)


class _FakeUnitManager:
    """Faithful in-process stand-in for RedisStateManager unit accounting.

    `in_flight` is units held (acquired-not-released) == concurrent converter
    calls, since the loop acquires before launching and releases after a child
    completes. `max_in_flight` is the observed peak — it must never exceed the
    ceiling.
    """

    def __init__(self, ceiling: int, fail_after: int | None = None) -> None:
        self.ceiling = ceiling
        self.in_flight = 0
        self.max_in_flight = 0
        self.total_acquired = 0
        self.total_released = 0
        self._fail_after = fail_after  # return -1 once this many grants happened

    async def get_tenant_limits(self, tenant_id: str) -> SimpleNamespace:
        return SimpleNamespace(max_concurrent_tasks=self.ceiling)

    async def acquire_converter_unit(
        self, tenant_id: str, task_id: str, ceiling: int
    ) -> int:
        if self._fail_after is not None and self.total_acquired >= self._fail_after:
            return -1
        if self.in_flight >= ceiling:
            return 0
        self.in_flight += 1
        self.total_acquired += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        return 1

    async def release_converter_units(
        self, tenant_id: str, task_id: str, count: int
    ) -> int:
        rel = min(count, self.in_flight)
        self.in_flight -= rel
        self.total_released += rel
        return rel


def _make_coordinator(
    config: RayOrchestratorConfig,
    converter_handle: object,
    manager: _FakeUnitManager,
    monkeypatch: pytest.MonkeyPatch,
) -> DoclingProcessorCoordinatorDeployment:
    monkeypatch.setattr(
        "docling_jobkit.orchestrators.ray.serve_deployment.serve.get_replica_context",
        lambda: type("ReplicaContext", (), {"replica_id": "coordinator-1"})(),
    )
    deployment_cls = getattr(
        DoclingProcessorCoordinatorDeployment, "func_or_class", None
    )
    assert deployment_cls is not None
    deployment = deployment_cls(
        converter_manager_config=MagicMock(),
        config=config,
        redis_url=config.redis_url,
        converter_handle=converter_handle,
    )
    deployment.redis_manager = manager  # type: ignore[assignment]
    return deployment


def _s3_refs(n: int) -> list[SourceDocumentRef]:
    return [
        SourceDocumentRef(
            id={"key": f"incoming/{i}.pdf"},
            source_index=i,
            source_uri=f"s3://source-bucket/incoming/{i}.pdf",
            filename=f"incoming/{i}.pdf",
        )
        for i in range(n)
    ]


def _s3_task(task_id: str) -> Task:
    return Task(
        task_id=task_id,
        sources=[
            S3Coordinates(
                endpoint="127.0.0.1:9000",
                verify_ssl=False,
                access_key="minioadmin",
                secret_key="minioadmin",
                bucket="source-bucket",
                key_prefix="incoming/",
            )
        ],
        target=S3Target(
            endpoint="127.0.0.1:9000",
            verify_ssl=False,
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket="target-bucket",
            key_prefix="out/",
        ),
        convert_options=ConvertDocumentsOptions(to_formats=[OutputFormat.JSON]),
    )


def _ok_result(filename: str) -> ConverterTaskResult:
    return ConverterTaskResult(
        task_result=DoclingTaskResult(
            result=RemoteTargetResult(),
            processing_time=0.01,
            num_succeeded=1,
            num_partially_succeeded=0,
            num_failed=0,
            num_converted=1,
        ),
        processed_docs=[
            ProcessedDocsItem(source=filename, status=ConversionStatus.SUCCESS)
        ],
    )


def _patch_source_processor(
    monkeypatch: pytest.MonkeyPatch, refs: list[SourceDocumentRef]
) -> None:
    class FakeSourceProcessor:
        def __init__(self, source: object) -> None:
            self.source = source

        def __enter__(self) -> "FakeSourceProcessor":
            return self

        def __exit__(self, *exc: object) -> bool:
            return False

        def iterate_document_chunks(self, chunk_size: int):
            for idx, ref in enumerate(refs):
                yield DocumentChunk(source=self.source, refs=[ref], chunk_index=idx)

    monkeypatch.setattr(
        "docling_jobkit.orchestrators.ray.serve_deployment.get_source_processor",
        lambda source: FakeSourceProcessor(source),
    )


async def _wait_for(predicate, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached before timeout")
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_s3_fanout_never_exceeds_ceiling_and_processes_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ceiling = 2
    total = 5
    refs = _s3_refs(total)
    _patch_source_processor(monkeypatch, refs)
    gate = asyncio.Event()

    class GatedConverter:
        async def remote(self, request: object) -> ConverterTaskResult:
            await gate.wait()  # hold children so concurrency can build to the cap
            return _ok_result(request.chunk.refs[0].filename)

    manager = _FakeUnitManager(ceiling=ceiling)
    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/", scratch_dir=tmp_path
    )
    deployment = _make_coordinator(
        config,
        SimpleNamespace(process_converter_request=GatedConverter()),
        manager,
        monkeypatch,
    )

    loop_task = asyncio.create_task(
        deployment._process_s3_fanout_task(_s3_task("t-ceiling"), time.monotonic())
    )
    # Loop fills to the ceiling and then blocks on child completion.
    await _wait_for(lambda: manager.in_flight == ceiling)
    await asyncio.sleep(0.05)  # give the loop a chance to (wrongly) over-acquire
    assert manager.in_flight == ceiling
    gate.set()

    result = await loop_task

    assert result.num_converted == total
    assert manager.max_in_flight == ceiling  # never exceeded, and reached, the cap
    assert manager.in_flight == 0  # every unit released — no leak
    assert manager.total_acquired == manager.total_released == total


@pytest.mark.asyncio
async def test_s3_fanout_releases_unit_on_child_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    refs = _s3_refs(4)
    _patch_source_processor(monkeypatch, refs)

    class FlakyConverter:
        async def remote(self, request: object) -> ConverterTaskResult:
            if request.chunk.chunk_index == 1:
                raise RuntimeError("boom")
            return _ok_result(request.chunk.refs[0].filename)

    manager = _FakeUnitManager(ceiling=3)
    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/", scratch_dir=tmp_path
    )
    deployment = _make_coordinator(
        config,
        SimpleNamespace(process_converter_request=FlakyConverter()),
        manager,
        monkeypatch,
    )

    result = await deployment._process_s3_fanout_task(
        _s3_task("t-fail"), time.monotonic()
    )

    assert result.num_failed == 1
    assert result.num_succeeded == 3
    # The failed child's unit is released too — acquired == released, nothing leaked.
    assert manager.in_flight == 0
    assert manager.total_acquired == manager.total_released == 4


@pytest.mark.asyncio
async def test_s3_fanout_terminalization_raises_and_releases_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    refs = _s3_refs(6)
    _patch_source_processor(monkeypatch, refs)
    gate = asyncio.Event()

    class GatedConverter:
        async def remote(self, request: object) -> ConverterTaskResult:
            await gate.wait()
            return _ok_result(request.chunk.refs[0].filename)

    # Grant 3 units, then acquire returns -1 (task terminalized/reconciled).
    manager = _FakeUnitManager(ceiling=10, fail_after=3)
    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/", scratch_dir=tmp_path
    )
    deployment = _make_coordinator(
        config,
        SimpleNamespace(process_converter_request=GatedConverter()),
        manager,
        monkeypatch,
    )

    with pytest.raises(RuntimeError, match="terminalized"):
        await deployment._process_s3_fanout_task(_s3_task("t-term"), time.monotonic())

    # The 3 in-flight children were cancelled and their units released — no leak.
    assert manager.in_flight == 0
    assert manager.total_acquired == 3
    assert manager.total_released == 3


@pytest.mark.asyncio
async def test_slice_plan_never_exceeds_ceiling_and_collects_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ceiling = 2
    slice_plan = _build_slice_plan(
        total_pages=10, requested_page_range=(1, 10), max_page_slice_size=2
    )
    total_slices = len(slice_plan.slices)
    assert total_slices > ceiling  # need more work than budget to exercise refill
    gate = asyncio.Event()

    class GatedConverter:
        async def remote(self, request: object) -> str:
            await gate.wait()
            return f"objref-{request.slice_index}"  # stand-in ObjectRef

    manager = _FakeUnitManager(ceiling=ceiling)
    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/", scratch_dir=tmp_path
    )
    deployment = _make_coordinator(
        config,
        SimpleNamespace(process_converter_request=GatedConverter()),
        manager,
        monkeypatch,
    )
    task = Task(
        task_id="t-slice",
        sources=[FileSource(filename="x.pdf", base64_string="")],
        target=InBodyTarget(),
        convert_options=ConvertDocumentsOptions(to_formats=[OutputFormat.JSON]),
    )

    loop_task = asyncio.create_task(
        deployment._run_slice_plan(
            artifact_ref=object(),
            filename="x.pdf",
            slice_plan=slice_plan,
            options=ConvertDocumentsOptions(to_formats=[OutputFormat.JSON]),
            task=task,
        )
    )
    await _wait_for(lambda: manager.in_flight == ceiling)
    await asyncio.sleep(0.05)
    assert manager.in_flight == ceiling
    gate.set()

    refs = await loop_task

    assert len(refs) == total_slices
    assert manager.max_in_flight == ceiling
    assert manager.in_flight == 0
    assert manager.total_acquired == manager.total_released == total_slices
