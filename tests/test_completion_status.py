import asyncio
import os
from contextlib import suppress
from unittest.mock import patch
from uuid import uuid4

import msgpack
import pytest
from rq.job import Job, JobStatus
from rq.results import Result

from docling.datamodel.service.targets import InBodyTarget

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.result import DoclingTaskResult, RemoteTargetResult
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.local.orchestrator import (
    LocalOrchestrator,
    LocalOrchestratorConfig,
)
from docling_jobkit.orchestrators.local.worker import AsyncLocalWorker
from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestrator,
    RQOrchestratorConfig,
    _TaskUpdate,
)
from docling_jobkit.orchestrators.rq.worker import _run_docling_task
from docling_jobkit.orchestrators.serialization import make_msgpack_safe


@pytest.fixture(params=[(0, 0), (1, 0), (0, 1)])
def completion_result(request):
    succeeded, partial = request.param
    return DoclingTaskResult(
        result=RemoteTargetResult(),
        processing_time=0.1,
        num_converted=2,
        num_succeeded=succeeded,
        num_partially_succeeded=partial,
        num_failed=2 - succeeded - partial,
    )


@pytest.fixture
def expected_status(completion_result):
    return (
        TaskStatus.FAILURE if completion_result.num_failed == 2 else TaskStatus.SUCCESS
    )


async def test_local_completion_retains_result(
    tmp_path, completion_result, expected_status
):
    cm = DoclingConverterManager(config=DoclingConverterManagerConfig())
    orch = LocalOrchestrator(
        config=LocalOrchestratorConfig(scratch_dir=tmp_path), converter_manager=cm
    )
    task = await orch.enqueue(
        sources=[], target=InBodyTarget(), convert_options=ConvertDocumentsOptions()
    )
    worker = AsyncLocalWorker(0, orch, True, tmp_path)
    with (
        patch.object(cm, "convert_documents", return_value=iter(())),
        patch(
            "docling_jobkit.orchestrators.local.worker.process_exportable_results",
            return_value=completion_result,
        ),
    ):
        running = asyncio.create_task(worker.loop())
        try:
            await asyncio.wait_for(orch.task_queue.join(), timeout=10)
        finally:
            running.cancel()
            with suppress(asyncio.CancelledError):
                await running
    assert await orch.task_result(task.task_id) == completion_result
    assert task.task_status == expected_status


@pytest.fixture
async def rq_orchestrator():
    prefix = f"test-status-{uuid4().hex}"
    config = RQOrchestratorConfig(
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        results_prefix=prefix,
        sub_channel=f"{prefix}:updates",
    )
    orch = RQOrchestrator(config=config)
    await orch._async_redis_conn.ping()
    try:
        yield orch
    finally:
        keys = [key async for key in orch._async_redis_conn.scan_iter(f"*{prefix}*")]
        if keys:
            await orch._async_redis_conn.delete(*keys)
        await orch._async_redis_conn.aclose()
        orch._redis_conn.close()


async def test_rq_worker_completion_retains_result(
    rq_orchestrator, tmp_path, completion_result, expected_status
):
    orch = rq_orchestrator
    task = Task(task_id=orch.config.results_prefix, sources=[], target=InBodyTarget())
    orch.tasks[task.task_id] = task
    task.convert_options = ConvertDocumentsOptions()
    cm = DoclingConverterManager(config=DoclingConverterManagerConfig())
    job = Job(connection=orch._redis_conn)
    async with orch._async_redis_conn.pubsub() as subscriber:
        await subscriber.subscribe(orch.config.sub_channel)
        with (
            patch.object(cm, "convert_documents", return_value=iter(())),
            patch(
                "docling_jobkit.orchestrators.rq.worker.get_current_job",
                return_value=job,
            ),
            patch(
                "docling_jobkit.orchestrators.rq.worker.process_exportable_results",
                return_value=completion_result,
            ),
        ):
            key = await asyncio.to_thread(
                _run_docling_task, task, cm, orch.config, tmp_path
            )
        updates = []
        messages = subscriber.listen()
        while len(updates) < 2:
            message = await asyncio.wait_for(anext(messages), timeout=10)
            if message["type"] == "message":
                updates.append(_TaskUpdate.model_validate_json(message["data"]))
    assert await orch.task_result(task.task_id) == completion_result
    assert updates[-1].result_key == key
    assert updates[-1].task_status == expected_status


async def test_rq_direct_recovery_retains_result(
    rq_orchestrator, completion_result, expected_status
):
    orch = rq_orchestrator
    task = Task(task_id=orch.config.results_prefix, sources=[], target=InBodyTarget())
    orch.tasks[task.task_id] = task
    key = f"{orch.config.results_prefix}:custom-result"
    await orch._async_redis_conn.set(
        key, msgpack.packb(make_msgpack_safe(completion_result.model_dump()))
    )
    job = Job.create("builtins.str", id=task.task_id, connection=orch._redis_conn)
    job.set_status(JobStatus.FINISHED)
    job.save()
    Result.create(job, Result.Type.SUCCESSFUL, ttl=60, return_value=key)
    await orch._refresh_task_from_rq(task.task_id)
    assert orch._task_result_keys[task.task_id] == key
    assert await orch.task_result(task.task_id) == completion_result
    assert task.task_status == expected_status


@pytest.mark.parametrize("expired", [False, True])
async def test_rq_direct_recovery_without_payload(rq_orchestrator, expired):
    orch = rq_orchestrator
    task = Task(task_id=orch.config.results_prefix, sources=[], target=InBodyTarget())
    orch.tasks[task.task_id] = task
    key = f"{orch.config.results_prefix}:custom-result"
    if expired:
        await orch._async_redis_conn.set(key, b"expired")
        await orch._async_redis_conn.expire(key, 0)
    assert await orch._async_redis_conn.get(key) is None
    job = Job.create("builtins.str", id=task.task_id, connection=orch._redis_conn)
    job.set_status(JobStatus.FINISHED)
    job.save()
    Result.create(job, Result.Type.SUCCESSFUL, ttl=60, return_value=key)

    await orch._refresh_task_from_rq(task.task_id)

    assert task.task_status == TaskStatus.SUCCESS
    assert orch._task_result_keys[task.task_id] == key


async def test_rq_publication_keeps_custom_result_key(
    rq_orchestrator, completion_result, expected_status
):
    orch = rq_orchestrator
    task = Task(task_id=orch.config.results_prefix, sources=[], target=InBodyTarget())
    orch.tasks[task.task_id] = task
    key = f"{orch.config.results_prefix}:custom-result"
    await orch._async_redis_conn.set(
        key, msgpack.packb(make_msgpack_safe(completion_result.model_dump()))
    )
    listener = asyncio.create_task(orch._listen_for_updates())
    try:
        deadline = asyncio.get_running_loop().time() + 10
        while not (await orch._async_redis_conn.pubsub_numsub(orch.config.sub_channel))[
            0
        ][1]:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)
        await orch._async_redis_conn.publish(
            orch.config.sub_channel,
            _TaskUpdate(
                task_id=task.task_id,
                task_status=expected_status,
                result_key=key,
            ).model_dump_json(),
        )
        while not task.is_completed():
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)
        assert orch._task_result_keys[task.task_id] == key
        assert await orch.task_result(task.task_id) == completion_result
    finally:
        listener.cancel()
        with suppress(asyncio.CancelledError):
            await listener
