import os
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

pytest.importorskip("ray")

from docling.datamodel.service.targets import InBodyTarget

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.datamodel.result import DoclingTaskResult, RemoteTargetResult
from docling_jobkit.datamodel.stored_outcome import StoredSuccessOutcome
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.models import TaskUpdate
from docling_jobkit.orchestrators.ray.orchestrator import RayOrchestrator
from docling_jobkit.orchestrators.ray.serve_deployment import (
    DoclingProcessorCoordinatorDeployment,
)


@pytest.mark.parametrize(
    "succeeded,partial,expected",
    [
        (0, 0, TaskStatus.FAILURE),
        (1, 0, TaskStatus.SUCCESS),
        (0, 1, TaskStatus.SUCCESS),
    ],
)
async def test_ray_completion_retains_result(tmp_path, succeeded, partial, expected):
    task_id = f"test-status-{uuid4().hex}"
    config = RayOrchestratorConfig(
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        scratch_dir=tmp_path,
        results_prefix=task_id,
    )
    orch = RayOrchestrator(
        config=config,
        converter_manager=DoclingConverterManager(
            config=DoclingConverterManagerConfig()
        ),
    )
    manager = orch.redis_manager
    await manager.connect()
    redis = manager._ensure_redis()
    result = DoclingTaskResult(
        result=RemoteTargetResult(),
        processing_time=0.1,
        num_converted=2,
        num_succeeded=succeeded,
        num_partially_succeeded=partial,
        num_failed=2 - succeeded - partial,
    )
    task = Task(
        task_id=task_id,
        sources=[],
        target=InBodyTarget(),
        metadata={"tenant_id": task_id},
    )
    with patch(
        "docling_jobkit.orchestrators.ray.serve_deployment.serve.get_replica_context",
        return_value=MagicMock(replica_id="test-coordinator"),
    ):
        coordinator = DoclingProcessorCoordinatorDeployment.func_or_class(
            converter_manager_config=DoclingConverterManagerConfig(),
            config=config,
            redis_url=config.redis_url,
            converter_handle=None,
        )
    coordinator.redis_manager = manager
    try:
        await manager.set_task_metadata(
            task_id=task_id,
            tenant_id=task_id,
            task_type=task.task_type,
            task_size=0,
        )
        with (
            patch.object(coordinator, "_process_task", AsyncMock(return_value=result)),
            patch.object(
                manager, "publish_update", wraps=manager.publish_update
            ) as publish,
            patch(
                "docling_jobkit.orchestrators.ray.serve_deployment.emit_task_completed_callback"
            ) as callback,
        ):
            assert await coordinator.process_task(task) == result
        callback.assert_called_once_with(task, expected.value)
        assert isinstance(await orch.task_outcome(task_id), StoredSuccessOutcome)
        assert await orch.task_result(task_id) == result
        assert await redis.hget(f"task:{task_id}", "status") == expected.value.encode()
        publish.assert_awaited_once_with(
            TaskUpdate(
                task_id=task_id,
                task_status=expected,
                result_key=f"{task_id}:task:{task_id}:result",
                progress=None,
            )
        )
        assert task_id not in orch.tasks
        recovered = await orch._task_from_redis(task_id)
        assert recovered is not None
        assert recovered.task_status == expected
        recovered.task_status = TaskStatus.STARTED
        assert await orch._task_from_redis(task_id) is recovered
        assert recovered.task_status == expected
        assert await orch.task_result(task_id) == result
        with (
            patch.object(coordinator, "_process_task", AsyncMock(return_value=result)),
            patch.object(
                manager, "publish_update", wraps=manager.publish_update
            ) as publish,
            patch(
                "docling_jobkit.orchestrators.ray.serve_deployment.emit_task_completed_callback"
            ) as callback,
        ):
            assert await coordinator.process_task(task) == result
        callback.assert_not_called()
        publish.assert_not_awaited()
        duplicate = await manager.finalize_task_success_atomic(
            tenant_id=task_id, task_id=task_id, task_size=0, result=result
        )
        assert duplicate.final_status == expected
        assert not duplicate.status_changed
        assert duplicate.result_key is None
        assert await orch.task_result(task_id) == result
    finally:
        keys = [key async for key in redis.scan_iter(f"*{task_id}*")]
        if keys:
            await redis.delete(*keys)
        await manager.disconnect()
