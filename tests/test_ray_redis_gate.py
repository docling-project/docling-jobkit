from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Skip all tests if Ray is not available
pytest.importorskip("ray")

from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.datamodel.task_targets import InBodyTarget
from docling_jobkit.orchestrators._redis_gate import RedisCallerGate
from docling_jobkit.orchestrators.base_orchestrator import RedisBackpressureError
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.models import TaskUpdate
from docling_jobkit.orchestrators.ray.orchestrator import RayOrchestrator


def _make_orchestrator(
    config: RayOrchestratorConfig | None = None,
) -> RayOrchestrator:
    config = config or RayOrchestratorConfig(redis_url="redis://localhost:6379/")
    with patch.object(RayOrchestrator, "__init__", lambda self, **kw: None):
        orch = object.__new__(RayOrchestrator)
    orch.config = config
    orch.tasks = {}
    orch.notifier = None
    orch.redis_manager = AsyncMock()
    orch._redis_gate = RedisCallerGate(config.redis_gate_concurrency or 1)
    return orch


async def _fake_updates(updates):
    for update in updates:
        yield update


class TestRayRedisGate:
    def test_redis_gate_concurrency_defaults_from_pool_size(self):
        config = RayOrchestratorConfig(
            redis_url="redis://localhost:6379/",
            redis_max_connections=20,
        )
        assert config.redis_gate_concurrency == 10

    @pytest.mark.asyncio
    async def test_enqueue_raises_backpressure_when_gate_saturated(self):
        config = RayOrchestratorConfig(
            redis_url="redis://localhost:6379/",
            redis_gate_concurrency=1,
            redis_gate_wait_timeout=0.01,
        )
        orch = _make_orchestrator(config)

        async with orch._redis_gate.acquire(1.0):
            with pytest.raises(RedisBackpressureError):
                await orch.enqueue(sources=[], target=InBodyTarget())

    @pytest.mark.asyncio
    async def test_get_queue_position_raises_backpressure_when_gate_saturated(self):
        config = RayOrchestratorConfig(
            redis_url="redis://localhost:6379/",
            redis_gate_concurrency=1,
            redis_gate_wait_timeout=0.01,
        )
        orch = _make_orchestrator(config)

        async with orch._redis_gate.acquire(1.0):
            with pytest.raises(RedisBackpressureError):
                await orch.get_queue_position("busy-task")

    @pytest.mark.asyncio
    async def test_task_result_raises_backpressure_when_gate_saturated(self):
        config = RayOrchestratorConfig(
            redis_url="redis://localhost:6379/",
            redis_gate_concurrency=1,
            redis_gate_wait_timeout=0.01,
        )
        orch = _make_orchestrator(config)

        async with orch._redis_gate.acquire(1.0):
            with pytest.raises(RedisBackpressureError):
                await orch.task_result("busy-task")

    @pytest.mark.asyncio
    async def test_pubsub_listener_is_not_gated(self):
        orch = _make_orchestrator()
        task = Task(task_id="task-1", sources=[], target=InBodyTarget())
        orch.tasks[task.task_id] = task
        orch.redis_manager.subscribe_to_updates = MagicMock(
            return_value=_fake_updates(
                [TaskUpdate(task_id=task.task_id, task_status=TaskStatus.STARTED)]
            )
        )

        async with orch._redis_gate.acquire(1.0):
            await orch._listen_for_updates()

        assert task.task_status == TaskStatus.STARTED
