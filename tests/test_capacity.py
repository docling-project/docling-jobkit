"""Tests for the get_capacity() method on orchestrators."""


import pytest
import pytest_asyncio

from docling_jobkit.orchestrators.base_orchestrator import (
    BaseOrchestrator,
    SystemCapacity,
)
from docling_jobkit.orchestrators.local.orchestrator import (
    LocalOrchestrator,
    LocalOrchestratorConfig,
)


class TestSystemCapacityModel:
    def test_required_fields(self):
        cap = SystemCapacity(queue_depth=5, active_jobs=2, active_workers=3)
        assert cap.queue_depth == 5
        assert cap.active_jobs == 2
        assert cap.active_workers == 3
        assert cap.max_queue_size is None

    def test_optional_max_queue_size(self):
        cap = SystemCapacity(
            queue_depth=5, active_jobs=2, active_workers=3, max_queue_size=100
        )
        assert cap.max_queue_size == 100

    def test_json_round_trip(self):
        cap = SystemCapacity(
            queue_depth=10, active_jobs=4, active_workers=8, max_queue_size=50
        )
        data = cap.model_dump()
        restored = SystemCapacity.model_validate(data)
        assert restored == cap

    def test_json_serialization(self):
        cap = SystemCapacity(queue_depth=1, active_jobs=0, active_workers=2)
        j = cap.model_dump_json()
        assert '"queue_depth":1' in j.replace(" ", "")


class TestBaseOrchestratorCapacity:
    @pytest.mark.asyncio
    async def test_default_returns_none(self):

        class StubOrchestrator(BaseOrchestrator):
            async def enqueue(self, sources, target, **kwargs):
                pass

            async def queue_size(self):
                return 0

            async def get_queue_position(self, task_id):
                return None

            async def process_queue(self):
                pass

            async def warm_up_caches(self):
                pass

            async def clear_converters(self):
                pass

            async def check_connection(self):
                pass

            async def task_result(self, task_id):
                return None

        orch = StubOrchestrator()
        result = await orch.get_capacity()
        assert result is None


class TestLocalOrchestratorCapacity:
    @pytest_asyncio.fixture
    async def orchestrator(self):
        from docling_jobkit.convert.manager import (
            DoclingConverterManager,
            DoclingConverterManagerConfig,
        )

        config = LocalOrchestratorConfig(num_workers=3)
        cm_config = DoclingConverterManagerConfig()
        cm = DoclingConverterManager(config=cm_config)
        orch = LocalOrchestrator(config=config, converter_manager=cm)
        return orch

    @pytest.mark.asyncio
    async def test_empty_capacity(self, orchestrator):
        cap = await orchestrator.get_capacity()
        assert isinstance(cap, SystemCapacity)
        assert cap.queue_depth == 0
        assert cap.active_jobs == 0
        assert cap.active_workers == 3
        assert cap.max_queue_size is None
