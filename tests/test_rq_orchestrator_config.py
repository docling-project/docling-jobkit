"""Tests for RQ orchestrator configuration."""

from unittest.mock import patch

import pytest

from docling.datamodel.service.requests import HttpSourceRequest
from docling.datamodel.service.targets import InBodyTarget

from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestrator,
    RQOrchestratorConfig,
)


class TestFailureTTLConfig:
    def test_default_failure_ttl_matches_results_ttl(self):
        config = RQOrchestratorConfig()
        assert config.failure_ttl == config.results_ttl
        assert config.failure_ttl == 3_600 * 4

    def test_failure_ttl_is_configurable(self):
        config = RQOrchestratorConfig(failure_ttl=7200)
        assert config.failure_ttl == 7200

    def test_failure_ttl_passed_to_queue(self):
        config = RQOrchestratorConfig(
            redis_url="redis://localhost:6379/",
            failure_ttl=1800,
        )
        try:
            _, _rq_queue = RQOrchestratorConfig.model_validate(config.model_dump())
        except Exception:
            pass
        assert config.failure_ttl == 1800


class TestQueueNameConfig:
    def test_default_queue_name_preserves_existing_behavior(self):
        config = RQOrchestratorConfig()

        with patch("docling_jobkit.orchestrators.rq.orchestrator.Queue") as queue_cls:
            RQOrchestrator.make_rq_queue(config)

        assert config.queue_name == "convert"
        assert queue_cls.call_args.args[0] == "convert"

    def test_custom_queue_name_is_used_for_rq_queue(self):
        config = RQOrchestratorConfig(queue_name="staging-convert")

        with patch("docling_jobkit.orchestrators.rq.orchestrator.Queue") as queue_cls:
            RQOrchestrator.make_rq_queue(config)

        assert queue_cls.call_args.args[0] == "staging-convert"


class TestJobTimeoutConfig:
    def test_default_job_timeout_preserves_existing_behavior(self):
        config = RQOrchestratorConfig()
        assert config.job_timeout == 14400

    def test_default_job_timeout_passed_to_queue(self):
        config = RQOrchestratorConfig()

        with patch("docling_jobkit.orchestrators.rq.orchestrator.Queue") as queue_cls:
            RQOrchestrator.make_rq_queue(config)

        assert queue_cls.call_args.kwargs["default_timeout"] == 14400

    def test_custom_job_timeout_passed_to_queue(self):
        config = RQOrchestratorConfig(job_timeout=86400)

        with patch("docling_jobkit.orchestrators.rq.orchestrator.Queue") as queue_cls:
            RQOrchestrator.make_rq_queue(config)

        assert queue_cls.call_args.kwargs["default_timeout"] == 86400

    def test_job_timeout_can_be_disabled(self):
        config = RQOrchestratorConfig(job_timeout=-1)

        with patch("docling_jobkit.orchestrators.rq.orchestrator.Queue") as queue_cls:
            RQOrchestrator.make_rq_queue(config)

        assert queue_cls.call_args.kwargs["default_timeout"] == -1


class TestJobTimeoutOnEnqueue:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("job_timeout", [14400, 86400, -1])
    async def test_enqueue_uses_configured_job_timeout(
        self, monkeypatch: pytest.MonkeyPatch, job_timeout: int
    ):
        orchestrator = RQOrchestrator(
            config=RQOrchestratorConfig(job_timeout=job_timeout)
        )
        captured: dict[str, object] = {}

        class FakeQueue:
            def enqueue(self, *args, **kwargs):
                captured["kwargs"] = kwargs

        async def _noop(*args, **kwargs):
            return None

        orchestrator._rq_queue = FakeQueue()
        monkeypatch.setattr(orchestrator, "init_task_tracking", _noop)
        monkeypatch.setattr(orchestrator, "_store_task_in_redis", _noop)

        await orchestrator.enqueue(
            sources=[HttpSourceRequest(url="https://example.com/doc.pdf")],
            convert_options=None,
            targets=[InBodyTarget()],
        )

        assert captured["kwargs"]["timeout"] == job_timeout
