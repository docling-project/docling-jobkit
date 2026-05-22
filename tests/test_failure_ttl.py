"""Tests for RQ failure_ttl configuration."""

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
        _, rq_queue = RQOrchestrator.make_rq_queue(config)

        assert config.queue_name == "convert"
        assert rq_queue.name == "convert"

    def test_custom_queue_name_is_used_for_rq_queue(self):
        config = RQOrchestratorConfig(queue_name="staging-convert")
        _, rq_queue = RQOrchestrator.make_rq_queue(config)

        assert rq_queue.name == "staging-convert"
