"""Unit tests for RayOrchestrator._initialize_ray_runtime recovery semantics.

Mock-based — does not require a live Ray cluster or Redis instance, so
unlike tests/test_ray_orchestrator.py these run in CI.

Regression coverage for https://github.com/docling-project/docling-jobkit/issues/160.
"""

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("ray")
pytest.importorskip("redis")

from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.orchestrator import (
    DispatcherUnavailableError,
    RayOrchestrator,
)


def _build_orchestrator() -> RayOrchestrator:
    """Construct an orchestrator with mocked dependencies (no real Ray/Redis)."""
    config = RayOrchestratorConfig()
    cm = MagicMock()
    cm.config = MagicMock()
    return RayOrchestrator(config=config, converter_manager=cm)


async def test_initialize_ray_runtime_calls_shutdown_when_mid_init_fails() -> None:
    """If init fails after ray.init() succeeded, ray.shutdown() must be called.

    Otherwise the half-initialized Ray client stays registered and the
    supervisor's retry loop wedges on `client has already connected to
    the cluster with allow_multiple=True`.
    """
    orchestrator = _build_orchestrator()

    with (
        patch("docling_jobkit.orchestrators.ray.orchestrator.ray") as mock_ray,
        patch("docling_jobkit.orchestrators.ray.orchestrator.serve") as mock_serve,
        patch(
            "docling_jobkit.orchestrators.ray.orchestrator.deploy_processor"
        ) as mock_deploy,
    ):
        mock_ray.is_initialized.return_value = False
        mock_ray.__version__ = "test"
        mock_ray.get_dashboard_url.return_value = None
        mock_serve.start.return_value = None
        # Simulate mid-init failure — ray.init() succeeded, then a later
        # step raises. deploy_processor is the most realistic candidate
        # because it goes through Ray Serve which exercises the client
        # data channel.
        mock_deploy.side_effect = RuntimeError("simulated mid-init failure")

        with pytest.raises(DispatcherUnavailableError):
            await orchestrator._initialize_ray_runtime()

        # The critical assertion: ray.shutdown was called so the next
        # supervisor iteration starts from a clean Ray client state.
        mock_ray.shutdown.assert_called_once()

        # State is cleared so the supervisor's `deployment_handle is None`
        # check triggers a retry.
        assert orchestrator.dispatcher is None
        assert orchestrator.deployment_handle is None


async def test_initialize_ray_runtime_shutdown_does_not_mask_original_error() -> None:
    """If ray.shutdown() itself raises, the original init error still propagates."""
    orchestrator = _build_orchestrator()

    with (
        patch("docling_jobkit.orchestrators.ray.orchestrator.ray") as mock_ray,
        patch("docling_jobkit.orchestrators.ray.orchestrator.serve") as mock_serve,
        patch(
            "docling_jobkit.orchestrators.ray.orchestrator.deploy_processor"
        ) as mock_deploy,
    ):
        mock_ray.is_initialized.return_value = False
        mock_ray.__version__ = "test"
        mock_ray.get_dashboard_url.return_value = None
        mock_serve.start.return_value = None
        mock_deploy.side_effect = RuntimeError("original failure")
        mock_ray.shutdown.side_effect = RuntimeError("shutdown also failed")

        with pytest.raises(DispatcherUnavailableError) as exc_info:
            await orchestrator._initialize_ray_runtime()

        # The DispatcherUnavailableError carries the original cause, not
        # the shutdown failure.
        assert "original failure" in str(exc_info.value)
        mock_ray.shutdown.assert_called_once()


async def test_initialize_ray_runtime_does_not_shutdown_on_success() -> None:
    """Happy-path: shutdown must NOT be called when init succeeds."""
    orchestrator = _build_orchestrator()

    with (
        patch("docling_jobkit.orchestrators.ray.orchestrator.ray") as mock_ray,
        patch("docling_jobkit.orchestrators.ray.orchestrator.serve") as mock_serve,
        patch(
            "docling_jobkit.orchestrators.ray.orchestrator.deploy_processor"
        ) as mock_deploy,
        patch.object(orchestrator, "_bind_dispatcher", return_value=MagicMock()),
    ):
        mock_ray.is_initialized.return_value = False
        mock_ray.__version__ = "test"
        mock_ray.get_dashboard_url.return_value = None
        mock_serve.start.return_value = None
        mock_deploy.return_value = MagicMock()  # deployment_handle

        await orchestrator._initialize_ray_runtime()

        mock_ray.shutdown.assert_not_called()
        assert orchestrator.deployment_handle is not None
        assert orchestrator.dispatcher is not None
