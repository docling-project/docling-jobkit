"""Unit tests for serve attach disambiguation in RayOrchestrator._initialize_ray_runtime().

Tests cover the _get_existing_serve_app_state() disambiguation primitive and the
decision table in _initialize_ray_runtime():
  - serve.status() fails                     → DispatcherUnavailableError, no deploy
  - app absent                               → deploy once
  - app RUNNING/DEPLOYING/DEPLOY_FAILED/UNHEALTHY → attach only, no deploy
  - app DELETING                             → DispatcherUnavailableError, no deploy
  - app present but get_app_handle() fails   → DispatcherUnavailableError, no deploy
"""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytest.importorskip("ray")
if os.getenv("CI"):
    pytest.skip("Skipping Ray tests in CI", allow_module_level=True)

from ray.serve.schema import ApplicationStatus

from docling_jobkit.orchestrators._redis_gate import RedisCallerGate
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.orchestrator import (
    DispatcherUnavailableError,
    RayOrchestrator,
)

_APP_NAME = "docling_processor"


def _make_orchestrator(config: RayOrchestratorConfig | None = None) -> RayOrchestrator:
    config = config or RayOrchestratorConfig(redis_url="redis://localhost:6379/")
    with patch.object(RayOrchestrator, "__init__", lambda self, **kw: None):
        orch = object.__new__(RayOrchestrator)

    orch.config = config
    orch.serve_app_name = _APP_NAME
    orch.tasks = {}
    orch.notifier = None
    orch.cm = MagicMock()
    orch.cm.config = MagicMock()
    orch.redis_manager = AsyncMock()
    orch._redis_gate = RedisCallerGate(config.redis_gate_concurrency or 1)
    orch._pubsub_task = None
    orch._dispatcher_supervisor_task = None
    orch.dispatcher = None
    orch.dispatcher_name = "docling_task_dispatcher"
    orch.deployment_handle = None
    orch._unhealthy_since = None
    orch._ray_session_needs_restart = False
    orch._ray_admin_executor = None
    return orch


def _make_serve_status(app_status: ApplicationStatus | None = None) -> MagicMock:
    """Build a fake ServeStatus; no app entry when app_status is None."""
    serve_status = MagicMock()
    if app_status is None:
        serve_status.applications = {}
    else:
        app_overview = MagicMock()
        app_overview.status = app_status
        serve_status.applications = {_APP_NAME: app_overview}
    return serve_status


@pytest.mark.asyncio
async def test_serve_status_fails_raises_unavailable() -> None:
    """serve.status() raises → DispatcherUnavailableError; deploy_processor not called."""
    orch = _make_orchestrator()

    with (
        patch("ray.is_initialized", return_value=True),
        patch("ray.serve.start", side_effect=RuntimeError("already running")),
        patch("ray.serve.status", side_effect=Exception("controller down")),
        patch(
            "docling_jobkit.orchestrators.ray.orchestrator.deploy_processor"
        ) as mock_deploy,
    ):
        with pytest.raises(DispatcherUnavailableError):
            await orch._initialize_ray_runtime()

    mock_deploy.assert_not_called()
    assert orch.deployment_handle is None
    assert orch.dispatcher is None


@pytest.mark.asyncio
async def test_deploy_fresh_when_app_absent() -> None:
    """serve.status() returns no target app → deploy_processor called once."""
    orch = _make_orchestrator()
    new_handle = MagicMock()
    mock_dispatcher = MagicMock()

    with (
        patch("ray.is_initialized", return_value=True),
        patch("ray.serve.start", side_effect=RuntimeError("already running")),
        patch("ray.serve.status", return_value=_make_serve_status()),
        patch(
            "docling_jobkit.orchestrators.ray.orchestrator.deploy_processor",
            return_value=new_handle,
        ) as mock_deploy,
        patch.object(orch, "_bind_dispatcher", return_value=mock_dispatcher),
    ):
        await orch._initialize_ray_runtime()

    mock_deploy.assert_called_once()
    assert orch.deployment_handle is new_handle
    assert orch.dispatcher is mock_dispatcher


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "app_status",
    [
        ApplicationStatus.RUNNING,
        ApplicationStatus.DEPLOYING,
        ApplicationStatus.DEPLOY_FAILED,
        ApplicationStatus.UNHEALTHY,
    ],
)
async def test_attach_when_app_present(app_status: ApplicationStatus) -> None:
    """serve.status() returns non-deleting app → attach only; deploy_processor not called."""
    orch = _make_orchestrator()
    existing_handle = MagicMock()
    mock_dispatcher = MagicMock()

    with (
        patch("ray.is_initialized", return_value=True),
        patch("ray.serve.start", side_effect=RuntimeError("already running")),
        patch("ray.serve.status", return_value=_make_serve_status(app_status)),
        patch("ray.serve.get_app_handle", return_value=existing_handle),
        patch(
            "docling_jobkit.orchestrators.ray.orchestrator.deploy_processor"
        ) as mock_deploy,
        patch.object(orch, "_bind_dispatcher", return_value=mock_dispatcher),
    ):
        await orch._initialize_ray_runtime()

    mock_deploy.assert_not_called()
    assert orch.deployment_handle is existing_handle
    assert orch.dispatcher is mock_dispatcher


@pytest.mark.asyncio
async def test_deleting_app_raises_unavailable() -> None:
    """serve.status() returns DELETING → DispatcherUnavailableError; no deploy."""
    orch = _make_orchestrator()

    with (
        patch("ray.is_initialized", return_value=True),
        patch("ray.serve.start", side_effect=RuntimeError("already running")),
        patch(
            "ray.serve.status",
            return_value=_make_serve_status(ApplicationStatus.DELETING),
        ),
        patch(
            "docling_jobkit.orchestrators.ray.orchestrator.deploy_processor"
        ) as mock_deploy,
    ):
        with pytest.raises(DispatcherUnavailableError):
            await orch._initialize_ray_runtime()

    mock_deploy.assert_not_called()
    assert orch.deployment_handle is None
    assert orch.dispatcher is None


@pytest.mark.asyncio
async def test_attach_failure_raises_unavailable_no_deploy() -> None:
    """serve.status() says app exists but get_app_handle fails → DispatcherUnavailableError; no deploy fallback."""
    orch = _make_orchestrator()

    with (
        patch("ray.is_initialized", return_value=True),
        patch("ray.serve.start", side_effect=RuntimeError("already running")),
        patch(
            "ray.serve.status",
            return_value=_make_serve_status(ApplicationStatus.RUNNING),
        ),
        patch("ray.serve.get_app_handle", side_effect=Exception("handle rpc failed")),
        patch(
            "docling_jobkit.orchestrators.ray.orchestrator.deploy_processor"
        ) as mock_deploy,
    ):
        with pytest.raises(DispatcherUnavailableError):
            await orch._initialize_ray_runtime()

    mock_deploy.assert_not_called()
    assert orch.deployment_handle is None
    assert orch.dispatcher is None


@pytest.mark.asyncio
async def test_initialization_failure_resets_handles() -> None:
    """Any unexpected failure during init surfaces as DispatcherUnavailableError and resets handles."""
    orch = _make_orchestrator()

    with (
        patch("ray.is_initialized", return_value=True),
        patch("ray.serve.start", side_effect=RuntimeError("already running")),
        patch("ray.serve.status", return_value=_make_serve_status()),
        patch(
            "docling_jobkit.orchestrators.ray.orchestrator.deploy_processor",
            side_effect=RuntimeError("serve cluster unreachable"),
        ),
    ):
        with pytest.raises(DispatcherUnavailableError):
            await orch._initialize_ray_runtime()

    assert orch.deployment_handle is None
    assert orch.dispatcher is None


@pytest.mark.asyncio
async def test_skips_if_already_initialized() -> None:
    """Returns immediately without any Ray calls if both handles are already set."""
    orch = _make_orchestrator()
    orch.deployment_handle = MagicMock()
    orch.dispatcher = MagicMock()

    with patch("ray.is_initialized") as mock_ray_init:
        await orch._initialize_ray_runtime()

    mock_ray_init.assert_not_called()
