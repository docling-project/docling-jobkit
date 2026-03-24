"""Tests for Ray Orchestrator."""

import asyncio
import base64
import os
from pathlib import Path

import pytest
import pytest_asyncio

# Skip all tests if Ray or Redis are not available
pytest.importorskip("ray")
pytest.importorskip("redis")

from docling.utils.model_downloader import download_models

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.http_inputs import FileSource
from docling_jobkit.datamodel.task_meta import TaskStatus, TaskType
from docling_jobkit.datamodel.task_targets import InBodyTarget
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.orchestrator import (
    RayOrchestrator,
)


@pytest.fixture
def redis_url():
    """Get Redis URL from environment or use default."""
    return os.getenv("REDIS_URL", "redis://localhost:6379/")


@pytest_asyncio.fixture
async def artifacts_path():
    download_path = download_models(with_easyocr=False)

    # # Extra models
    # model = LayoutObjectDetectionOptions.get_preset("layout_heron_default")
    # for repo_id in get_model_repos(model.model_spec):
    #     repo_cache_folder = repo_id.replace("/", "--")
    #     download_hf_model(
    #         repo_id=repo_id,
    #         local_dir=download_path / repo_cache_folder,
    #     )
    return download_path


@pytest.fixture
def test_pdf_path():
    """Get path to test PDF file."""
    return Path(__file__).parent / "2206.01062v1-pg4.pdf"


@pytest.fixture
def test_pdf_base64(test_pdf_path: Path):
    """Get base64-encoded test PDF content."""
    with open(test_pdf_path, "rb") as f:
        return base64.b64encode(f.read()).decode()


@pytest_asyncio.fixture
async def orchestrator(artifacts_path: Path, redis_url: str):
    """Create Ray Orchestrator for testing."""
    # Setup
    config = RayOrchestratorConfig(
        redis_url=redis_url,
        dispatcher_interval=1.0,  # Faster for testing
        max_concurrent_tasks=2,
        max_queued_tasks=5,
        enable_queue_limit_rejection=False,
        ray_address=None,  # Use local Ray
        ray_runtime_env={
            "excludes": ["*"]  # Don't package anything
        },
    )

    cm_config = DoclingConverterManagerConfig(
        enable_remote_services=False,
        artifacts_path=artifacts_path,
    )
    cm = DoclingConverterManager(config=cm_config)

    orchestrator = RayOrchestrator(config=config, converter_manager=cm)

    # Start processing in background
    queue_task = asyncio.create_task(orchestrator.process_queue())

    yield orchestrator

    # Teardown
    await orchestrator.shutdown()
    queue_task.cancel()
    try:
        await queue_task
    except asyncio.CancelledError:
        pass


@pytest_asyncio.fixture
async def orchestrator_with_limits(artifacts_path: Path, redis_url: str):
    """Create Ray Orchestrator with strict queue limits for testing rejection."""
    # Setup with strict limits
    config = RayOrchestratorConfig(
        redis_url=redis_url,
        dispatcher_interval=0.5,  # Faster for testing
        max_concurrent_tasks=1,  # Only 1 task can run at a time
        max_queued_tasks=1,  # Only 1 task can be queued
        enable_queue_limit_rejection=True,  # Enable rejection
        ray_address=None,  # Use local Ray
        ray_runtime_env={
            "excludes": ["*"]  # Don't package anything
        },
    )

    cm_config = DoclingConverterManagerConfig(
        enable_remote_services=False,
        artifacts_path=artifacts_path,
    )
    cm = DoclingConverterManager(config=cm_config)

    orchestrator = RayOrchestrator(config=config, converter_manager=cm)

    # Start processing in background
    queue_task = asyncio.create_task(orchestrator.process_queue())

    yield orchestrator

    # Teardown
    await orchestrator.shutdown()
    queue_task.cancel()
    try:
        await queue_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_orchestrator_initialization(orchestrator: RayOrchestrator):
    """Test that orchestrator initializes correctly."""
    assert orchestrator is not None
    assert orchestrator.config is not None
    assert orchestrator.redis_manager is not None
    assert orchestrator.dispatcher is not None


@pytest.mark.asyncio
async def test_check_connection(orchestrator: RayOrchestrator):
    """Test connection health check."""
    # This will raise if connections are not healthy
    await orchestrator.check_connection()


@pytest.mark.asyncio
async def test_convert_document_full_lifecycle(
    orchestrator: RayOrchestrator, test_pdf_base64: str
):
    """
    Comprehensive test that converts a document and validates:
    - Task enqueueing
    - Queue size tracking
    - User metadata handling
    - Task tracking
    - Successful completion
    """
    # Get initial queue size
    initial_queue_size = await orchestrator.queue_size()

    # Create task with sources
    sources = [
        FileSource(
            filename="2206.01062v1-pg4.pdf",
            base64_string=test_pdf_base64,
        )
    ]

    # Enqueue task with convert_options
    task = await orchestrator.enqueue(
        sources=sources,
        target=InBodyTarget(),
        task_type=TaskType.CONVERT,
        convert_options=ConvertDocumentsOptions(),
    )

    # Validate task enqueueing
    assert task is not None
    assert task.task_id is not None
    assert task.task_status == TaskStatus.PENDING
    assert len(task.sources) == 1

    # Validate queue size increased
    new_queue_size = await orchestrator.queue_size()
    assert new_queue_size >= initial_queue_size

    # Validate user metadata handling
    task.metadata["user_id"] = "test_user_123"
    assert task.metadata.get("user_id") == "test_user_123"

    # Validate task tracking
    assert task.task_id in orchestrator.tasks
    tracked_task = await orchestrator.task_status(task.task_id)
    assert tracked_task.task_id == task.task_id

    # Wait for successful completion
    max_wait = 60  # seconds
    wait_interval = 1  # seconds
    final_status = None
    for _ in range(max_wait):
        final_status = await orchestrator.task_status(task.task_id)
        if final_status.is_completed():
            break
        await asyncio.sleep(wait_interval)

    assert final_status is not None, "Failed to get final task status"
    assert final_status.task_status == TaskStatus.SUCCESS, (
        f"Task failed: {final_status.error_message}"
    )
    assert final_status.is_completed()


# @pytest.mark.asyncio
# async def test_queue_limit_rejection(
#     orchestrator_with_limits: RayOrchestrator, test_pdf_base64: str
# ):
#     """
#     Test queue limit rejection when enabled.

#     Steps:
#     1. Convert one document successfully (warms up the actor)
#     2. Wait for task to complete
#     3. Submit a new task (should succeed)
#     4. Submit a second task immediately (should fail due to queue limit)
#     """
#     sources = [
#         FileSource(filename="2206.01062v1-pg4.pdf", base64_string=test_pdf_base64)
#     ]

#     # Step 1: Convert one document to warm up the actor
#     warmup_task = await orchestrator_with_limits.enqueue(
#         sources=sources,
#         target=InBodyTarget(),
#         task_type=TaskType.CONVERT,
#         convert_options=ConvertDocumentsOptions(),
#     )
#     assert warmup_task is not None

#     # Step 2: Wait for the warmup task to complete successfully
#     max_wait = 60  # seconds
#     wait_interval = 1  # seconds
#     task_status = None
#     for _ in range(max_wait):
#         task_status = await orchestrator_with_limits.task_status(warmup_task.task_id)
#         if task_status.is_completed():
#             break
#         await asyncio.sleep(wait_interval)

#     assert task_status is not None, "Failed to get task status"
#     assert task_status.task_status == TaskStatus.SUCCESS, (
#         f"Warmup task failed: {task_status.error_message}"
#     )

#     # Step 3: Submit a new task (should succeed - actor is hot, queue is empty)
#     task1 = await orchestrator_with_limits.enqueue(
#         sources=sources,
#         target=InBodyTarget(),
#         convert_options=ConvertDocumentsOptions(),
#     )
#     assert task1 is not None
#     print(f"{task1.task_status}")

#     await asyncio.sleep(1)

#     # Step 4: Submit a second task immediately (should fail - queue limit exceeded)
#     # With max_concurrent_tasks=1 and max_queued_tasks=1:
#     # - task1 is either running or queued (1 slot used)
#     # - task2 would exceed the queue limit
#     with pytest.raises(QueueLimitExceededError):
#         task2 = await orchestrator_with_limits.enqueue(
#             sources=sources,
#             target=InBodyTarget(),
#             convert_options=ConvertDocumentsOptions(),
#         )
#         print(f"{task2.task_status}")


@pytest.mark.asyncio
async def test_get_stats(orchestrator: RayOrchestrator):
    """Test getting orchestrator statistics."""
    stats = await orchestrator.get_stats()

    assert stats is not None
    assert "orchestrator_type" in stats
    assert stats["orchestrator_type"] == "ray"
    assert "total_tasks" in stats
    assert "queue_size" in stats
    assert "config" in stats


@pytest.mark.asyncio
async def test_metadata_field_backward_compatibility():
    """Test that metadata field doesn't break existing code."""
    from docling_jobkit.datamodel.task import Task

    # Create task without metadata - should use default empty dict
    task = Task(
        task_id="test",
        sources=[],
    )

    assert task.metadata == {}

    # Should be able to add metadata
    task.metadata["user_id"] = "test_user"
    assert task.metadata["user_id"] == "test_user"


@pytest.mark.asyncio
async def test_expire_result():
    """RedisStateManager.expire_result calls redis.expire with correct args."""
    from unittest.mock import AsyncMock

    from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager

    manager = RedisStateManager(redis_url="redis://localhost:6379/")
    manager.redis = AsyncMock()
    manager.redis.expire = AsyncMock(return_value=True)

    await manager.expire_result("mykey:task:abc:result", 42)

    manager.redis.expire.assert_called_once_with("mykey:task:abc:result", 42)


@pytest.mark.asyncio
async def test_on_result_fetched_ray():
    """on_result_fetched calls expire_result with correct key and result_removal_delay."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig

    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/",
        results_prefix="docling:ray:results",
        result_removal_delay=99,
    )

    with patch(
        "docling_jobkit.orchestrators.ray.orchestrator.RayOrchestrator.__init__",
        return_value=None,
    ):
        from docling_jobkit.orchestrators.ray.orchestrator import RayOrchestrator

        orch = RayOrchestrator.__new__(RayOrchestrator)
        orch.config = config
        orch.tasks = {}
        orch.notifier = None
        orch.redis_manager = AsyncMock()
        orch.redis_manager.expire_result = AsyncMock()

        task_id = "abc-123"
        orch.tasks[task_id] = MagicMock()

        await orch.on_result_fetched(task_id)

    expected_key = f"{config.results_prefix}:task:{task_id}:result"
    orch.redis_manager.expire_result.assert_called_once_with(expected_key, 99)
    assert task_id not in orch.tasks
