import asyncio
import base64
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

import pytest
import pytest_asyncio

from docling.datamodel.base_models import ConversionStatus, InputFormat

if TYPE_CHECKING:
    pass
from docling.datamodel.service.requests import FileSourceRequest, HttpSourceRequest
from docling.datamodel.service.targets import InBodyTarget
from docling.document_converter import PdfFormatOption
from docling_core.types.doc import DoclingDocument

from docling_jobkit.convert.manager import (
    DoclingConverterManagerConfig,
)
from docling_jobkit.datamodel.callback import CallbackSpec, ProgressKind
from docling_jobkit.datamodel.chunking import (
    ChunkingExportOptions,
    HybridChunkerOptions,
)
from docling_jobkit.datamodel.convert import (
    ConvertDocumentsOptions,
)
from docling_jobkit.datamodel.result import (
    ChunkedDocumentResult,
    ExportDocumentResponse,
    ExportResult,
)
from docling_jobkit.datamodel.task import Task, TaskSource
from docling_jobkit.datamodel.task_meta import TaskType
from docling_jobkit.orchestrators.base_orchestrator import BaseOrchestrator
from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestrator,
    RQOrchestratorConfig,
)
from docling_jobkit.orchestrators.rq.worker import (
    CustomRQWorker,
    _prepare_convert_sources,
)


@pytest_asyncio.fixture
async def orchestrator():
    # Setup
    config = RQOrchestratorConfig()
    orchestrator = RQOrchestrator(config=config)
    queue_task = asyncio.create_task(orchestrator.process_queue())

    yield orchestrator

    # Teardown
    # Cancel the background queue processor on shutdown
    queue_task.cancel()
    try:
        await queue_task
    except asyncio.CancelledError:
        print("Queue processor cancelled.")


async def _wait_task_complete(
    orchestrator: BaseOrchestrator, task_id: str, max_wait: int = 60
) -> bool:
    start_time = time.monotonic()
    while True:
        task = await orchestrator.task_status(task_id=task_id)
        if task.is_completed():
            return True
        await asyncio.sleep(5)
        elapsed_time = time.monotonic() - start_time
        if elapsed_time > max_wait:
            return False


@dataclass
class TestOption:
    options: ConvertDocumentsOptions
    name: str
    ci: bool


def convert_options_gen() -> Iterable[TestOption]:
    options = ConvertDocumentsOptions()
    yield TestOption(options=options, name="default", ci=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("test_option", convert_options_gen(), ids=lambda o: o.name)
async def test_convert_url(orchestrator: RQOrchestrator, test_option: TestOption):
    options = test_option.options

    if os.getenv("CI") and not test_option.ci:
        pytest.skip("Skipping test in CI")

    sources: list[TaskSource] = []
    sources.append(HttpSourceRequest(url="https://arxiv.org/pdf/2311.18481"))

    task = await orchestrator.enqueue(
        sources=sources,
        convert_options=options,
        target=InBodyTarget(),
    )

    await _wait_task_complete(orchestrator, task.task_id)
    task_result = await orchestrator.task_result(task_id=task.task_id)

    assert task_result is not None
    assert isinstance(task_result.result, ExportResult)

    assert task_result.result.status == ConversionStatus.SUCCESS


async def test_convert_file(orchestrator: RQOrchestrator):
    options = ConvertDocumentsOptions()

    doc_filename = Path(__file__).parent / "2206.01062v1-pg4.pdf"
    encoded_doc = base64.b64encode(doc_filename.read_bytes()).decode()

    sources: list[TaskSource] = []
    sources.append(
        FileSourceRequest(base64_string=encoded_doc, filename=doc_filename.name)
    )

    task = await orchestrator.enqueue(
        sources=sources,
        convert_options=options,
        target=InBodyTarget(),
    )

    await _wait_task_complete(orchestrator, task.task_id)
    task_result = await orchestrator.task_result(task_id=task.task_id)

    assert task_result is not None
    assert isinstance(task_result.result, ExportResult)

    assert task_result.result.status == ConversionStatus.SUCCESS


def test_prepare_convert_sources_threads_max_file_size(monkeypatch: pytest.MonkeyPatch):
    task = Task(
        task_id="task-prepare",
        sources=[
            FileSourceRequest(
                filename="doc.pdf", base64_string=base64.b64encode(b"pdf").decode()
            )
        ],
        target=InBodyTarget(),
    )
    captured: list[int | None] = []

    def _fake_expand(task_arg, *, max_file_size=None, allow_external_plugins=False):
        assert task_arg is task
        assert allow_external_plugins is False
        captured.append(max_file_size)
        return ([], None)

    monkeypatch.setattr(
        "docling_jobkit.orchestrators.rq.worker.expand_task_sources",
        _fake_expand,
    )

    convert_sources, headers, source_info = _prepare_convert_sources(
        task,
        max_file_size=123,
    )

    assert convert_sources == []
    assert headers is None
    assert source_info == [
        {"type": "FileSourceRequest", "kind": "file", "filename": "doc.pdf"}
    ]
    assert captured == [123]


@pytest.mark.parametrize("include_converted_doc", [False, True])
async def test_chunk_file(orchestrator: RQOrchestrator, include_converted_doc: bool):
    conversion_options = ConvertDocumentsOptions()
    chunking_options = HybridChunkerOptions()
    export_options = ChunkingExportOptions(include_converted_doc=include_converted_doc)

    doc_filename = Path(__file__).parent / "2206.01062v1-pg4.pdf"
    encoded_doc = base64.b64encode(doc_filename.read_bytes()).decode()

    sources: list[TaskSource] = []
    sources.append(
        FileSourceRequest(base64_string=encoded_doc, filename=doc_filename.name)
    )

    task: Task = await orchestrator.enqueue(
        task_type=TaskType.CHUNK,
        sources=sources,
        convert_options=conversion_options,
        chunking_options=chunking_options,
        chunking_export_options=export_options,
        target=InBodyTarget(),
    )

    await _wait_task_complete(orchestrator, task.task_id)
    task_result = await orchestrator.task_result(task_id=task.task_id)

    assert task_result is not None
    assert isinstance(task_result.result, ChunkedDocumentResult)

    assert len(task_result.result.documents) == 1
    assert len(task_result.result.chunks) > 1

    if include_converted_doc:
        DoclingDocument.model_validate(
            task_result.result.documents[0].document.json_content
        )
    else:
        task_result.result.documents[0].document.json_content is None


@pytest.mark.asyncio
async def test_delete_task_cleans_up_job(orchestrator: RQOrchestrator):
    """Test that delete_task removes both result data and RQ job from Redis."""

    import msgpack
    from rq.job import Job

    options = ConvertDocumentsOptions()

    doc_filename = Path(__file__).parent / "2206.01062v1-pg4.pdf"
    encoded_doc = base64.b64encode(doc_filename.read_bytes()).decode()

    sources: list[TaskSource] = []
    sources.append(
        FileSourceRequest(base64_string=encoded_doc, filename=doc_filename.name)
    )

    # Enqueue a task (this creates the job in Redis but won't process it without a worker)
    task = await orchestrator.enqueue(
        sources=sources,
        convert_options=options,
        target=InBodyTarget(),
    )

    # Verify the RQ job exists in Redis
    job = Job.fetch(task.task_id, connection=orchestrator._redis_conn)
    assert job is not None, "Job should exist in Redis after enqueue"
    assert job.id == task.task_id

    # Simulate a completed task by adding a result key
    # (normally this would be done by the worker)
    result_key = f"{orchestrator.config.results_prefix}:{task.task_id}"
    mock_result = ExportResult(
        document=ExportDocumentResponse(filename="test.pdf"),
        status=ConversionStatus.SUCCESS,
    )
    packed = msgpack.packb(mock_result.model_dump(), use_bin_type=True)
    await orchestrator._async_redis_conn.setex(
        result_key, orchestrator.config.results_ttl, packed
    )
    orchestrator._task_result_keys[task.task_id] = result_key

    # Verify result key exists in the tracking dict
    assert task.task_id in orchestrator._task_result_keys

    # Verify result data exists in Redis
    result_data = await orchestrator._async_redis_conn.get(result_key)
    assert result_data is not None, "Result data should exist in Redis"

    # Delete the task
    await orchestrator.delete_task(task.task_id)

    # Verify result key is removed from tracking dict
    assert task.task_id not in orchestrator._task_result_keys

    # Verify result data is deleted from Redis
    result_data = await orchestrator._async_redis_conn.get(result_key)
    assert result_data is None, "Result data should be deleted from Redis"

    # Verify the RQ job is deleted from Redis
    try:
        Job.fetch(task.task_id, connection=orchestrator._redis_conn)
        assert False, "Job should have been deleted from Redis"
    except Exception:
        # Expected: job should not exist anymore
        pass

    # Verify task is removed from orchestrator's task tracking
    try:
        await orchestrator.get_raw_task(task_id=task.task_id)
        assert False, "Task should have been removed from orchestrator"
    except Exception:
        # Expected: task should not exist anymore
        pass


@pytest.mark.asyncio
async def test_clear_converters_clears_worker_cache():
    """Test that clear_converters enqueues a job that clears the worker's converter cache."""
    config = RQOrchestratorConfig()
    orchestrator = RQOrchestrator(config=config)
    with tempfile.TemporaryDirectory(prefix="docling_test_") as scratch_dir:
        cm_config = DoclingConverterManagerConfig()
        worker = CustomRQWorker(
            [orchestrator._rq_queue],
            connection=orchestrator._redis_conn,
            orchestrator_config=config,
            cm_config=cm_config,
            scratch_dir=scratch_dir,
        )

        # Populate the converter cache by calling get_converter
        pdf_option = PdfFormatOption()
        worker.conversion_manager.get_converter(pdf_option)
        cache_info = worker.conversion_manager._get_converter_from_hash.cache_info()
        assert cache_info.currsize > 0, "Cache should have items before clearing"

        # Enqueue the clear_converters job via the orchestrator
        await orchestrator.clear_converters()

        # Process the job with the worker in burst mode
        worker.work(burst=True)

        # Verify the cache was cleared
        cache_info = worker.conversion_manager._get_converter_from_hash.cache_info()
        assert cache_info.currsize == 0, (
            "Worker converter cache should be empty after clear_converters"
        )


@pytest.mark.asyncio
async def test_on_result_fetched_rq():
    """on_result_fetched sets EXPIRE on result key, pops tracking dict, deletes RQ job."""
    from unittest.mock import AsyncMock, MagicMock, patch

    config = RQOrchestratorConfig(result_removal_delay=42)
    orch = RQOrchestrator(config=config)

    task_id = "test-task-id"
    result_key = f"{config.results_prefix}:{task_id}"
    orch._task_result_keys[task_id] = result_key
    # Seed self.tasks so super().delete_task() doesn't raise
    from docling_jobkit.datamodel.task import Task

    orch.tasks[task_id] = MagicMock(spec=Task)

    # Mock async redis expire
    orch._async_redis_conn = AsyncMock()
    orch._async_redis_conn.expire = AsyncMock(return_value=True)

    # Mock RQ Job.fetch to raise (job already gone)
    with patch("docling_jobkit.orchestrators.rq.orchestrator.Job") as mock_job_cls:
        mock_job_cls.fetch.side_effect = Exception("job gone")
        await orch.on_result_fetched(task_id)

    # Result key must have had EXPIRE set with result_removal_delay
    orch._async_redis_conn.expire.assert_called_once_with(result_key, 42)
    # In-memory tracking must be cleaned up
    assert task_id not in orch._task_result_keys
    assert task_id not in orch.tasks


@pytest.mark.asyncio
async def test_convert_with_callbacks(orchestrator: RQOrchestrator, callback_server_rq):
    """Test document conversion with callback invocations using RQ orchestrator."""
    callback_server = callback_server_rq
    options = ConvertDocumentsOptions()

    doc_filename = Path(__file__).parent / "2206.01062v1-pg4.pdf"
    encoded_doc = base64.b64encode(doc_filename.read_bytes()).decode()

    sources: list[TaskSource] = []
    sources.append(
        FileSourceRequest(base64_string=encoded_doc, filename=doc_filename.name)
    )

    # Create task with callback
    task = await orchestrator.enqueue(
        sources=sources,
        convert_options=options,
        target=InBodyTarget(),
        callbacks=[
            CallbackSpec(
                url="http://localhost:8766/callback",
            )
        ],
    )

    await _wait_task_complete(orchestrator, task.task_id)
    task_result = await orchestrator.task_result(task_id=task.task_id)

    assert task_result is not None
    assert isinstance(task_result.result, ExportResult)
    assert task_result.result.status == ConversionStatus.SUCCESS

    # Give callbacks time to be invoked (they run in background threads)
    await asyncio.sleep(2)

    # Verify callbacks were received
    assert len(callback_server.callbacks) >= 2, (
        f"Expected at least 2 callbacks, got {len(callback_server.callbacks)}"
    )

    # Verify ProgressSetNumDocs callback
    set_num_docs_callbacks = callback_server.get_callbacks_by_kind(
        ProgressKind.SET_NUM_DOCS
    )
    assert len(set_num_docs_callbacks) == 1, "Expected 1 SET_NUM_DOCS callback"
    assert set_num_docs_callbacks[0]["progress"]["num_docs"] == 1

    # Verify ProgressDocumentCompleted callback
    doc_completed_callbacks = callback_server.get_callbacks_by_kind(
        ProgressKind.DOCUMENT_COMPLETED
    )
    assert len(doc_completed_callbacks) == 1, "Expected 1 DOCUMENT_COMPLETED callback"

    doc_callback = doc_completed_callbacks[0]["progress"]
    assert doc_callback["document"]["source"] == doc_filename.name
    assert doc_callback["document"]["status"] == ConversionStatus.SUCCESS
    assert doc_callback["document"]["document_type"] == InputFormat.PDF
    assert doc_callback["document"]["num_pages"] is not None
    assert doc_callback["document"]["num_pages"] > 0
    assert doc_callback["document"]["num_characters"] is not None
    assert doc_callback["document"]["num_characters"] > 0
    assert doc_callback["document"]["num_tables"] is not None
    assert doc_callback["document"]["num_tables"] >= 0
    assert doc_callback["document"]["num_pictures"] is not None
    assert doc_callback["document"]["num_pictures"] >= 0
    assert doc_callback["total_processed"] == 1
    assert doc_callback["total_docs"] == 1

    # Verify ProgressUpdateProcessed callback
    update_processed_callbacks = callback_server.get_callbacks_by_kind(
        ProgressKind.UPDATE_PROCESSED
    )
    assert len(update_processed_callbacks) == 1, "Expected 1 UPDATE_PROCESSED callback"

    final_callback = update_processed_callbacks[0]["progress"]
    assert final_callback["num_processed"] == 1
    assert final_callback["num_succeeded"] == 1
    assert final_callback["num_failed"] == 0
    assert len(final_callback["docs"]) == 1
    assert final_callback["docs"][0]["source"] == doc_filename.name
    assert final_callback["docs"][0]["status"] == ConversionStatus.SUCCESS


async def test_get_task_from_rq_direct_preserves_metadata(
    orchestrator: RQOrchestrator,
):
    from rq.job import Job, JobStatus

    options = ConvertDocumentsOptions()
    sources: list[TaskSource] = [HttpSourceRequest(url="https://example.com/test.pdf")]

    task = await orchestrator.enqueue(
        sources=sources,
        convert_options=options,
        target=InBodyTarget(),
        metadata={"tenant_id": "test-tenant"},
    )
    assert task.metadata == {"tenant_id": "test-tenant"}

    orchestrator.tasks.pop(task.task_id, None)

    job = Job.fetch(task.task_id, connection=orchestrator._redis_conn)
    job.set_status(JobStatus.FINISHED)

    refreshed = await orchestrator.task_status(task_id=task.task_id)
    assert refreshed.metadata == {"tenant_id": "test-tenant"}

    cached = await orchestrator._get_task_from_redis(task.task_id)
    assert cached is not None
    assert cached.metadata == {"tenant_id": "test-tenant"}
