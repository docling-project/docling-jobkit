import asyncio
import base64
import time
from pathlib import Path

import pytest
import pytest_asyncio

from docling.datamodel.base_models import ConversionStatus

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
from docling_jobkit.datamodel.task import TaskSource
from docling_jobkit.orchestrators.local.orchestrator import (
    LocalOrchestrator,
    LocalOrchestratorConfig,
)


@pytest_asyncio.fixture
async def orchestrator():
    # Setup
    config = LocalOrchestratorConfig(
        num_workers=2,
    )

    cm_config = DoclingConverterManagerConfig()
    cm = DoclingConverterManager(config=cm_config)

    orchestrator = LocalOrchestrator(config=config, converter_manager=cm)
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
    orchestrator: LocalOrchestrator, task_id: str, max_wait: int = 60
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


@pytest.mark.asyncio
async def test_convert_url(orchestrator: LocalOrchestrator):
    options = ConvertDocumentsOptions()

    sources: list[TaskSource] = []
    sources.append(HttpSource(url="https://arxiv.org/pdf/2311.18481"))

    task = await orchestrator.enqueue(
        sources=sources,
        options=options,
    )

    await _wait_task_complete(orchestrator, task.task_id)
    results = await orchestrator.task_result(task_id=task.task_id)

    assert results is not None
    assert len(results) == 1

    result = results[0]
    assert result.status == ConversionStatus.SUCCESS


async def test_convert_file(orchestrator: LocalOrchestrator):
    options = ConvertDocumentsOptions()

    doc_filename = Path(__file__).parent / "2206.01062v1-pg4.pdf"
    encoded_doc = base64.b64encode(doc_filename.read_bytes()).decode()

    sources: list[TaskSource] = []
    sources.append(FileSource(base64_string=encoded_doc, filename=doc_filename.name))

    task = await orchestrator.enqueue(
        sources=sources,
        options=options,
    )

    await _wait_task_complete(orchestrator, task.task_id)
    results = await orchestrator.task_result(task_id=task.task_id)

    assert results is not None
    assert len(results) == 1

    result = results[0]
    assert result.status == ConversionStatus.SUCCESS
