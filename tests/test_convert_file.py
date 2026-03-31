import asyncio
import base64
import logging
import time
from pathlib import Path

import pytest
import pytest_asyncio

from docling.datamodel.base_models import ConversionStatus

from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.http_inputs import FileSource
from docling_jobkit.datamodel.result import ExportResult
from docling_jobkit.datamodel.task import TaskSource
from docling_jobkit.datamodel.task_targets import InBodyTarget
from docling_jobkit.orchestrators.base_orchestrator import BaseOrchestrator
from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestrator,
    RQOrchestratorConfig,
)


def pytest_configure(config):
    logging.getLogger("docling").setLevel(logging.INFO)


@pytest_asyncio.fixture
async def orchestrator():
    config = RQOrchestratorConfig()
    orchestrator = RQOrchestrator(config=config)
    queue_task = asyncio.create_task(orchestrator.process_queue())

    yield orchestrator

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
        print("Still waiting for completion...")
        await asyncio.sleep(5)
        elapsed_time = time.monotonic() - start_time
        if elapsed_time > max_wait:
            return False


@pytest.mark.asyncio
async def test_convert_file(orchestrator: RQOrchestrator):
    options = ConvertDocumentsOptions()

    #doc_filename = Path(__file__).parent / "2206.01062v1-pg4.pdf"
    doc_filename = Path(__file__).parent / "2206.01062v1-pg4.pdf"

    encoded_doc = base64.b64encode(doc_filename.read_bytes()).decode()

    sources: list[TaskSource] = []
    sources.append(FileSource(base64_string=encoded_doc, filename=doc_filename.name))

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
