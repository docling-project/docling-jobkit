import asyncio
import base64
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pytest
import pytest_asyncio

from docling.datamodel.base_models import ConversionStatus
from docling_core.types.doc import DoclingDocument

from docling_jobkit.datamodel.chunking import (
    ChunkingExportOptions,
    HybridChunkerOptions,
)
from docling_jobkit.datamodel.convert import (
    ConvertDocumentsOptions,
)
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
from docling_jobkit.datamodel.result import ChunkedDocumentResult, ExportResult
from docling_jobkit.datamodel.task import Task, TaskSource
from docling_jobkit.datamodel.task_meta import TaskType
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
    sources.append(HttpSource(url="https://arxiv.org/pdf/2311.18481"))

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


@pytest.mark.parametrize("include_converted_doc", [False, True])
async def test_chunk_file(orchestrator: RQOrchestrator, include_converted_doc: bool):
    conversion_options = ConvertDocumentsOptions()
    chunking_options = HybridChunkerOptions()
    export_options = ChunkingExportOptions(include_converted_doc=include_converted_doc)

    doc_filename = Path(__file__).parent / "2206.01062v1-pg4.pdf"
    encoded_doc = base64.b64encode(doc_filename.read_bytes()).decode()

    sources: list[TaskSource] = []
    sources.append(FileSource(base64_string=encoded_doc, filename=doc_filename.name))

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
            task_result.result.documents[0].content.json_content
        )
    else:
        task_result.result.documents[0].content.json_content is None
