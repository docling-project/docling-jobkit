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
from docling.utils.model_downloader import download_models

from docling_jobkit.datamodel.convert import (
    ConvertDocumentsOptions,
)
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
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
async def artifacts_path():
    download_path = download_models()
    return download_path


@pytest_asyncio.fixture
async def orchestrator(artifacts_path: Path):
    # Setup
    config = RQOrchestratorConfig()
    orchestrator = RQOrchestrator(config=config)
    # queue_task = asyncio.create_task(orchestrator.process_queue())

    yield orchestrator

    # Teardown
    # Cancel the background queue processor on shutdown
    # queue_task.cancel()
    # try:
    #     await queue_task
    # except asyncio.CancelledError:
    #     print("Queue processor cancelled.")


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
        options=options,
        target=InBodyTarget(),
    )

    await _wait_task_complete(orchestrator, task.task_id)
    results = await orchestrator.task_results(task_id=task.task_id)

    assert results is not None
    assert len(results) == 1

    result = results[0]
    assert result.status == ConversionStatus.SUCCESS


async def test_convert_file(orchestrator: RQOrchestrator):
    options = ConvertDocumentsOptions()

    doc_filename = Path(__file__).parent / "2206.01062v1-pg4.pdf"
    encoded_doc = base64.b64encode(doc_filename.read_bytes()).decode()

    sources: list[TaskSource] = []
    sources.append(FileSource(base64_string=encoded_doc, filename=doc_filename.name))

    task = await orchestrator.enqueue(
        sources=sources,
        options=options,
        target=InBodyTarget(),
    )

    await _wait_task_complete(orchestrator, task.task_id)
    results = await orchestrator.task_results(task_id=task.task_id)

    assert results is not None
    assert len(results) == 1

    result = results[0]
    assert result.status == ConversionStatus.SUCCESS
