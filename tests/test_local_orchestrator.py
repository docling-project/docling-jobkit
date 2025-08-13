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

from docling.datamodel import vlm_model_specs
from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.pipeline_options import (
    ProcessingPipeline,
)
from docling.datamodel.pipeline_options_vlm_model import ResponseFormat
from docling.utils.model_downloader import download_models

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.datamodel.convert import (
    ConvertDocumentsOptions,
    VlmModelApi,
    VlmModelLocal,
)
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
from docling_jobkit.datamodel.result import ExportResult
from docling_jobkit.datamodel.task import TaskSource
from docling_jobkit.datamodel.task_targets import InBodyTarget
from docling_jobkit.orchestrators.local.orchestrator import (
    LocalOrchestrator,
    LocalOrchestratorConfig,
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
    config = LocalOrchestratorConfig(
        num_workers=2,
        shared_models=True,
    )

    remote_models = not bool(os.getenv("CI"))
    cm_config = DoclingConverterManagerConfig(
        enable_remote_services=remote_models, artifacts_path=artifacts_path
    )
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


@pytest_asyncio.fixture
async def replicated_orchestrator(artifacts_path: Path):
    NUM_WORKERS = 4
    if os.getenv("CI"):
        NUM_WORKERS = 2
    # Setup
    config = LocalOrchestratorConfig(
        num_workers=NUM_WORKERS,
        shared_models=False,
    )

    cm_config = DoclingConverterManagerConfig(artifacts_path=artifacts_path)
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
async def test_convert_warmup():
    cm_config = DoclingConverterManagerConfig()
    cm = DoclingConverterManager(config=cm_config)

    config = LocalOrchestratorConfig(shared_models=True)
    orchestrator = LocalOrchestrator(config=config, converter_manager=cm)

    options = ConvertDocumentsOptions()
    pdf_format_option = cm.get_pdf_pipeline_opts(options)
    converter = cm.get_converter(pdf_format_option)

    assert len(converter.initialized_pipelines) == 0

    await orchestrator.warm_up_caches()
    assert len(converter.initialized_pipelines) > 0


@dataclass
class TestOption:
    options: ConvertDocumentsOptions
    name: str
    ci: bool


def convert_options_gen() -> Iterable[TestOption]:
    options = ConvertDocumentsOptions()
    yield TestOption(options=options, name="default", ci=True)

    options = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
    )
    yield TestOption(options=options, name="vlm_default", ci=False)

    options = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
        vlm_pipeline_model=vlm_model_specs.VlmModelType.SMOLDOCLING,
    )
    yield TestOption(options=options, name="vlm_smoldocling", ci=False)

    # options = ConvertDocumentsOptions(
    #     pipeline=ProcessingPipeline.VLM,
    #     vlm_pipeline_model=vlm_model_specs.VlmModelType.GRANITE_VISION_OLLAMA
    # )
    # yield TestOption(options=options, name="vlm_granite_vision_ollama", ci=False)

    options = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
        vlm_pipeline_model_local=VlmModelLocal.from_docling(
            vlm_model_specs.SMOLDOCLING_MLX
        ),
    )
    yield TestOption(options=options, name="vlm_local_smoldocling_mlx", ci=False)

    options = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
        vlm_pipeline_model_api=VlmModelApi(
            url="http://localhost:1234/v1/chat/completions",
            params={"model": "ds4sd/SmolDocling-256M-preview-mlx-bf16"},
            response_format=ResponseFormat.DOCTAGS,
            prompt="Convert this page to docling.",
        ),
    )
    yield TestOption(options=options, name="vlm_lmstudio_smoldocling_mlx", ci=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("test_option", convert_options_gen(), ids=lambda o: o.name)
async def test_convert_url(orchestrator: LocalOrchestrator, test_option: TestOption):
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
    task_result = await orchestrator.task_result(task_id=task.task_id)

    assert task_result is not None
    assert isinstance(task_result.result, ExportResult)

    assert task_result.result.status == ConversionStatus.SUCCESS


async def test_convert_file(orchestrator: LocalOrchestrator):
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
    task_result = await orchestrator.task_result(task_id=task.task_id)

    assert task_result is not None
    assert isinstance(task_result.result, ExportResult)

    assert task_result.result.status == ConversionStatus.SUCCESS


@pytest.mark.asyncio
async def test_replicated_convert(replicated_orchestrator: LocalOrchestrator):
    options = ConvertDocumentsOptions()

    sources: list[TaskSource] = []
    sources.append(HttpSource(url="https://arxiv.org/pdf/2311.18481"))

    NUM_TASKS = 6
    if os.getenv("CI"):
        NUM_TASKS = 3

    for _ in range(NUM_TASKS):
        task = await replicated_orchestrator.enqueue(
            sources=sources,
            options=options,
            target=InBodyTarget(),
        )

    await _wait_task_complete(replicated_orchestrator, task.task_id)
    task_result = await replicated_orchestrator.task_result(task_id=task.task_id)

    assert task_result is not None
    assert isinstance(task_result.result, ExportResult)

    assert task_result.result.status == ConversionStatus.SUCCESS
