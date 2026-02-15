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

from docling_jobkit.convert.chunking import process_chunk_results
from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.datamodel.chunking import (
    BaseChunkerOptions,
    HierarchicalChunkerOptions,
    HybridChunkerOptions,
)
from docling_jobkit.datamodel.convert import (
    ConvertDocumentsOptions,
    VlmModelApi,
    VlmModelLocal,
)
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
from docling_jobkit.datamodel.result import ChunkedDocumentResult, ExportResult
from docling_jobkit.datamodel.task import Task, TaskSource
from docling_jobkit.datamodel.task_meta import TaskType
from docling_jobkit.datamodel.task_targets import InBodyTarget
from docling_jobkit.orchestrators.local.orchestrator import (
    LocalOrchestrator,
    LocalOrchestratorConfig,
)


def pytest_configure(config):
    logging.getLogger("docling").setLevel(logging.INFO)


@pytest_asyncio.fixture
async def artifacts_path():
    download_path = download_models(with_easyocr=False)
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
        enable_remote_services=remote_models,
        artifacts_path=artifacts_path,
        # Enable custom configs for testing both preset and custom config paths
        allow_custom_vlm_config=True,
        allow_custom_picture_description_config=True,
        allow_custom_code_formula_config=True,
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

    cm_config = DoclingConverterManagerConfig(
        artifacts_path=artifacts_path,
        # Enable custom configs for testing
        allow_custom_vlm_config=True,
        allow_custom_picture_description_config=True,
        allow_custom_code_formula_config=True,
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
    """Generate test options for both deprecated and new configuration styles.

    Tests with '_DEPRECATED' suffix use legacy fields and should trigger deprecation warnings.
    Tests without the suffix use the new preset/custom_config system.
    """
    # Default configuration (no VLM)
    options = ConvertDocumentsOptions()
    yield TestOption(options=options, name="default", ci=True)

    # VLM with default settings (no specific model)
    options = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
    )
    yield TestOption(options=options, name="vlm_default", ci=False)

    # DEPRECATED: VLM with model enum
    options = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
        vlm_pipeline_model=vlm_model_specs.VlmModelType.GRANITEDOCLING,
    )
    yield TestOption(options=options, name="vlm_granitedocling_DEPRECATED", ci=False)

    # NEW: VLM with default preset
    options = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
        vlm_pipeline_preset="default",
    )
    yield TestOption(options=options, name="vlm_preset_default", ci=False)

    # DEPRECATED: VLM with local model configuration
    options = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
        vlm_pipeline_model_local=VlmModelLocal.from_docling(
            vlm_model_specs.GRANITEDOCLING_MLX
        ),
    )
    yield TestOption(
        options=options, name="vlm_local_granitedocling_mlx_DEPRECATED", ci=False
    )

    # NEW: VLM with custom MLX config (VlmConvertOptions as dict)
    options = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
        vlm_pipeline_custom_config={
            "model_spec": {
                "name": "Custom Granite Docling MLX",
                "default_repo_id": "ibm-granite/granite-docling-258M-mlx",
                "prompt": "Convert this page to docling.",
                "response_format": "doctags",
            },
            "engine_options": {
                "engine_type": "mlx",
                "trust_remote_code": False,
            },
            "scale": 2.0,
            "batch_size": 1,
        },
    )
    yield TestOption(options=options, name="vlm_custom_mlx_config", ci=False)

    # DEPRECATED: VLM with API model configuration
    options = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
        vlm_pipeline_model_api=VlmModelApi(
            url="http://localhost:1234/v1/chat/completions",
            params={"model": "ibm-granite/granite-docling-258M-mlx"},
            response_format=ResponseFormat.DOCTAGS,
            prompt="Convert this page to docling.",
        ),
    )
    yield TestOption(
        options=options, name="vlm_lmstudio_granitedocling_mlx_DEPRECATED", ci=False
    )

    # NEW: VLM with custom API config (VlmConvertOptions as dict)
    options = ConvertDocumentsOptions(
        pipeline=ProcessingPipeline.VLM,
        vlm_pipeline_custom_config={
            "model_spec": {
                "name": "Custom Granite Docling API",
                "default_repo_id": "ibm-granite/granite-docling-258M-mlx",
                "prompt": "Convert this page to docling.",
                "response_format": "doctags",
            },
            "engine_options": {
                "engine_type": "api",
                "url": "http://localhost:1234/v1/chat/completions",
                "params": {"model": "ibm-granite/granite-docling-258M-mlx"},
            },
            "scale": 2.0,
            "batch_size": 1,
        },
    )
    yield TestOption(options=options, name="vlm_custom_api_config", ci=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("test_option", convert_options_gen(), ids=lambda o: o.name)
async def test_convert_url(orchestrator: LocalOrchestrator, test_option: TestOption):
    """Test document conversion with both deprecated and new configuration styles.

    Tests with '_DEPRECATED' suffix should trigger deprecation warnings.
    """
    options = test_option.options

    if os.getenv("CI") and not test_option.ci:
        pytest.skip("Skipping test in CI")

    sources: list[TaskSource] = []
    sources.append(HttpSource(url="https://arxiv.org/pdf/2311.18481"))

    # Check if this is a deprecated test case and expect deprecation warning
    if "_DEPRECATED" in test_option.name:
        with pytest.warns(DeprecationWarning):
            task = await orchestrator.enqueue(
                sources=sources,
                convert_options=options,
                target=InBodyTarget(),
            )
    else:
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


async def test_convert_file(orchestrator: LocalOrchestrator):
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
            convert_options=options,
            target=InBodyTarget(),
        )

    await _wait_task_complete(replicated_orchestrator, task.task_id)
    task_result = await replicated_orchestrator.task_result(task_id=task.task_id)

    assert task_result is not None
    assert isinstance(task_result.result, ExportResult)

    assert task_result.result.status == ConversionStatus.SUCCESS


@pytest.mark.parametrize(
    "chunking_options", [HybridChunkerOptions(), HierarchicalChunkerOptions()]
)
async def test_chunk_file(
    orchestrator: LocalOrchestrator, chunking_options: BaseChunkerOptions
):
    conversion_options = ConvertDocumentsOptions(to_formats=[])

    doc_filename = Path(__file__).parent / "2206.01062v1-pg4.pdf"
    encoded_doc = base64.b64encode(doc_filename.read_bytes()).decode()

    sources: list[TaskSource] = []
    sources.append(FileSource(base64_string=encoded_doc, filename=doc_filename.name))

    task: Task = await orchestrator.enqueue(
        task_type=TaskType.CHUNK,
        sources=sources,
        convert_options=conversion_options,
        chunking_options=chunking_options,
        target=InBodyTarget(),
    )

    await _wait_task_complete(orchestrator, task.task_id)
    task_result = await orchestrator.task_result(task_id=task.task_id)

    assert task_result is not None
    assert isinstance(task_result.result, ChunkedDocumentResult)

    assert len(task_result.result.documents) == 1
    assert (
        task_result.result.documents[0].content.json_content is None
    )  # by default no document
    assert len(task_result.result.chunks) > 1

    if isinstance(chunking_options, HybridChunkerOptions):
        assert task_result.result.chunks[0].num_tokens > 0

    if isinstance(chunking_options, HierarchicalChunkerOptions):
        assert task_result.result.chunks[0].num_tokens is None


@pytest.mark.asyncio
async def test_clear_converters_clears_caches():
    """Test that clear_converters clears all caches including chunker_manager and worker_cms."""
    cm_config = DoclingConverterManagerConfig()
    cm = DoclingConverterManager(config=cm_config)

    config = LocalOrchestratorConfig(num_workers=2, shared_models=False)
    orchestrator = LocalOrchestrator(config=config, converter_manager=cm)

    # Verify chunker_manager exists
    assert orchestrator.chunker_manager is not None
    assert orchestrator.worker_cms == []

    # Start queue processing to initialize workers
    queue_task = asyncio.create_task(orchestrator.process_queue())

    # Enqueue a chunking task to populate caches
    doc_filename = Path(__file__).parent / "2206.01062v1-pg4.pdf"
    encoded_doc = base64.b64encode(doc_filename.read_bytes()).decode()

    sources: list[TaskSource] = []
    sources.append(FileSource(base64_string=encoded_doc, filename=doc_filename.name))

    task = await orchestrator.enqueue(
        task_type=TaskType.CHUNK,
        sources=sources,
        convert_options=ConvertDocumentsOptions(to_formats=[]),
        chunking_options=HybridChunkerOptions(),
        target=InBodyTarget(),
    )

    # Wait for task to complete
    await _wait_task_complete(orchestrator, task.task_id, max_wait=30)

    # Verify worker_cms list is populated (workers with non-shared models)
    assert len(orchestrator.worker_cms) > 0, (
        "worker_cms should be populated with worker converter managers"
    )

    # Check that caches have items before clearing
    chunker_cache_info_before = (
        orchestrator.chunker_manager._get_chunker_from_cache.cache_info()
    )
    assert chunker_cache_info_before.currsize > 0, (
        "Chunker cache should have items before clearing"
    )

    # Call clear_converters
    await orchestrator.clear_converters()

    # Verify chunker cache is cleared
    chunker_cache_info_after = (
        orchestrator.chunker_manager._get_chunker_from_cache.cache_info()
    )
    assert chunker_cache_info_after.currsize == 0, (
        "Chunker cache should be empty after clearing"
    )

    # Verify worker converter manager caches are cleared
    for worker_cm in orchestrator.worker_cms:
        worker_cache_info = worker_cm._get_converter_from_hash.cache_info()
        assert worker_cache_info.currsize == 0, (
            "Worker converter cache should be empty after clearing"
        )

    # Cleanup
    queue_task.cancel()
    try:
        await queue_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_chunker_manager_shared_across_workers():
    """Test that chunker_manager is passed to process_chunk_results in workers."""
    from unittest.mock import patch

    cm_config = DoclingConverterManagerConfig()
    cm = DoclingConverterManager(config=cm_config)

    config = LocalOrchestratorConfig(num_workers=2, shared_models=True)
    orchestrator = LocalOrchestrator(config=config, converter_manager=cm)

    # Verify chunker_manager is initialized
    assert orchestrator.chunker_manager is not None
    expected_chunker_manager = orchestrator.chunker_manager

    # Start queue processing
    queue_task = asyncio.create_task(orchestrator.process_queue())

    # Patch process_chunk_results to capture the call arguments
    with patch(
        "docling_jobkit.orchestrators.local.worker.process_chunk_results",
        wraps=process_chunk_results,
    ) as mock_process:
        # Enqueue a chunking task
        doc_filename = Path(__file__).parent / "2206.01062v1-pg4.pdf"
        encoded_doc = base64.b64encode(doc_filename.read_bytes()).decode()

        sources: list[TaskSource] = []
        sources.append(
            FileSource(base64_string=encoded_doc, filename=doc_filename.name)
        )

        task = await orchestrator.enqueue(
            task_type=TaskType.CHUNK,
            sources=sources,
            convert_options=ConvertDocumentsOptions(to_formats=[]),
            chunking_options=HybridChunkerOptions(),
            target=InBodyTarget(),
        )

        await _wait_task_complete(orchestrator, task.task_id, max_wait=30)
        task_result = await orchestrator.task_result(task_id=task.task_id)

        # Verify task completed successfully
        assert task_result is not None
        assert isinstance(task_result.result, ChunkedDocumentResult)

        # Verify process_chunk_results was called with the orchestrator's chunker_manager
        assert mock_process.called, "process_chunk_results should have been called"
        call_kwargs = mock_process.call_args.kwargs
        assert "chunker_manager" in call_kwargs, (
            "chunker_manager should be passed as kwarg"
        )
        assert call_kwargs["chunker_manager"] is expected_chunker_manager, (
            "The same chunker_manager instance from orchestrator should be passed to process_chunk_results"
        )

    # Cleanup
    queue_task.cancel()
    try:
        await queue_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_worker_cms_tracking():
    """Test that worker_cms list tracks converter managers for non-shared workers."""
    cm_config = DoclingConverterManagerConfig()
    cm = DoclingConverterManager(config=cm_config)

    # Use non-shared models to trigger worker_cms tracking
    config = LocalOrchestratorConfig(num_workers=2, shared_models=False)
    orchestrator = LocalOrchestrator(config=config, converter_manager=cm)

    # Initially empty
    assert orchestrator.worker_cms == []

    # Start queue processing
    queue_task = asyncio.create_task(orchestrator.process_queue())

    # Enqueue a task to trigger worker initialization
    doc_filename = Path(__file__).parent / "2206.01062v1-pg4.pdf"
    encoded_doc = base64.b64encode(doc_filename.read_bytes()).decode()

    sources: list[TaskSource] = []
    sources.append(FileSource(base64_string=encoded_doc, filename=doc_filename.name))

    task = await orchestrator.enqueue(
        sources=sources,
        convert_options=ConvertDocumentsOptions(),
        target=InBodyTarget(),
    )

    # Wait for task to complete
    await _wait_task_complete(orchestrator, task.task_id, max_wait=30)

    # Verify worker_cms is populated
    assert len(orchestrator.worker_cms) > 0, (
        "worker_cms should contain converter managers from workers"
    )

    # Verify each worker_cm is a DoclingConverterManager instance
    for worker_cm in orchestrator.worker_cms:
        assert isinstance(worker_cm, DoclingConverterManager), (
            "Each worker_cm should be a DoclingConverterManager"
        )

    # Cleanup
    queue_task.cancel()
    try:
        await queue_task
    except asyncio.CancelledError:
        pass
