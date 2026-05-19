"""Ray Serve coordinator/converter deployments for document processing."""

from __future__ import annotations

import asyncio
import datetime
import gc
import logging
import shutil
import tempfile
import time
from copy import deepcopy
from io import BytesIO
from pathlib import Path
from typing import Any, Optional, Union

import ray
from ray import serve

from docling.datamodel.base_models import (
    ConversionStatus,
    DoclingComponentType,
    DocumentStream,
    ErrorItem,
)
from docling.datamodel.document import ConversionResult
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.sources import FileSource, HttpSource
from docling.datamodel.service.tasks import TaskType
from docling.utils.profiling import ProfilingItem
from docling_core.types.doc.document import DoclingDocument

from docling_jobkit.convert.chunking import (
    DocumentChunkerManager,
    process_chunkable_results,
)
from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.convert.materialization import (
    MaterializationLimits,
    materialize_and_preflight,
)
from docling_jobkit.convert.results import process_exportable_results
from docling_jobkit.datamodel.exportable_document import ExportableDocument
from docling_jobkit.datamodel.result import DoclingTaskResult
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.logging_utils import (
    configure_ray_actor_logging,
)
from docling_jobkit.orchestrators.ray.models import (
    ConverterRequest,
    ConverterTaskResult,
    MaterializedConvertRequest,
    PassthroughTaskRequest,
    SliceConvertRequest,
    SlicePlan,
    SliceSpec,
    TaskUpdate,
)
from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager

_log = logging.getLogger(__name__)

try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    _log.warning("psutil not available - memory monitoring disabled")


def _is_exportable_status(status: ConversionStatus) -> bool:
    return status in (ConversionStatus.SUCCESS, ConversionStatus.PARTIAL_SUCCESS)


def _is_pdf_source(source: Any) -> bool:
    if isinstance(source, FileSource):
        return source.filename.lower().endswith(".pdf")
    if isinstance(source, HttpSource):
        return str(source.url).lower().split("?", 1)[0].endswith(".pdf")
    return False


def _build_convert_sources(
    task: Task,
) -> tuple[list[Union[str, DocumentStream]], Optional[dict[str, Any]]]:
    convert_sources: list[Union[str, DocumentStream]] = []
    headers: Optional[dict[str, Any]] = None

    for source in task.sources:
        if isinstance(source, DocumentStream):
            convert_sources.append(source)
        elif isinstance(source, FileSource):
            convert_sources.append(source.to_document_stream())
        elif isinstance(source, HttpSource):
            convert_sources.append(str(source.url))
            if headers is None and source.headers:
                headers = source.headers

    return convert_sources, headers


def _materialized_stream(filename: str, payload: bytes) -> DocumentStream:
    return DocumentStream(name=filename, stream=BytesIO(payload))


def _build_slice_plan(
    total_pages: int,
    requested_page_range: tuple[int, int],
    max_page_slice_size: int,
) -> SlicePlan:
    start_page, end_page = requested_page_range
    if start_page > total_pages:
        raise ValueError(
            f"Requested page_range starts at {start_page}, but the document has only {total_pages} pages"
        )

    effective_range = (start_page, min(end_page, total_pages))
    slices: list[SliceSpec] = []
    slice_index = 0
    for slice_start in range(
        effective_range[0], effective_range[1] + 1, max_page_slice_size
    ):
        slice_end = min(slice_start + max_page_slice_size - 1, effective_range[1])
        slices.append(
            SliceSpec(page_range=(slice_start, slice_end), slice_index=slice_index)
        )
        slice_index += 1

    return SlicePlan(
        total_pages=total_pages,
        slices=slices,
        effective_page_range=effective_range,
    )


def _merge_timings(
    exportable_documents: list[ExportableDocument],
) -> dict[str, ProfilingItem]:
    merged: dict[str, ProfilingItem] = {}
    for exportable_document in exportable_documents:
        for name, item in exportable_document.timings.items():
            if name not in merged:
                merged[name] = deepcopy(item)
                continue

            merged_item = merged[name]
            merged_item.count += item.count
            merged_item.times.extend(item.times)
            merged_item.start_timestamps.extend(item.start_timestamps)

    return merged


def _build_failed_slice_result(
    filename: str,
    page_range: tuple[int, int],
    slice_index: int,
    exc: Exception,
) -> ExportableDocument:
    return ExportableDocument(
        file=Path(filename),
        status=ConversionStatus.FAILURE,
        errors=[
            ErrorItem(
                component_type=DoclingComponentType.PIPELINE,
                module_name=type(exc).__name__,
                error_message=str(exc) or type(exc).__name__,
            )
        ],
        page_range=page_range,
        slice_index=slice_index,
    )


def _slice_sort_key(result: ExportableDocument) -> int:
    if result.slice_index is None:
        raise RuntimeError("Slice result is missing slice_index")
    return result.slice_index


def _assemble_slice_results(
    slice_results: list[ExportableDocument],
) -> ExportableDocument:
    ordered_results = sorted(slice_results, key=_slice_sort_key)
    successful_results = [
        result
        for result in ordered_results
        if _is_exportable_status(result.status) and result.document is not None
    ]
    if not successful_results:
        raise RuntimeError("No successful child chunks were produced")

    assembled_doc = (
        successful_results[0].document
        if len(successful_results) == 1
        else DoclingDocument.concatenate(
            [
                result.document
                for result in successful_results
                if result.document is not None
            ]
        )
    )
    final_status = (
        ConversionStatus.SUCCESS
        if all(result.status == ConversionStatus.SUCCESS for result in ordered_results)
        else ConversionStatus.PARTIAL_SUCCESS
    )
    errors = [error for result in ordered_results for error in result.errors]

    return ExportableDocument(
        file=successful_results[0].file,
        document_hash=successful_results[0].document_hash,
        status=final_status,
        errors=errors,
        timings=_merge_timings(ordered_results),
        document=assembled_doc,
    )


def _build_callback_invoker(task: Task) -> Optional[CallbackInvoker]:
    if not task.callbacks:
        return None

    return CallbackInvoker(
        max_retries=3,
        timeout=30.0,
        retry_delay=1.0,
    )


def _parse_memory_limit_bytes(limit_str: Optional[str]) -> Optional[int]:
    if not limit_str:
        return None

    if limit_str.endswith("GB"):
        return int(float(limit_str[:-2]) * 1024 * 1024 * 1024)
    if limit_str.endswith("MB"):
        return int(float(limit_str[:-2]) * 1024 * 1024)
    return int(limit_str)


@serve.deployment
class DoclingProcessorConverterDeployment:
    """Warm conversion replica with no Redis lifecycle responsibility."""

    def __init__(
        self,
        converter_manager_config: DoclingConverterManagerConfig,
        config: RayOrchestratorConfig,
    ) -> None:
        configure_ray_actor_logging(config.log_level)

        self.config = config
        self.converter_manager_config = converter_manager_config

        try:
            replica_context = serve.get_replica_context()
            self.replica_id = str(replica_context.replica_id)
        except RuntimeError:
            self.replica_id = "unknown"

        _log.info(
            "Converter replica %s: initializing DoclingConverterManager",
            self.replica_id,
        )
        self.cm = DoclingConverterManager(config=converter_manager_config)
        _log.setLevel(self.config.log_level.upper())
        self.scratch_dir = config.scratch_dir
        if self.scratch_dir is not None:
            self.scratch_dir.mkdir(exist_ok=True, parents=True)

        self.tasks_processed = 0
        self.documents_processed = 0
        self.last_task_time: Optional[datetime.datetime] = None
        self.memory_warnings = 0

    async def process_converter_request(
        self, request: ConverterRequest
    ) -> ConverterTaskResult | ExportableDocument:
        if self.config.enable_oom_protection and PSUTIL_AVAILABLE:
            await self._check_memory()

        if isinstance(request, PassthroughTaskRequest):
            request_start = time.monotonic()
            conv_results = await self._run_with_retry(
                request.task.task_id,
                lambda: self._convert_passthrough_task(request.task),
            )
            result = self._build_task_result(
                request.task,
                [
                    ExportableDocument.from_conversion_result(conv_res)
                    for conv_res in conv_results
                ],
                start_time=request_start,
            )
            self.documents_processed += result.task_result.num_converted
        elif isinstance(request, MaterializedConvertRequest):
            request_start = time.monotonic()
            conv_results = await self._run_with_retry(
                request.filename,
                lambda: self._convert_materialized_request(request),
            )
            result = self._build_task_result(
                request.task,
                [
                    ExportableDocument.from_conversion_result(conv_res)
                    for conv_res in conv_results
                ],
                expected_doc_count=request.source_count,
                start_time=request_start,
            )
            self.documents_processed += result.task_result.num_converted
        elif isinstance(request, SliceConvertRequest):
            result = await self._run_with_retry(
                f"{request.filename}:{request.page_range}",
                lambda: self._process_slice_convert(request),
            )
            if _is_exportable_status(result.status):
                self.documents_processed += 1
        else:
            raise ValueError(f"Unsupported converter request: {type(request)!r}")

        self.tasks_processed += 1
        self.last_task_time = datetime.datetime.now(datetime.timezone.utc)
        return result

    async def _run_with_retry(self, task_label: str, func: Any) -> Any:
        max_retries = self.config.max_task_retries
        retry_delay = self.config.retry_delay
        last_exception: Optional[Exception] = None

        for attempt in range(max_retries + 1):
            try:
                return await asyncio.to_thread(func)
            except Exception as exc:  # pragma: no cover - exercised in tests via mocks
                last_exception = exc
                if attempt < max_retries:
                    _log.warning(
                        "Converter replica %s: %s failed (attempt %s/%s): %s",
                        self.replica_id,
                        task_label,
                        attempt + 1,
                        max_retries + 1,
                        exc,
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    _log.error(
                        "Converter replica %s: %s failed after %s attempts: %s",
                        self.replica_id,
                        task_label,
                        max_retries + 1,
                        exc,
                    )

        raise last_exception or RuntimeError("Converter request failed")

    def _get_chunker_manager(self) -> DocumentChunkerManager:
        chunker_manager = getattr(self, "_chunker_manager", None)
        if chunker_manager is None:
            chunker_manager = DocumentChunkerManager()
            self._chunker_manager = chunker_manager
        return chunker_manager

    def _build_task_result(
        self,
        task: Task,
        exportable_documents: list[ExportableDocument],
        *,
        expected_doc_count: Optional[int] = None,
        start_time: Optional[float] = None,
    ) -> ConverterTaskResult:
        callback_invoker = _build_callback_invoker(task)
        temp_dir_kwargs: dict[str, Any] = {
            "prefix": f"docling_converter_{task.task_id}_",
        }
        if self.config.scratch_dir is not None:
            temp_dir_kwargs["dir"] = str(self.config.scratch_dir)
            Path(temp_dir_kwargs["dir"]).mkdir(exist_ok=True, parents=True)

        with tempfile.TemporaryDirectory(**temp_dir_kwargs) as temp_dir:
            workdir = Path(temp_dir)
            if task.task_type == TaskType.CONVERT:
                task_result = process_exportable_results(
                    task=task,
                    exportable_documents=exportable_documents,
                    work_dir=workdir,
                    callback_invoker=callback_invoker,
                    expected_doc_count=expected_doc_count,
                    start_time=start_time,
                )
            elif task.task_type == TaskType.CHUNK:
                task_result = process_chunkable_results(
                    task=task,
                    exportable_documents=exportable_documents,
                    work_dir=workdir,
                    chunker_manager=self._get_chunker_manager(),
                    callback_invoker=callback_invoker,
                    expected_doc_count=expected_doc_count,
                    start_time=start_time,
                )
            else:
                raise ValueError(f"Unsupported task type: {task.task_type}")

        return ConverterTaskResult(task_result=task_result)

    def _convert_passthrough_task(self, task: Task) -> list[ConversionResult]:
        convert_sources, headers = _build_convert_sources(task)
        convert_opts = task.convert_options or ConvertDocumentsOptions()
        return list(
            self.cm.convert_documents(
                sources=convert_sources, options=convert_opts, headers=headers
            )
        )

    def _convert_materialized_request(
        self, request: MaterializedConvertRequest
    ) -> list[ConversionResult]:
        payload = ray.get(request.artifact_ref)
        return list(
            self.cm.convert_documents(
                sources=[_materialized_stream(request.filename, payload)],
                options=request.task.convert_options or ConvertDocumentsOptions(),
            )
        )

    def _process_slice_convert(
        self, request: SliceConvertRequest
    ) -> ExportableDocument:
        payload = ray.get(request.artifact_ref)
        options = request.options.model_copy(update={"page_range": request.page_range})
        conv_results = list(
            self.cm.convert_documents(
                sources=[_materialized_stream(request.filename, payload)],
                options=options,
            )
        )
        if not conv_results:
            raise RuntimeError("Slice conversion returned no results")

        return ExportableDocument.from_conversion_result(
            conv_results[0],
            page_range=request.page_range,
            slice_index=request.slice_index,
        )

    async def _check_memory(self) -> None:
        if not PSUTIL_AVAILABLE:
            return

        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            limit_str = self.config.converter_actor_memory_request
            if not limit_str:
                return

            limit_bytes = _parse_memory_limit_bytes(limit_str)
            if limit_bytes is None:
                return

            limit_mb = limit_bytes / (1024 * 1024)
            if memory_mb > limit_mb * self.config.memory_warning_threshold:
                self.memory_warnings += 1
                _log.warning(
                    "Converter replica %s: high memory usage %.0fMB / %.0fMB",
                    self.replica_id,
                    memory_mb,
                    limit_mb,
                )
                if self.memory_warnings > 3:
                    gc.collect()
                    self.memory_warnings = 0
        except Exception as exc:  # pragma: no cover - observability only
            _log.warning(
                "Converter replica %s: memory check failed: %s",
                self.replica_id,
                exc,
            )

    async def health_check(self) -> dict[str, Any]:
        return {
            "replica_id": self.replica_id,
            "healthy": self.cm is not None,
            "tasks_processed": self.tasks_processed,
            "documents_processed": self.documents_processed,
            "memory_warnings": self.memory_warnings,
            "last_task_time": (
                self.last_task_time.isoformat() if self.last_task_time else None
            ),
        }

    async def get_stats(self) -> dict[str, Any]:
        return {
            "replica_id": self.replica_id,
            "tasks_processed": self.tasks_processed,
            "documents_processed": self.documents_processed,
            "memory_warnings": self.memory_warnings,
            "last_task_time": (
                self.last_task_time.isoformat() if self.last_task_time else None
            ),
        }

    async def clear_cache(self) -> None:
        _log.info("Converter replica %s: clearing converter cache", self.replica_id)
        self.cm.clear_cache()
        gc.collect()
        self.memory_warnings = 0


@serve.deployment
class DoclingProcessorCoordinatorDeployment:
    """Cheap coordinator that owns the parent task lifecycle and result assembly."""

    def __init__(
        self,
        converter_manager_config: DoclingConverterManagerConfig,
        config: RayOrchestratorConfig,
        redis_url: str,
        converter_handle: Any,
    ) -> None:
        configure_ray_actor_logging(config.log_level)

        self.config = config
        self.converter_manager_config = converter_manager_config
        self.converter_handle = converter_handle

        try:
            replica_context = serve.get_replica_context()
            self.replica_id = str(replica_context.replica_id)
        except RuntimeError:
            self.replica_id = "unknown"

        self.redis_manager = RedisStateManager(
            redis_url=redis_url,
            results_ttl=config.results_ttl,
            task_timeout=config.task_timeout,
            dispatcher_interval=config.dispatcher_interval,
            log_level=config.log_level,
        )
        self.scratch_dir = config.scratch_dir or Path(
            tempfile.mkdtemp(prefix=f"docling_serve_{self.replica_id}_")
        )
        self.scratch_dir.mkdir(exist_ok=True, parents=True)

        self.tasks_processed = 0
        self.documents_processed = 0
        self.last_task_time: Optional[datetime.datetime] = None

        _log.setLevel(self.config.log_level.upper())

    async def process_task(self, task: Task) -> DoclingTaskResult:
        task_start = datetime.datetime.now(datetime.timezone.utc)
        tenant_id = task.metadata.get("tenant_id", "default")
        task_size = len(task.sources)
        heartbeat_task: Optional[asyncio.Task[None]] = None
        workdir = self.scratch_dir / task.task_id

        try:
            await self.redis_manager.update_task_status(
                task.task_id, TaskStatus.STARTED
            )
        except Exception as exc:  # pragma: no cover - best effort status update
            _log.warning(
                "Coordinator replica %s: failed to mark %s as STARTED: %s",
                self.replica_id,
                task.task_id,
                exc,
            )

        try:
            execution_lease_written = False
            try:
                await self.redis_manager.write_task_execution_lease(
                    task_id=task.task_id,
                    tenant_id=tenant_id,
                    replica_id=self.replica_id,
                )
                execution_lease_written = True
            except Exception as exc:
                _log.error(
                    "Coordinator replica %s: failed to write execution lease for %s: %s",
                    self.replica_id,
                    task.task_id,
                    exc,
                )

            if execution_lease_written:
                heartbeat_task = asyncio.create_task(
                    self._maintain_execution_heartbeat(task.task_id)
                )

            workdir.mkdir(exist_ok=True, parents=True)
            result = await self._process_task(task, workdir)

            self.tasks_processed += 1
            self.documents_processed += task_size
            self.last_task_time = datetime.datetime.now(datetime.timezone.utc)

            terminalization = await self.redis_manager.finalize_task_success_atomic(
                tenant_id=tenant_id,
                task_id=task.task_id,
                task_size=task_size,
                result=result,
            )
            if (
                terminalization.status_changed
                and terminalization.final_status == TaskStatus.SUCCESS
                and terminalization.result_key is not None
            ):
                try:
                    await self.redis_manager.publish_update(
                        TaskUpdate(
                            task_id=task.task_id,
                            task_status=TaskStatus.SUCCESS,
                            result_key=terminalization.result_key,
                            progress=None,
                        )
                    )
                    await self.redis_manager.update_tenant_stats(
                        tenant_id,
                        delta_total_tasks=1,
                        delta_total_documents=task_size,
                        delta_successful_documents=result.num_succeeded,
                        delta_failed_documents=result.num_failed,
                    )
                except (
                    Exception
                ) as follow_up_exc:  # pragma: no cover - observability only
                    _log.warning(
                        "Coordinator replica %s: durable success follow-up failed for %s: %s",
                        self.replica_id,
                        task.task_id,
                        follow_up_exc,
                    )

            duration = (
                (self.last_task_time - task_start).total_seconds()
                if self.last_task_time
                else 0.0
            )
            _log.info(
                "Coordinator replica %s: task %s completed in %.2fs",
                self.replica_id,
                task.task_id,
                duration,
            )
            return result
        except Exception as exc:
            error_message = str(exc) or exc.__class__.__name__
            terminalization = await self.redis_manager.finalize_task_failure_atomic(
                tenant_id=tenant_id,
                task_id=task.task_id,
                task_size=task_size,
                error_message=error_message,
            )
            if (
                terminalization.status_changed
                and terminalization.final_status == TaskStatus.FAILURE
            ):
                try:
                    await self.redis_manager.publish_update(
                        TaskUpdate(
                            task_id=task.task_id,
                            task_status=TaskStatus.FAILURE,
                            error_message=error_message,
                        )
                    )
                    await self.redis_manager.update_tenant_stats(
                        tenant_id,
                        delta_total_tasks=1,
                        delta_total_documents=task_size,
                        delta_failed_documents=task_size,
                    )
                except (
                    Exception
                ) as follow_up_exc:  # pragma: no cover - observability only
                    _log.warning(
                        "Coordinator replica %s: durable failure follow-up failed for %s: %s",
                        self.replica_id,
                        task.task_id,
                        follow_up_exc,
                    )
            raise
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
            if workdir.exists():
                shutil.rmtree(workdir, ignore_errors=True)

    async def _process_task(self, task: Task, workdir: Path) -> DoclingTaskResult:
        if task.task_type == TaskType.CONVERT:
            return await self._process_convert_task(task, workdir)
        if task.task_type == TaskType.CHUNK:
            return await self._process_chunk_task(task, workdir)
        raise ValueError(f"Unknown task type: {task.task_type}")

    async def _process_convert_task(
        self, task: Task, workdir: Path
    ) -> DoclingTaskResult:
        convert_options = task.convert_options or ConvertDocumentsOptions()
        materialized_start_time = time.monotonic()

        if self._should_materialize_pdf(task):
            source = task.sources[0]
            if not isinstance(source, (FileSource, HttpSource)):
                raise TypeError(
                    "Materialized PDF path only supports FileSource and HttpSource"
                )
            materialized = await materialize_and_preflight(
                source,
                limits=MaterializationLimits(
                    max_file_size=self.converter_manager_config.max_file_size,
                    max_num_pages=self.converter_manager_config.max_num_pages,
                ),
            )
            artifact_ref = ray.put(materialized.content_bytes)
            page_count = materialized.page_count
            filename = materialized.filename
            del (
                materialized
            )  # release heap copy; bytes live in plasma store via artifact_ref
            try:
                slice_plan = _build_slice_plan(
                    total_pages=page_count,
                    requested_page_range=convert_options.page_range,
                    max_page_slice_size=self.config.max_page_slice_size,
                )
                effective_pages = (
                    slice_plan.effective_page_range[1]
                    - slice_plan.effective_page_range[0]
                    + 1
                )
                if effective_pages > self.config.max_page_slice_size:
                    slice_results = await self._run_slice_plan(
                        artifact_ref=artifact_ref,
                        filename=filename,
                        slice_plan=slice_plan,
                        options=convert_options,
                    )
                    callback_invoker = _build_callback_invoker(task)
                    return await asyncio.to_thread(
                        process_exportable_results,
                        task,
                        [_assemble_slice_results(slice_results)],
                        workdir,
                        callback_invoker,
                        start_time=materialized_start_time,
                    )
                else:
                    converter_result = (
                        await self.converter_handle.process_converter_request.remote(
                            MaterializedConvertRequest(
                                artifact_ref=artifact_ref,
                                filename=filename,
                                task=task.model_copy(update={"sources": []}),
                                source_count=len(task.sources),
                            )
                        )
                    )
                    return converter_result.task_result
            finally:
                del artifact_ref
        else:
            converter_result = (
                await self.converter_handle.process_converter_request.remote(
                    PassthroughTaskRequest(task=task)
                )
            )
            return converter_result.task_result

    async def _process_chunk_task(self, task: Task, workdir: Path) -> DoclingTaskResult:
        del workdir
        converter_result = await self.converter_handle.process_converter_request.remote(
            PassthroughTaskRequest(task=task)
        )
        return converter_result.task_result

    def _should_materialize_pdf(self, task: Task) -> bool:
        return (
            self.config.enable_pdf_page_slice_fanout
            and task.task_type == TaskType.CONVERT
            and len(task.sources) == 1
            and _is_pdf_source(task.sources[0])
        )

    async def _run_slice_plan(
        self,
        artifact_ref: Any,
        filename: str,
        slice_plan: SlicePlan,
        options: ConvertDocumentsOptions,
    ) -> list[ExportableDocument]:
        requests = [
            SliceConvertRequest(
                artifact_ref=artifact_ref,
                filename=filename,
                options=options,
                page_range=page_slice.page_range,
                slice_index=page_slice.slice_index,
            )
            for page_slice in slice_plan.slices
        ]

        parallelism = self.config.max_page_slice_parallelism
        assert parallelism is not None
        in_flight: set[asyncio.Task[ExportableDocument]] = set()
        pending_requests = iter(requests)
        collected_results: list[ExportableDocument] = []

        for _ in range(min(parallelism, len(requests))):
            request = next(pending_requests, None)
            if request is None:
                break
            in_flight.add(asyncio.create_task(self._execute_slice_request(request)))

        while in_flight:
            done, in_flight = await asyncio.wait(
                in_flight, return_when=asyncio.FIRST_COMPLETED
            )
            for completed in done:
                collected_results.append(await completed)
                next_request = next(pending_requests, None)
                if next_request is not None:
                    in_flight.add(
                        asyncio.create_task(self._execute_slice_request(next_request))
                    )

        return collected_results

    async def _execute_slice_request(
        self, request: SliceConvertRequest
    ) -> ExportableDocument:
        try:
            return await self.converter_handle.process_converter_request.remote(request)
        except Exception as exc:
            _log.warning(
                "Coordinator replica %s: slice %s for %s failed: %s",
                self.replica_id,
                request.page_range,
                request.filename,
                exc,
            )
            return _build_failed_slice_result(
                filename=request.filename,
                page_range=request.page_range,
                slice_index=request.slice_index,
                exc=exc,
            )

    async def _maintain_execution_heartbeat(self, task_id: str) -> None:
        interval = max(self.config.heartbeat_interval, 0.01)
        while True:
            try:
                updated = await self.redis_manager.update_task_execution_heartbeat(
                    task_id
                )
                if not updated:
                    return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - observability only
                _log.warning(
                    "Coordinator replica %s [EXEC-HEARTBEAT] %s failed: %s",
                    self.replica_id,
                    task_id,
                    exc,
                )
            await asyncio.sleep(interval)

    async def health_check(self) -> dict[str, Any]:
        return {
            "replica_id": self.replica_id,
            "healthy": self.converter_handle is not None,
            "tasks_processed": self.tasks_processed,
            "documents_processed": self.documents_processed,
            "last_task_time": (
                self.last_task_time.isoformat() if self.last_task_time else None
            ),
        }

    async def get_stats(self) -> dict[str, Any]:
        return {
            "replica_id": self.replica_id,
            "tasks_processed": self.tasks_processed,
            "documents_processed": self.documents_processed,
            "last_task_time": (
                self.last_task_time.isoformat() if self.last_task_time else None
            ),
        }


def _build_deployment_options(
    *,
    name: str,
    min_replicas: int,
    max_replicas: int,
    target_requests_per_replica: int,
    max_ongoing_requests: int,
    num_cpus: float,
    memory_limit: Optional[str],
    upscale_delay_s: float,
    downscale_delay_s: float,
    graceful_shutdown_wait_loop_s: Optional[float],
    graceful_shutdown_timeout_s: Optional[float],
) -> dict[str, Any]:
    deployment_options: dict[str, Any] = {
        "name": name,
        "autoscaling_config": {
            "min_replicas": min_replicas,
            "max_replicas": max_replicas,
            "target_num_ongoing_requests_per_replica": target_requests_per_replica,
            "upscale_delay_s": upscale_delay_s,
            "downscale_delay_s": downscale_delay_s,
        },
        "ray_actor_options": {"num_cpus": num_cpus},
        "max_ongoing_requests": max_ongoing_requests,
    }

    memory_bytes = _parse_memory_limit_bytes(memory_limit)
    if memory_bytes is not None:
        deployment_options["ray_actor_options"]["memory"] = memory_bytes
    if graceful_shutdown_wait_loop_s is not None:
        deployment_options["graceful_shutdown_wait_loop_s"] = (
            graceful_shutdown_wait_loop_s
        )
    if graceful_shutdown_timeout_s is not None:
        deployment_options["graceful_shutdown_timeout_s"] = graceful_shutdown_timeout_s

    return deployment_options


def create_deployment(
    converter_manager_config: DoclingConverterManagerConfig,
    config: RayOrchestratorConfig,
    redis_url: str,
    app_name: str = "docling_processor",
) -> Any:
    coordinator_target_requests_per_replica = (
        config.coordinator_target_requests_per_replica
    )
    coordinator_max_ongoing_requests_per_replica = (
        config.coordinator_max_ongoing_requests_per_replica
    )
    coordinator_actor_num_cpus = config.coordinator_actor_num_cpus
    coordinator_min_actors = config.coordinator_min_actors
    coordinator_max_actors = config.coordinator_max_actors
    assert coordinator_min_actors is not None
    assert coordinator_max_actors is not None
    assert coordinator_target_requests_per_replica is not None
    assert coordinator_max_ongoing_requests_per_replica is not None
    assert coordinator_actor_num_cpus is not None

    converter_options = _build_deployment_options(
        name="converter",
        min_replicas=config.min_actors,
        max_replicas=config.max_actors,
        target_requests_per_replica=config.target_requests_per_replica,
        max_ongoing_requests=(
            config.max_ongoing_requests_per_replica
            or config.target_requests_per_replica
        ),
        num_cpus=config.converter_actor_num_cpus,
        memory_limit=config.converter_actor_memory_request,
        upscale_delay_s=config.upscale_delay_s,
        downscale_delay_s=config.downscale_delay_s,
        graceful_shutdown_wait_loop_s=config.graceful_shutdown_wait_loop_s,
        graceful_shutdown_timeout_s=config.graceful_shutdown_timeout_s,
    )
    coordinator_options = _build_deployment_options(
        name="coordinator",
        min_replicas=coordinator_min_actors,
        max_replicas=coordinator_max_actors,
        target_requests_per_replica=coordinator_target_requests_per_replica,
        max_ongoing_requests=coordinator_max_ongoing_requests_per_replica,
        num_cpus=coordinator_actor_num_cpus,
        memory_limit=config.coordinator_actor_memory_request,
        upscale_delay_s=config.upscale_delay_s,
        downscale_delay_s=config.downscale_delay_s,
        graceful_shutdown_wait_loop_s=config.graceful_shutdown_wait_loop_s,
        graceful_shutdown_timeout_s=config.graceful_shutdown_timeout_s,
    )

    _log.info(
        "Creating Ray Serve app '%s' with coordinator '%s' and converter '%s'",
        app_name,
        coordinator_options["name"],
        converter_options["name"],
    )

    converter = DoclingProcessorConverterDeployment.options(  # type: ignore[attr-defined]
        **converter_options
    ).bind(
        converter_manager_config=converter_manager_config,
        config=config,
    )
    coordinator = DoclingProcessorCoordinatorDeployment.options(  # type: ignore[attr-defined]
        **coordinator_options,
    ).bind(
        converter_manager_config=converter_manager_config,
        config=config,
        redis_url=redis_url,
        converter_handle=converter,
    )
    return coordinator


def deploy_processor(
    converter_manager_config: DoclingConverterManagerConfig,
    config: RayOrchestratorConfig,
    redis_url: str,
    app_name: str = "docling_processor",
) -> Any:
    deployment = create_deployment(
        converter_manager_config=converter_manager_config,
        config=config,
        redis_url=redis_url,
        app_name=app_name,
    )

    handle = serve.run(deployment, name=app_name, route_prefix=f"/{app_name}")
    _log.info("Ray Serve app '%s' is running", app_name)
    return handle
