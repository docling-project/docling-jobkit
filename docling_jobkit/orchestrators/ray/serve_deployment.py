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
from typing import Any, Iterator, Optional, cast

import ray
from ray import ObjectRef, serve

from docling_jobkit.orchestrators.ray.metrics_utils import get_metrics_from_exportable_doc

from docling.datamodel.settings import settings as docling_settings
from docling.datamodel.base_models import (
    ConversionStatus,
    DocumentStream,
    ErrorItem,
    InputFormat,
)
from docling.datamodel.document import ConversionResult
from docling.datamodel.service.callbacks import (
    ProgressSetNumDocs,
    ProgressUpdateProcessed,
)
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.responses import FailurePhase, PublicFailureInfo
from docling.datamodel.service.sources import FileSource, HttpSource, S3Coordinates
from docling.datamodel.service.targets import PresignedUrlTarget, S3Target
from docling.datamodel.service.tasks import TaskType
from docling.utils.profiling import ProfilingItem
from docling_core.types.doc.document import DoclingDocument

from docling_jobkit.connectors.source_processor import (
    DocumentChunk,
    SourceDocumentRef,
)
from docling_jobkit.connectors.source_processor_factory import get_source_processor
from docling_jobkit.convert.chunking import (
    DocumentChunkerManager,
    process_chunkable_results,
)
from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.convert.materialization import (
    MaterializationLimitExceededError,
    MaterializationLimits,
    materialize_and_preflight,
)
from docling_jobkit.convert.results import (
    CallbackMode,
    _build_processed_docs_item,
    _is_exportable_status,
    _maybe_emit_document_completed,
    _process_exportable_results_internal,
    process_exportable_results,
)
from docling_jobkit.convert.source_expansion import expand_task_sources
from docling_jobkit.datamodel.exportable_document import (
    ExportableDocument,
    source_to_public_uri,
)
from docling_jobkit.datamodel.result import (
    DoclingTaskResult,
    DocumentArtifactItem,
    ExportDocumentResponse,
    ExportResult,
    PresignedArtifactResult,
    RemoteTargetResult,
    ResultType,
)
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker
from docling_jobkit.orchestrators.ray.config import (
    RayOrchestratorConfig,
    parse_memory_bytes,
)
from docling_jobkit.orchestrators.ray.failure_classification import (
    classify_ray_public_task_failure,
)
from docling_jobkit.orchestrators.ray.logging_utils import (
    configure_ray_actor_logging,
)
from docling_jobkit.orchestrators.ray.models import (
    ConverterFailureResult,
    ConverterRequest,
    ConverterTaskResult,
    MaterializedConvertRequest,
    PassthroughTaskRequest,
    SliceConvertRequest,
    SlicePlan,
    SliceSpec,
    SourceChunkConvertRequest,
    TaskUpdate,
)
from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager
from docling_jobkit.public_errors import (
    build_public_error_item,
    build_public_error_item_from_failure,
    is_client_actionable_failure,
)

# metrics
from ray.util.metrics import Counter, Gauge, Histogram
import random
import string

def random_digit_string(length: int) -> str:
    return ''.join(random.choices(string.digits, k=length))

_log = logging.getLogger(__name__)

_SOURCE_CHUNK_CALLBACK_MODE = CallbackMode.CHILD_ONLY

# Back-off between retries when a coordinator is fully starved of converter units
# (the tenant's whole budget is held by its own sibling tasks and nothing is in
# flight on this coordinator to wait on). Only this rare case polls; coordinators
# with in-flight children wait on child completion instead.
_CONVERTER_UNIT_POLL_INTERVAL_S = 0.25

DEFAULT_SERVE_APP_NAME = "docling_processor"

try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    _log.warning("psutil not available - memory monitoring disabled")


def _is_pdf_source(source: Any) -> bool:
    if isinstance(source, FileSource):
        return source.filename.lower().endswith(".pdf")
    if isinstance(source, HttpSource):
        return str(source.url).lower().split("?", 1)[0].endswith(".pdf")
    return False


def _to_exportable_documents(
    task: Task,
    conv_results: list[ConversionResult],
) -> list[ExportableDocument]:
    exportable_docs = [
        ExportableDocument.from_conversion_result(
            conv_res,
            source_index=idx,
            source_uri=(
                source_to_public_uri(task.sources[idx])
                if idx < len(task.sources)
                else str(conv_res.input.file)
            ),
        )
        for idx, conv_res in enumerate(conv_results)
    ]
    return exportable_docs


def _to_exportable_documents_from_chunk(
    chunk: DocumentChunk[Any, Any],
    conv_results: list[ConversionResult],
) -> list[ExportableDocument]:
    exportable: list[ExportableDocument] = []
    metrics = []
    for idx, conv_res in enumerate(conv_results):
        ref = chunk.refs[idx] if idx < len(chunk.refs) else None
        exportable.append(
            ExportableDocument.from_conversion_result(
                conv_res,
                source_index=ref.source_index if ref is not None else idx,
                source_uri=(
                    ref.source_uri if ref is not None else str(conv_res.input.file)
                ),
            )
        )
    return exportable


def _is_s3_fanout_task(task: Task) -> bool:
    return (
        task.task_type == TaskType.CONVERT
        and isinstance(task.target, (PresignedUrlTarget, S3Target))
        and len(task.sources) > 0
        and any(isinstance(source, S3Coordinates) for source in task.sources)
    )


def _validate_no_s3_source_in_passthrough(task: Task) -> None:
    if any(isinstance(source, S3Coordinates) for source in task.sources):
        raise RuntimeError(
            "S3Coordinates sources must not reach the passthrough path; "
            "routing should have directed this task to the S3 fan-out handler."
        )


def _offset_chunk_refs(
    chunk: DocumentChunk[Any, Any], source_index_offset: int, chunk_index: int
) -> DocumentChunk[Any, Any]:
    refs = [
        ref.model_copy(update={"source_index": ref.source_index + source_index_offset})
        for ref in chunk.refs
    ]
    return DocumentChunk(source=chunk.source, refs=refs, chunk_index=chunk_index)


def _aggregate_s3_fanout_results(
    task: Task,
    child_results: list[ConverterTaskResult],
    processing_time: float,
) -> DoclingTaskResult:
    result: PresignedArtifactResult | RemoteTargetResult
    if all(
        isinstance(child_result.task_result.result, PresignedArtifactResult)
        for child_result in child_results
    ):
        presigned_results = [
            cast(PresignedArtifactResult, child_result.task_result.result)
            for child_result in child_results
        ]
        documents = sorted(
            (
                document
                for presigned_result in presigned_results
                for document in presigned_result.documents
            ),
            key=lambda document: document.source_index,
        )
        result = PresignedArtifactResult(documents=documents)
    else:
        result = RemoteTargetResult()

    return DoclingTaskResult(
        result=result,
        processing_time=processing_time,
        num_succeeded=sum(
            child_result.task_result.num_succeeded for child_result in child_results
        ),
        num_partially_succeeded=sum(
            child_result.task_result.num_partially_succeeded
            for child_result in child_results
        ),
        num_failed=sum(
            child_result.task_result.num_failed for child_result in child_results
        ),
        num_converted=sum(
            child_result.task_result.num_converted for child_result in child_results
        ),
    )


def _failed_task_placeholder(task_size: int) -> DoclingTaskResult:
    return DoclingTaskResult(
        result=RemoteTargetResult(),
        processing_time=0.0,
        num_converted=0,
        num_succeeded=0,
        num_partially_succeeded=0,
        num_failed=task_size,
    )


def _build_failed_source_chunk_exportable_document(
    ref: SourceDocumentRef[Any],
    error: ErrorItem,
) -> ExportableDocument:
    return ExportableDocument(
        file=Path(ref.filename),
        status=ConversionStatus.FAILURE,
        errors=[error],
        source_index=ref.source_index,
        source_uri=ref.source_uri,
    )


def _build_failed_source_chunk_result(
    chunk: DocumentChunk[Any, Any],
    error: ErrorItem,
    *,
    target: Any,
    debug_error_details: bool,
) -> ConverterTaskResult:
    failed_documents = [
        _build_failed_source_chunk_exportable_document(ref, error) for ref in chunk.refs
    ]
    processed_docs = [
        _build_processed_docs_item(
            exportable_document,
            debug_error_details=debug_error_details,
        )
        for exportable_document in failed_documents
    ]
    if isinstance(target, PresignedUrlTarget):
        presigned_result = PresignedArtifactResult(documents=[])
        for exportable_document in failed_documents:
            source_index = exportable_document.source_index
            if source_index is None:
                raise RuntimeError(
                    "Failed source chunk document is missing source_index"
                )
            presigned_result.documents.append(
                DocumentArtifactItem(
                    source_index=source_index,
                    source_uri=exportable_document.source_uri
                    or str(exportable_document.file),
                    filename=exportable_document.file.name,
                    status=exportable_document.status,
                    errors=exportable_document.errors,
                    timings=exportable_document.timings,
                    artifacts=[],
                )
            )
        result: RemoteTargetResult | PresignedArtifactResult = presigned_result
    else:
        result = RemoteTargetResult()
    return ConverterTaskResult(
        task_result=DoclingTaskResult(
            result=result,
            processing_time=0.0,
            num_succeeded=0,
            num_partially_succeeded=0,
            num_failed=len(failed_documents),
            num_converted=len(failed_documents),
        ),
        processed_docs=processed_docs,
    )


def _build_materialization_failure_result(
    task: Task,
    source: FileSource | HttpSource,
    exc: MaterializationLimitExceededError,
    start_time: float,
) -> DoclingTaskResult:
    """Build the public per-document result for admission-time source rejection."""
    source_uri = source_to_public_uri(source)
    filename = source.filename if isinstance(source, FileSource) else "document.pdf"
    error_item = build_public_error_item(exc)
    elapsed = time.monotonic() - start_time

    if isinstance(task.target, PresignedUrlTarget):
        result: ResultType = PresignedArtifactResult(
            documents=[
                DocumentArtifactItem(
                    source_index=0,
                    source_uri=source_uri,
                    filename=filename,
                    status=ConversionStatus.FAILURE,
                    errors=[error_item],
                )
            ]
        )
    elif isinstance(task.target, S3Target):
        result = RemoteTargetResult()
    else:
        result = ExportResult(
            document=ExportDocumentResponse(filename=filename),
            status=ConversionStatus.FAILURE,
            errors=[error_item],
        )

    return DoclingTaskResult(
        result=result,
        processing_time=elapsed,
        num_converted=1,
        num_succeeded=0,
        num_partially_succeeded=0,
        num_failed=1,
    )


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
    *,
    debug_error_details: bool,
) -> ExportableDocument:
    return ExportableDocument(
        file=Path(filename),
        status=ConversionStatus.FAILURE,
        errors=[build_public_error_item(exc)],
        source_index=0,
        source_uri=filename,
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
        source_index=successful_results[0].source_index,
        source_uri=successful_results[0].source_uri,
    )


def _finalize_slice_results(
    *,
    task: Task,
    slice_refs: list[ObjectRef],
    work_dir: Path,
    s3_presigned_config: Any,
    callback_invoker: Optional[CallbackInvoker],
    start_time: float,
    debug_error_details: bool,
) -> DoclingTaskResult:
    """Fetch child slice outputs, merge them, and build the parent task result."""
    # This is the only place where full slice documents enter the coordinator's
    # heap, and it runs inside the slice_finalization_semaphore guard.
    slice_results: list[ExportableDocument] = ray.get(slice_refs)

    return process_exportable_results(
        task=task,
        exportable_documents=[_assemble_slice_results(slice_results)],
        work_dir=work_dir,
        s3_presigned_config=s3_presigned_config,
        callback_invoker=callback_invoker,
        start_time=start_time,
        debug_error_details=debug_error_details,
    )


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
        self._chunker_manager: DocumentChunkerManager | None = None

        _log.warning(
                    "---==== Checking if metric generation is enabled ====---"
                )
        ## ------------ metrics ---------------
        # DON'T instantiate metrics here yet
        self._metrics_initialized = False

    def init_metrics(self):
        if not self._metrics_initialized:

            _log.warning(
                    f"============ Setting up metric generation"
                )

            docling_settings.debug.profile_pipeline_timings = True

            timings_hist_buckets = [
                0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.025, 0.05, 0.075, 0.1,
                0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 3.5, 4.0,
                5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 12.5, 15.0, 17.5, 20.0, 25.0, 30.0
            ]


            self.metric_emission_counter = Counter(
                "dcls_metrics_emitted",
                description="Number of attemps to emmit metrics",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.metric_emission_counter.set_default_tags({"replica_tag": replica_tag})

            self.success_counter = Counter(
                "dcls_conversion_success",
                description="Number of successeful conversions",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.success_counter.set_default_tags({"replica_tag": replica_tag})
            
            self.partial_counter = Counter(
                "dcls_conversion_partial",
                description="Number of partial conversions",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.partial_counter.set_default_tags({"replica_tag": replica_tag})

            self.failed_counter = Counter(
                "dcls_conversion_failed",
                description="Number of failed conversions",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.failed_counter.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.pipeline_total_hist = Histogram(
                "dcls_pipeline_total",
                description="Total pipeline execution time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.pipeline_total_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.page_parse_low_hist = Histogram(
                "dcls_page_parse_low",
                description="Lowest page parse time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.page_parse_low_hist.set_default_tags({"replica_tag": replica_tag})

            self.page_parse_high_hist = Histogram(
                "dcls_page_parse_high",
                description="Highest page parse time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.page_parse_high_hist.set_default_tags({"replica_tag": replica_tag})

            self.page_parse_median_hist = Histogram(
                "dcls_page_parse_median",
                description="Median page parse time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.page_parse_median_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.ocr_low_hist = Histogram(
                "dcls_ocr_low",
                description="Lowest ocr time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.ocr_low_hist.set_default_tags({"replica_tag": replica_tag})

            self.ocr_high_hist = Histogram(
                "dcls_ocr_high",
                description="Highest ocr time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.ocr_high_hist.set_default_tags({"replica_tag": replica_tag})

            self.ocr_median_hist = Histogram(
                "dcls_ocr_median",
                description="Median ocr time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.ocr_median_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.layout_low_hist = Histogram(
                "dcls_layout_low",
                description="Lowest layout time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.layout_low_hist.set_default_tags({"replica_tag": replica_tag})

            self.layout_high_hist = Histogram(
                "dcls_layout_high",
                description="Highest layout time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.layout_high_hist.set_default_tags({"replica_tag": replica_tag})

            self.layout_median_hist = Histogram(
                "dcls_layout_median",
                description="Median layout time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.layout_median_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.table_structure_low_hist = Histogram(
                "dcls_table_structure_low",
                description="Lowest table structure time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.table_structure_low_hist.set_default_tags({"replica_tag": replica_tag})
            
            self.table_structure_high_hist = Histogram(
                "dcls_table_structure_high",
                description="Highest table structure time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.table_structure_high_hist.set_default_tags({"replica_tag": replica_tag})

            self.table_structure_median_hist = Histogram(
                "dcls_table_structure_median",
                description="Median table structure time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.table_structure_median_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.page_assemble_low_hist = Histogram(
                "dcls_page_assemble_low",
                description="Lowest page assemble time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.page_assemble_low_hist.set_default_tags({"replica_tag": replica_tag})

            self.page_assemble_high_hist = Histogram(
                "dcls_page_assemble_high",
                description="Highest page assemble time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.page_assemble_high_hist.set_default_tags({"replica_tag": replica_tag})

            self.page_assemble_median_hist = Histogram(
                "dcls_page_assemble_median",
                description="Median page assemble time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.page_assemble_median_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.doc_assemble_hist = Histogram(
                "dcls_doc_assemble",
                description="Document assemble time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.doc_assemble_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.reading_order_hist = Histogram(
                "dcls_reading_order",
                description="Reading order time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.reading_order_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.doc_enrich_hist = Histogram(
                "dcls_doc_enrich",
                description="Document enrichment time in seconds",
                boundaries=timings_hist_buckets,
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.doc_enrich_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.doc_type_pdf_counter = Counter(
                "dcls_doc_type_pdf",
                description="Number of pdf documents",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.doc_type_pdf_counter.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.doc_type_docx_counter = Counter(
                "dcls_doc_type_docx",
                description="Number of docx documents",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.doc_type_docx_counter.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.doc_type_pptx_counter = Counter(
                "dcls_doc_type_pptx",
                description="Number of pptx documents",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.doc_type_pptx_counter.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.doc_type_html_counter = Counter(
                "dcls_doc_type_html",
                description="Number of html documents",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.doc_type_html_counter.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.doc_type_image_counter = Counter(
                "dcls_doc_type_image",
                description="Number of image documents",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.doc_type_image_counter.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.doc_type_md_counter = Counter(
                "dcls_doc_type_md",
                description="Number of md documents",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.doc_type_md_counter.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.doc_type_xlsx_counter = Counter(
                "dcls_doc_type_xlsx",
                description="Number of xlsx documents",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.doc_type_xlsx_counter.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.doc_type_xml_counter = Counter(
                "dcls_doc_type_xml",
                description="Number of xml documents",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.doc_type_xml_counter.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.doc_type_doclang_counter = Counter(
                "dcls_doc_type_doclang",
                description="Number of doclang documents",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.doc_type_doclang_counter.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.doc_type_docling_counter = Counter(
                "dcls_doc_type_docling",
                description="Number of docling type documents",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.doc_type_docling_counter.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.doc_type_other_counter = Counter(
                "dcls_doc_type_other",
                description="Number of other type documents",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.doc_type_other_counter.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.num_pages_hist = Counter(
                "dcls_num_pages",
                description="Number of pages in converted document",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.num_pages_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.pictures_hist = Counter(
                "dcls_pictures",
                description="Number of pictures in converted document",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.pictures_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.tables_hist = Counter(
                "dcls_tables",
                description="Number of tables in converted document",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.tables_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.key_value_items_hist = Counter(
                "dcls_key_value_items",
                description="Number of key value items in converted document",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.key_value_items_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.form_items_hist = Counter(
                "dcls_form_items",
                description="Number of form items in converted document",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.form_items_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.texts_hist = Counter(
                "dcls_texts",
                description="Number of text items in converted document",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.texts_hist.set_default_tags({"replica_tag": replica_tag})
            #-----
            self.groups_hist = Counter(
                "dcls_groups",
                description="Number of group items in converted document",
                tag_keys=("tenant_id", "replica_tag",),
            )
            #self.groups_hist.set_default_tags({"replica_tag": replica_tag})

            self._metrics_initialized = True

    def emit_metrics(self, metrics: list, tenant_id: str):
        replica_tag = serve.get_replica_context().replica_tag
        if not replica_tag:
            replica_tag = random_digit_string(12)
        _log.warning(
                    f"Emitting metrics, total number of records {len(metrics)}, replica tag: {replica_tag}",
                    
                )
        self.metric_emission_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
        self.metric_emission_counter.inc()
        for item in metrics:
            if 'reference' in item:
                metric_list = item["metrics"]
            else:
                metrics_list = [item]
            
            for record in metrics_list:
                document_hash = record["document_hash"]
                pipeline_stats = record["timings_stats"]
                if 'pipeline_total' in pipeline_stats:
                    self.pipeline_total_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.pipeline_total_hist.observe(pipeline_stats["pipeline_total"])
                if 'page_parse' in pipeline_stats:
                    self.page_parse_low_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.page_parse_high_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.page_parse_median_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.page_parse_low_hist.observe(pipeline_stats["page_parse"]["min"])
                    self.page_parse_high_hist.observe(pipeline_stats["page_parse"]["max"])
                    self.page_parse_median_hist.observe(pipeline_stats["page_parse"]["median"])
                if 'ocr' in pipeline_stats:
                    self.ocr_low_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.ocr_high_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.ocr_median_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.ocr_low_hist.observe(pipeline_stats["ocr"]["min"])
                    self.ocr_high_hist.observe(pipeline_stats["ocr"]["max"])
                    self.ocr_median_hist.observe(pipeline_stats["ocr"]["median"])
                if 'layout' in pipeline_stats:
                    self.layout_low_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.layout_high_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.layout_median_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.layout_low_hist.observe(pipeline_stats["layout"]["min"])
                    self.layout_high_hist.observe(pipeline_stats["layout"]["max"])
                    self.layout_median_hist.observe(pipeline_stats["layout"]["median"])
                if 'table_structure' in pipeline_stats:
                    self.table_structure_low_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.table_structure_high_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.table_structure_median_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.table_structure_low_hist.observe(pipeline_stats["table_structure"]["min"])
                    self.table_structure_high_hist.observe(pipeline_stats["table_structure"]["max"])
                    self.table_structure_median_hist.observe(pipeline_stats["table_structure"]["median"])
                if 'page_assemble' in pipeline_stats:
                    self.page_assemble_low_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.page_assemble_high_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.page_assemble_median_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.page_assemble_low_hist.observe(pipeline_stats["page_assemble"]["min"])
                    self.page_assemble_high_hist.observe(pipeline_stats["page_assemble"]["max"])
                    self.page_assemble_median_hist.observe(pipeline_stats["page_assemble"]["median"])
                if 'doc_assemble' in pipeline_stats:
                    self.doc_assemble_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.doc_assemble_hist.observe(pipeline_stats["doc_assemble"])
                if 'reading_order' in pipeline_stats:
                    self.reading_order_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.reading_order_hist.observe(pipeline_stats["reading_order"])
                if 'doc_enrich' in pipeline_stats:
                    self.doc_enrich_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.doc_enrich_hist.observe(pipeline_stats["doc_enrich"])

                document_stats = record["document_stats"]
                if 'input_format' in document_stats:
                    doc_type = document_stats["input_format"]
                    if doc_type == InputFormat.PDF :
                        self.doc_type_pdf_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                        self.doc_type_pdf_counter.inc()
                    elif doc_type == InputFormat.DOCX :
                        self.doc_type_docx_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                        self.doc_type_docx_counter.inc()
                    elif doc_type == InputFormat.PPTX :
                        self.doc_type_pptx_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                        self.doc_type_pptx_counter.inc()
                    elif doc_type == InputFormat.HTML :
                        self.doc_type_html_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                        self.doc_type_html_counter.inc()
                    elif doc_type == InputFormat.IMAGE :
                        self.doc_type_image_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                        self.doc_type_image_counter.inc()
                    elif doc_type == InputFormat.MD :
                        self.doc_type_md_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                        self.doc_type_md_counter.inc()
                    elif doc_type == InputFormat.XLSX :
                        self.doc_type_xlsx_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                        self.doc_type_xlsx_counter.inc()
                    elif doc_type in (InputFormat.XML_USPTO, InputFormat.XML_JATS, InputFormat.XML_XBRL) :
                        self.doc_type_xml_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                        self.doc_type_xml_counter.inc()
                    elif doc_type == InputFormat.XML_DOCLANG :
                        self.doc_type_doclang_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                        self.doc_type_doclang_counter.inc()
                    elif doc_type == InputFormat.JSON_DOCLING :
                        self.doc_type_docling_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                        self.doc_type_docling_counter.inc()
                    else:
                        self.doc_type_other_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                        self.doc_type_other_counter.inc()
                if 'num_pages' in document_stats:
                    self.num_pages_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.num_pages_hist.inc(document_stats["num_pages"])
                if 'pictures' in document_stats:
                    self.pictures_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.pictures_hist.inc(document_stats["pictures"])
                if 'tables' in document_stats:
                    self.tables_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.tables_hist.inc(document_stats["tables"])
                if 'key_value_items' in document_stats:
                    self.key_value_items_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.key_value_items_hist.inc(document_stats["key_value_items"])
                if 'form_items' in document_stats:
                    self.form_items_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.form_items_hist.inc(document_stats["form_items"])
                if 'texts' in document_stats:
                    self.texts_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.texts_hist.inc(document_stats["texts"])
                if 'groups' in document_stats:
                    self.groups_hist.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.groups_hist.inc(document_stats["groups"])

                conv_status = record["status"]
                if conv_status == "success":
                    self.success_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.success_counter.inc()
                elif conv_status == "partial":
                    self.partial_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.partial_counter.inc()
                else:
                    self.failed_counter.set_default_tags({"tenant_id": tenant_id, "replica_tag": replica_tag})
                    self.failed_counter.inc()


    async def process_converter_request(
        self, request: ConverterRequest, tenant_id: str
    ) -> ConverterTaskResult | ConverterFailureResult | ObjectRef:
        if self.config.enable_oom_protection and PSUTIL_AVAILABLE:
            await self._check_memory()
        
        # metrics lazy init
        if self.config.generate_metrics:
            self.init_metrics()

        if isinstance(request, PassthroughTaskRequest):
            request_start = time.monotonic()
            conv_results = await self._run_with_retry(
                request.task.task_id,
                lambda: self._convert_passthrough_task(request.task),
                task=request.task,
            )
            if isinstance(conv_results, ConverterFailureResult):
                return conv_results
            exportable = _to_exportable_documents(request.task, conv_results)
            if self.config.generate_metrics:
                metrics = [
                    get_metrics_from_exportable_doc(doc) for doc in exportable
                ]
                self.emit_metrics(metrics=metrics, tenant_id=tenant_id)
            result = await asyncio.to_thread(
                lambda: self._build_task_result(
                    request.task,
                    exportable,
                    start_time=request_start,
                )
            )
            self.documents_processed += result.task_result.num_converted
        elif isinstance(request, MaterializedConvertRequest):
            request_start = time.monotonic()
            conv_results = await self._run_with_retry(
                request.filename,
                lambda: self._convert_materialized_request(request),
            )
            exportable = _to_exportable_documents(request.task, conv_results)
            if self.config.generate_metrics:
                metrics = [
                    get_metrics_from_exportable_doc(doc) for doc in exportable
                ]
                self.emit_metrics(metrics=metrics, tenant_id=tenant_id)
            result = await asyncio.to_thread(
                lambda: self._build_task_result(
                    request.task,
                    exportable,
                    expected_doc_count=request.source_count,
                    start_time=request_start,
                )
            )
            self.documents_processed += result.task_result.num_converted
        elif isinstance(request, SourceChunkConvertRequest):
            request_start = time.monotonic()
            conv_results = await self._run_with_retry(
                f"{request.task.task_id}:chunk:{request.chunk.chunk_index}",
                lambda: self._convert_source_chunk_request(request),
                task=request.task,
            )
            if isinstance(conv_results, ConverterFailureResult):
                return conv_results
            exportable = _to_exportable_documents_from_chunk(
                request.chunk, conv_results
            )
            if self.config.generate_metrics:
                metrics = [
                    get_metrics_from_exportable_doc(doc) for doc in exportable
                ]
                self.emit_metrics(metrics=metrics, tenant_id=tenant_id)
            result = await asyncio.to_thread(
                lambda: self._build_task_result(
                    request.task,
                    exportable,
                    expected_doc_count=request.expected_doc_count,
                    start_time=request_start,
                    callback_mode=_SOURCE_CHUNK_CALLBACK_MODE,
                )
            )
            self.documents_processed += result.task_result.num_converted
        elif isinstance(request, SliceConvertRequest):
            slice_ref, slice_status = await self._run_with_retry(
                f"{request.filename}:{request.page_range}",
                lambda: self._process_slice_convert(request),
            )
            if _is_exportable_status(slice_status):
                self.documents_processed += 1
            result = slice_ref
        else:
            raise ValueError(f"Unsupported converter request: {type(request)!r}")

        self.tasks_processed += 1
        self.last_task_time = datetime.datetime.now(datetime.timezone.utc)
        return result

    async def _run_with_retry(
        self, task_label: str, func: Any, *, task: Task | None = None
    ) -> Any:
        max_retries = self.config.max_task_retries
        retry_delay = self.config.retry_delay
        last_exception: Optional[Exception] = None

        for attempt in range(max_retries + 1):
            try:
                return await asyncio.to_thread(func)
            except Exception as exc:  # pragma: no cover - exercised in tests via mocks
                last_exception = exc
                failure = None
                if task is not None:
                    failure = classify_ray_public_task_failure(
                        exc,
                        task_id=task.task_id,
                        phase=FailurePhase.EXECUTION,
                        details={
                            "task_size": str(len(task.sources)),
                            "target_kind": task.target.kind,
                        },
                    )
                    if is_client_actionable_failure(failure):
                        if not failure.retryable or attempt >= max_retries:
                            _log.info(
                                "Converter replica %s: %s failed with client-actionable source error: %s",
                                self.replica_id,
                                task_label,
                                failure.message,
                            )
                            return ConverterFailureResult(failure=failure)

                if attempt < max_retries:
                    _log.warning(
                        "Converter replica %s: %s failed (attempt %s/%s): %s",
                        self.replica_id,
                        task_label,
                        attempt + 1,
                        max_retries + 1,
                        failure.message if failure is not None else exc,
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    _log.error(
                        "Converter replica %s: %s failed after %s attempts: %s",
                        self.replica_id,
                        task_label,
                        max_retries + 1,
                        failure.message if failure is not None else exc,
                    )

        raise last_exception or RuntimeError("Converter request failed")

    def _get_chunker_manager(self) -> DocumentChunkerManager:
        chunker_manager = self._chunker_manager
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
        callback_mode: CallbackMode = CallbackMode.FULL,
    ) -> ConverterTaskResult:
        callback_invoker = CallbackInvoker() if task.callbacks else None
        temp_dir_kwargs: dict[str, Any] = {
            "prefix": f"docling_converter_{task.task_id}_",
        }
        if self.config.scratch_dir is not None:
            temp_dir_kwargs["dir"] = str(self.config.scratch_dir)
            Path(temp_dir_kwargs["dir"]).mkdir(exist_ok=True, parents=True)

        with tempfile.TemporaryDirectory(**temp_dir_kwargs) as temp_dir:
            workdir = Path(temp_dir)
            if task.task_type == TaskType.CONVERT:
                processed = _process_exportable_results_internal(
                    task=task,
                    exportable_documents=exportable_documents,
                    work_dir=workdir,
                    s3_presigned_config=self.config.s3_presigned_config,
                    callback_invoker=callback_invoker,
                    debug_error_details=self.config.debug_error_details,
                    expected_doc_count=expected_doc_count,
                    start_time=start_time,
                    callback_mode=callback_mode,
                )
                task_result = processed.task_result
                processed_docs = processed.processed_docs
            elif task.task_type == TaskType.CHUNK:
                task_result = process_chunkable_results(
                    task=task,
                    exportable_documents=exportable_documents,
                    work_dir=workdir,
                    chunker_manager=self._get_chunker_manager(),
                    callback_invoker=callback_invoker,
                    debug_error_details=self.config.debug_error_details,
                    expected_doc_count=expected_doc_count,
                    start_time=start_time,
                )
                processed_docs = []
            else:
                raise ValueError(f"Unsupported task type: {task.task_type}")

        return ConverterTaskResult(
            task_result=task_result,
            processed_docs=processed_docs,
        )

    def _convert_passthrough_task(self, task: Task) -> list[ConversionResult]:
        _validate_no_s3_source_in_passthrough(task)
        convert_sources, headers = expand_task_sources(
            task,
            max_file_size=self.converter_manager_config.max_file_size,
        )
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
                sources=[
                    DocumentStream(name=request.filename, stream=BytesIO(payload))
                ],
                options=request.task.convert_options or ConvertDocumentsOptions(),
            )
        )

    def _convert_source_chunk_request(
        self, request: SourceChunkConvertRequest
    ) -> list[ConversionResult]:
        with get_source_processor(request.chunk.source) as source_processor:
            convert_sources: list[str | DocumentStream] = []
            headers: Optional[dict[str, Any]] = None
            for ref in request.chunk.refs:
                convert_source = source_processor.fetch_converter_source_by_ref(
                    ref,
                    max_file_size=self.converter_manager_config.max_file_size,
                )
                convert_sources.append(convert_source)
                if not isinstance(convert_source, str):
                    continue
                ref_headers = source_processor.headers_for_ref(ref)
                if headers is None and ref_headers:
                    headers = ref_headers

        return list(
            self.cm.convert_documents(
                sources=convert_sources,
                options=request.task.convert_options or ConvertDocumentsOptions(),
                headers=headers,
            )
        )

    def _process_slice_convert(
        self, request: SliceConvertRequest
    ) -> tuple[ObjectRef, ConversionStatus]:
        payload = ray.get(request.artifact_ref)
        options = request.options.model_copy(update={"page_range": request.page_range})
        conv_results = list(
            self.cm.convert_documents(
                sources=[
                    DocumentStream(name=request.filename, stream=BytesIO(payload))
                ],
                options=options,
            )
        )
        if not conv_results:
            raise RuntimeError("Slice conversion returned no results")

        exportable = ExportableDocument.from_conversion_result(
            conv_results[0],
            source_index=0,
            source_uri=request.filename,
            page_range=request.page_range,
            slice_index=request.slice_index,
        )
        # Move the ExportableDocument (which holds a full DoclingDocument) to the
        # Ray plasma store. The coordinator receives only an ObjectRef handle so
        # the large document object never lands on the coordinator's heap until it
        # is explicitly loaded inside the semaphore-guarded finalization step.
        return ray.put(exportable), exportable.status

    async def _check_memory(self) -> None:
        if not PSUTIL_AVAILABLE:
            return

        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            limit_str = self.config.converter_actor_memory_request
            if not limit_str:
                return

            limit_bytes = parse_memory_bytes(limit_str)
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
        self._slice_finalization_semaphore = asyncio.Semaphore(
            self.config.max_concurrent_coordinator_slice_finalizations
        )

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

            if isinstance(result, ConverterFailureResult):
                self.tasks_processed += 1
                self.documents_processed += task_size
                self.last_task_time = datetime.datetime.now(datetime.timezone.utc)
                await self._finalize_client_actionable_task_failure(
                    task=task,
                    tenant_id=tenant_id,
                    task_size=task_size,
                    failure=result.failure,
                )
                duration = (
                    (self.last_task_time - task_start).total_seconds()
                    if self.last_task_time
                    else 0.0
                )
                _log.info(
                    "Coordinator replica %s: task %s failed in %.2fs: %s",
                    self.replica_id,
                    task.task_id,
                    duration,
                    result.failure.message,
                )
                return _failed_task_placeholder(task_size)

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
            failure = classify_ray_public_task_failure(
                exc,
                task_id=task.task_id,
                phase=FailurePhase.EXECUTION,
                details={
                    "task_size": str(task_size),
                    "target_kind": task.target.kind,
                },
            )
            error_message = failure.message
            terminalization = await self.redis_manager.finalize_task_failure_atomic(
                tenant_id=tenant_id,
                task_id=task.task_id,
                task_size=task_size,
                error_message=error_message,
                failure=failure,
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
                            failure=failure,
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

    async def _finalize_client_actionable_task_failure(
        self,
        *,
        task: Task,
        tenant_id: str,
        task_size: int,
        failure: PublicFailureInfo,
    ) -> None:
        """Persist and publish a terminal failure produced before conversion starts."""
        error_message = failure.message
        terminalization = await self.redis_manager.finalize_task_failure_atomic(
            tenant_id=tenant_id,
            task_id=task.task_id,
            task_size=task_size,
            error_message=error_message,
            failure=failure,
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
                        failure=failure,
                    )
                )
                await self.redis_manager.update_tenant_stats(
                    tenant_id,
                    delta_total_tasks=1,
                    delta_total_documents=task_size,
                    delta_failed_documents=task_size,
                )
            except Exception as follow_up_exc:  # pragma: no cover - observability only
                _log.warning(
                    "Coordinator replica %s: durable failure follow-up failed for %s: %s",
                    self.replica_id,
                    task.task_id,
                    follow_up_exc,
                )

    async def _process_task(
        self, task: Task, workdir: Path
    ) -> DoclingTaskResult | ConverterFailureResult:
        if task.task_type == TaskType.CONVERT:
            return await self._process_convert_task(task, workdir)
        if task.task_type == TaskType.CHUNK:
            return await self._process_chunk_task(task, workdir)
        raise ValueError(f"Unknown task type: {task.task_type}")

    async def _process_convert_task(
        self, task: Task, workdir: Path
    ) -> DoclingTaskResult | ConverterFailureResult:
        convert_options = task.convert_options or ConvertDocumentsOptions()
        materialized_start_time = time.monotonic()

        if _is_s3_fanout_task(task):
            return await self._process_s3_fanout_task(task, materialized_start_time)

        if self._should_materialize_pdf(task):
            # "Passthrough" keeps the original task sources intact and lets the
            # converter expand or fetch them when it executes the task. We only
            # materialize for the single-PDF fanout path, where the coordinator
            # must read one PDF up front to enforce preflight limits, determine
            # page count, and share the same bytes across slice requests.
            source = task.sources[0]
            if not isinstance(source, (FileSource, HttpSource)):
                raise TypeError(
                    "Materialized PDF path only supports FileSource and HttpSource"
                )
            try:
                materialized = await materialize_and_preflight(
                    source,
                    limits=MaterializationLimits(
                        max_file_size=self.converter_manager_config.max_file_size,
                        max_num_pages=self.converter_manager_config.max_num_pages,
                    ),
                )
            except MaterializationLimitExceededError as exc:
                return _build_materialization_failure_result(
                    task, source, exc, materialized_start_time
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
                    # _run_slice_plan returns ObjectRefs pointing to ExportableDocuments
                    # in the plasma store. The coordinator holds only handles here —
                    # the actual document data stays in plasma until _finalize_slice_results
                    # loads it inside the semaphore guard below.
                    slice_refs = await self._run_slice_plan(
                        artifact_ref=artifact_ref,
                        filename=filename,
                        slice_plan=slice_plan,
                        options=convert_options,
                        task=task,
                    )
                    callback_invoker = CallbackInvoker() if task.callbacks else None
                    try:
                        async with self._slice_finalization_semaphore:
                            return await asyncio.to_thread(
                                _finalize_slice_results,
                                task=task,
                                slice_refs=slice_refs,
                                work_dir=workdir,
                                s3_presigned_config=self.config.s3_presigned_config,
                                callback_invoker=callback_invoker,
                                start_time=materialized_start_time,
                                debug_error_details=self.config.debug_error_details,
                            )
                    finally:
                        # Release ObjectRefs so plasma can free the slice documents
                        # once finalization is done. Error shells (inline
                        # ExportableDocuments) are also released here.
                        del slice_refs
                else:
                    converter_result = await self._run_single_converter_call(
                        task,
                        MaterializedConvertRequest(
                            artifact_ref=artifact_ref,
                            filename=filename,
                            task=task.model_copy(update={"sources": []}),
                            source_count=len(task.sources),
                        ),
                    )
                    if isinstance(converter_result, ConverterFailureResult):
                        return converter_result
                    return converter_result.task_result
            finally:
                del artifact_ref
        else:
            converter_result = await self._run_single_converter_call(
                task, PassthroughTaskRequest(task=task)
            )
            if isinstance(converter_result, ConverterFailureResult):
                return converter_result
            return converter_result.task_result

    async def _process_chunk_task(
        self, task: Task, workdir: Path
    ) -> DoclingTaskResult | ConverterFailureResult:
        del workdir
        converter_result = await self._run_single_converter_call(
            task, PassthroughTaskRequest(task=task)
        )
        if isinstance(converter_result, ConverterFailureResult):
            return converter_result
        return converter_result.task_result

    async def _converter_unit_ceiling(self, tenant_id: str) -> int:
        """Per-tenant in-flight converter-unit ceiling (= max_concurrent_tasks)."""
        limits = await self.redis_manager.get_tenant_limits(tenant_id)
        return limits.max_concurrent_tasks

    async def _acquire_converter_unit_blocking(
        self, tenant_id: str, task_id: str, ceiling: int
    ) -> bool:
        """Block until one converter unit is acquired for the task.

        Returns False if the task no longer holds an execution lease
        (terminalized/reconciled) so the caller can abort.
        """
        while True:
            granted = await self.redis_manager.acquire_converter_unit(
                tenant_id, task_id, ceiling
            )
            if granted < 0:
                return False
            if granted >= 1:
                return True
            await asyncio.sleep(_CONVERTER_UNIT_POLL_INTERVAL_S)

    async def _run_single_converter_call(self, task: Task, request: Any) -> Any:
        """Dispatch a single (non-fanned-out) converter call under one converter unit."""
        tenant_id = task.metadata.get("tenant_id", "default")
        ceiling = await self._converter_unit_ceiling(tenant_id)
        acquired = await self._acquire_converter_unit_blocking(
            tenant_id, task.task_id, ceiling
        )
        if not acquired:
            raise RuntimeError(
                f"Task {task.task_id} was terminalized before converter dispatch"
            )
        try:
            return await self.converter_handle.process_converter_request.remote(request=request, tenant_id=tenant_id)
        finally:
            await self.redis_manager.release_converter_units(tenant_id, task.task_id, 1)

    def _iter_source_chunks_for_s3_fanout(
        self, task: Task
    ) -> Iterator[DocumentChunk[Any, Any]]:
        source_index_offset = 0
        chunk_index = 0
        for source in task.sources:
            if isinstance(source, DocumentStream):
                raise TypeError(
                    "Raw DocumentStream sources are not supported in Ray source-chunk fan-out"
                )
            with get_source_processor(source) as source_processor:
                source_doc_count = 0
                for chunk in source_processor.iterate_document_chunks(
                    self.config.s3_dispatch_batch_size
                ):
                    adjusted_chunk = _offset_chunk_refs(
                        chunk, source_index_offset, chunk_index
                    )
                    yield adjusted_chunk
                    source_doc_count += len(adjusted_chunk.refs)
                    chunk_index += 1
                source_index_offset += source_doc_count

    async def _process_s3_fanout_task(
        self, task: Task, task_start: float
    ) -> DoclingTaskResult:
        # Pre-collect all source chunk metadata in a thread pool worker so that
        # S3 list_objects_v2 pagination — synchronous network I/O in boto3 — never
        # blocks the coordinator's event loop. The coordinator handles up to
        # max_ongoing_requests tasks concurrently; a blocked event loop would stall
        # heartbeats, dispatch calls, and all other in-flight tasks on this replica.
        #
        # Using asyncio.to_thread moves the entire listing into a worker thread.
        # asyncio.wait_for adds a hard outer time limit that catches:
        #   - unreachable S3 endpoints (connect_timeout in boto3 caps per-attempt,
        #     but listing a large bucket requires many paginator calls)
        #   - misconfigured credentials that cause silent retries or hangs
        #   - pathological S3 responses that are individually within boto3's
        #     read_timeout but collectively stall progress
        # If the timeout fires, asyncio.wait_for cancels the asyncio.to_thread
        # future, but the underlying OS thread will continue running until boto3's
        # own socket timeout triggers and the thread returns. The thread cannot be
        # forcibly killed — it exits naturally when boto3 raises. The coordinator
        # task is already marked failed at that point so the thread result is
        # discarded.
        #
        # Memory: chunk refs hold only key strings and metadata (not file bytes),
        # so pre-collecting the full listing is bounded — ~200 bytes per key.
        timeout = self.config.s3_source_listing_timeout_s
        try:
            all_chunks: list[DocumentChunk[Any, Any]] = await asyncio.wait_for(
                asyncio.to_thread(
                    lambda: list(self._iter_source_chunks_for_s3_fanout(task))
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"S3 source listing timed out after {timeout}s — verify that the "
                "S3 endpoint is reachable and the supplied credentials are valid."
            )

        if not all_chunks:
            raise RuntimeError("No S3 source documents were found for fan-out task.")

        total_docs = sum(len(chunk.refs) for chunk in all_chunks)
        callback_invoker = CallbackInvoker() if task.callbacks else None
        if callback_invoker and task.callbacks and total_docs:
            callback_invoker.invoke_callbacks_async(
                callbacks=task.callbacks,
                task_id=task.task_id,
                progress=ProgressSetNumDocs(num_docs=total_docs),
            )

        # Fan-out is gated purely by the tenant's in-flight converter-unit budget
        # (= max_concurrent_tasks). A child only launches once a unit is acquired;
        # in-flight count therefore self-adjusts to available tenant capacity with
        # no separate per-task parallelism knob.
        #
        # NOTE: this acquire/launch/drain loop is intentionally duplicated in
        # _dispatch_slice_conversions. The bookkeeping differs just enough
        # (chunk-keyed results and per-child failure wrapping here vs. bare
        # ObjectRefs there) that a shared abstraction would obscure the gating
        # logic. Keep the acquire/release semantics of both loops in sync.
        tenant_id = task.metadata.get("tenant_id", "default")
        ceiling = await self._converter_unit_ceiling(tenant_id)
        pending_chunks = iter(all_chunks)
        in_flight: dict[asyncio.Task[ConverterTaskResult], DocumentChunk[Any, Any]] = {}
        child_results: list[tuple[DocumentChunk[Any, Any], ConverterTaskResult]] = []
        next_chunk = next(pending_chunks, None)

        try:
            while next_chunk is not None or in_flight:
                while next_chunk is not None:
                    granted = await self.redis_manager.acquire_converter_unit(
                        tenant_id, task.task_id, ceiling
                    )
                    if granted < 0:
                        raise RuntimeError(
                            f"Task {task.task_id} was terminalized during S3 fan-out"
                        )
                    if granted == 0:
                        break  # at tenant ceiling; drain an in-flight child first
                    in_flight[
                        asyncio.create_task(
                            self._execute_source_chunk(next_chunk, task, total_docs)
                        )
                    ] = next_chunk
                    next_chunk = next(pending_chunks, None)
                if not in_flight:
                    # Fully starved: budget held by sibling tasks. Back off, retry.
                    await asyncio.sleep(_CONVERTER_UNIT_POLL_INTERVAL_S)
                    continue
                done, _ = await asyncio.wait(
                    set(in_flight), return_when=asyncio.FIRST_COMPLETED
                )
                for completed in done:
                    chunk = in_flight.pop(completed)
                    await self.redis_manager.release_converter_units(
                        tenant_id, task.task_id, 1
                    )
                    try:
                        converter_result = await completed
                    except Exception as exc:
                        # An unexpected/raised chunk failure: degrade to a
                        # document-level FAILURE for this chunk so sibling chunks
                        # still complete instead of aborting the whole task.
                        _log.warning(
                            "Coordinator replica %s: source chunk %s for task %s failed: %s",
                            self.replica_id,
                            chunk.chunk_index,
                            task.task_id,
                            exc,
                        )
                        converter_result = self._handle_failed_source_chunk(
                            chunk=chunk,
                            error=build_public_error_item(exc),
                            task=task,
                            total_docs=total_docs,
                            callback_invoker=callback_invoker,
                        )
                    else:
                        if isinstance(converter_result, ConverterFailureResult):
                            # A per-source converter request returned a structured,
                            # client-actionable failure (e.g. a missing or too-large
                            # S3 object). The fan-out aggregator only understands
                            # ConverterTaskResult, so without this the whole task
                            # would abort on a single bad source. Record it as a
                            # document-level FAILURE and keep going.
                            _log.warning(
                                "Coordinator replica %s: source chunk %s for task %s "
                                "returned client-actionable failure: %s",
                                self.replica_id,
                                chunk.chunk_index,
                                task.task_id,
                                converter_result.failure.message,
                            )
                            converter_result = self._handle_failed_source_chunk(
                                chunk=chunk,
                                error=build_public_error_item_from_failure(
                                    converter_result.failure
                                ),
                                task=task,
                                total_docs=total_docs,
                                callback_invoker=callback_invoker,
                            )
                    child_results.append((chunk, converter_result))
        finally:
            if in_flight:
                for child_task in in_flight:
                    child_task.cancel()
                await asyncio.gather(*in_flight, return_exceptions=True)
                await self.redis_manager.release_converter_units(
                    tenant_id, task.task_id, len(in_flight)
                )

        ordered_results = [
            converter_result
            for _, converter_result in sorted(
                child_results, key=lambda item: item[0].chunk_index
            )
        ]
        aggregated = _aggregate_s3_fanout_results(
            task=task,
            child_results=ordered_results,
            processing_time=time.monotonic() - task_start,
        )
        if callback_invoker and task.callbacks:
            callback_invoker.invoke_callbacks_async(
                callbacks=task.callbacks,
                task_id=task.task_id,
                progress=ProgressUpdateProcessed(
                    num_processed=sum(
                        len(converter_result.processed_docs)
                        for converter_result in ordered_results
                    ),
                    num_succeeded=aggregated.num_succeeded,
                    num_partially_succeeded=aggregated.num_partially_succeeded,
                    num_failed=aggregated.num_failed,
                    docs=[
                        processed_doc
                        for converter_result in ordered_results
                        for processed_doc in converter_result.processed_docs
                    ],
                ),
            )

        return aggregated

    def _handle_failed_source_chunk(
        self,
        *,
        chunk: DocumentChunk[Any, Any],
        error: ErrorItem,
        task: Task,
        total_docs: int,
        callback_invoker: Optional[CallbackInvoker],
    ) -> ConverterTaskResult:
        """Turn a failed source chunk into a document-level FAILURE result.

        Shared by both the raised-exception and the returned-
        ``ConverterFailureResult`` paths in the fan-out drain loop, so that a
        single unreachable/invalid source becomes a per-document failure instead
        of aborting the whole task, and emits the same completion callbacks.
        """
        converter_result = _build_failed_source_chunk_result(
            chunk,
            error,
            target=task.target,
            debug_error_details=self.config.debug_error_details,
        )
        if callback_invoker and task.callbacks:
            for ref in chunk.refs:
                exportable_document = _build_failed_source_chunk_exportable_document(
                    ref, error
                )
                _maybe_emit_document_completed(
                    callback_invoker=callback_invoker,
                    callbacks=task.callbacks,
                    task_id=task.task_id,
                    exportable_document=exportable_document,
                    total_processed=1,
                    total_docs=total_docs,
                    callback_mode=_SOURCE_CHUNK_CALLBACK_MODE,
                    debug_error_details=self.config.debug_error_details,
                )
        return converter_result

    async def _execute_source_chunk(
        self,
        chunk: DocumentChunk[Any, Any],
        task: Task,
        expected_doc_count: int,
    ) -> ConverterTaskResult:
        child_task = task.model_copy(update={"sources": [chunk.source]})
        tenant_id = task.metadata.get("tenant_id", "default")
        # Strip the fetcher before cross-process dispatch. The fetcher is a bound
        # method of the coordinator's initialized S3SourceProcessor, which holds a
        # boto3 client containing thread locks. Pydantic v2 serializes
        # __pydantic_private__ via __getstate__, so the fetcher would be included
        # when Ray cloudpickle serializes the chunk — causing a TypeError on thread
        # locks. The converter never calls iter_documents(); it reconstructs its own
        # source processor from chunk.source and uses fetch_converter_source_by_ref().
        serializable_chunk = DocumentChunk(
            source=chunk.source, refs=chunk.refs, chunk_index=chunk.chunk_index
        )
        return await self.converter_handle.process_converter_request.remote(
            request=SourceChunkConvertRequest(
                task=child_task,
                chunk=serializable_chunk,
                expected_doc_count=expected_doc_count,
            ),
            tenant_id=tenant_id
        )

    def _should_materialize_pdf(self, task: Task) -> bool:
        return (
            self.config.enable_pdf_page_slice_fanout
            and task.task_type == TaskType.CONVERT
            and len(task.sources) == 1
            and isinstance(task.sources[0], (FileSource, HttpSource))
            and _is_pdf_source(task.sources[0])
        )

    async def _run_slice_plan(
        self,
        artifact_ref: Any,
        filename: str,
        slice_plan: SlicePlan,
        options: ConvertDocumentsOptions,
        task: Task,
    ) -> list[ObjectRef]:
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

        # Slice fan-out is gated by the tenant converter-unit budget, like S3
        # fan-out — no per-task parallelism knob.
        #
        # NOTE: this acquire/launch/drain loop is intentionally duplicated in
        # _handle_s3_fanout (which additionally wraps per-child failures and
        # keys results by chunk). A shared abstraction would obscure the gating
        # logic. Keep the acquire/release semantics of both loops in sync.
        tenant_id = task.metadata.get("tenant_id", "default")
        ceiling = await self._converter_unit_ceiling(tenant_id)
        in_flight: set[asyncio.Task[ObjectRef]] = set()
        pending_requests = iter(requests)
        collected_results: list[ObjectRef] = []
        next_request = next(pending_requests, None)

        try:
            while next_request is not None or in_flight:
                while next_request is not None:
                    granted = await self.redis_manager.acquire_converter_unit(
                        tenant_id, task.task_id, ceiling
                    )
                    if granted < 0:
                        raise RuntimeError(
                            f"Task {task.task_id} was terminalized during slice fan-out"
                        )
                    if granted == 0:
                        break  # at tenant ceiling; drain an in-flight slice first
                    in_flight.add(
                        asyncio.create_task(self._execute_slice_request(request=next_request, tenant_id=tenant_id))
                    )
                    next_request = next(pending_requests, None)
                if not in_flight:
                    await asyncio.sleep(_CONVERTER_UNIT_POLL_INTERVAL_S)
                    continue
                done, in_flight = await asyncio.wait(
                    in_flight, return_when=asyncio.FIRST_COMPLETED
                )
                for completed in done:
                    await self.redis_manager.release_converter_units(
                        tenant_id, task.task_id, 1
                    )
                    collected_results.append(await completed)
        finally:
            if in_flight:
                for slice_task in in_flight:
                    slice_task.cancel()
                await asyncio.gather(*in_flight, return_exceptions=True)
                await self.redis_manager.release_converter_units(
                    tenant_id, task.task_id, len(in_flight)
                )

        return collected_results

    async def _execute_slice_request(self, request: SliceConvertRequest, tenant_id: str) -> ObjectRef:
        try:
            # On success the converter returns an ObjectRef pointing to the
            # ExportableDocument in the plasma store — the coordinator never
            # holds the document object itself while waiting for other slices.
            return await self.converter_handle.process_converter_request.remote(request=request, tenant_id=tenant_id)
        except Exception as exc:
            _log.warning(
                "Coordinator replica %s: slice %s for %s failed: %s",
                self.replica_id,
                request.page_range,
                request.filename,
                exc,
            )
            return ray.put(
                _build_failed_slice_result(
                    filename=request.filename,
                    page_range=request.page_range,
                    slice_index=request.slice_index,
                    exc=exc,
                    debug_error_details=self.config.debug_error_details,
                )
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


def _build_deployment_options(
    *,
    name: str,
    min_replicas: int,
    max_replicas: int,
    target_requests_per_replica: float,
    max_ongoing_requests: float,
    num_cpus: float,
    memory_limit: Optional[str],
    upscale_delay_s: float,
    downscale_delay_s: float,
    graceful_shutdown_wait_loop_s: Optional[float],
    graceful_shutdown_timeout_s: Optional[float],
    max_replicas_per_node: Optional[int] = None,
) -> dict[str, Any]:
    deployment_options: dict[str, Any] = {
        "name": name,
        "autoscaling_config": {
            "min_replicas": min_replicas,
            "max_replicas": max_replicas,
            "target_ongoing_requests": target_requests_per_replica,
            "upscale_delay_s": upscale_delay_s,
            "downscale_delay_s": downscale_delay_s,
        },
        "ray_actor_options": {"num_cpus": num_cpus},
        "max_ongoing_requests": max_ongoing_requests,
    }

    if max_replicas_per_node is not None:
        deployment_options["max_replicas_per_node"] = max_replicas_per_node

    memory_bytes = parse_memory_bytes(memory_limit)
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
    app_name: str = DEFAULT_SERVE_APP_NAME,
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
        max_replicas_per_node=config.converter_max_replicas_per_node,
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
        max_replicas_per_node=config.coordinator_max_replicas_per_node,
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
    app_name: str = DEFAULT_SERVE_APP_NAME,
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
