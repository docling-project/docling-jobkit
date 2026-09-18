"""Result construction for extraction tasks."""

import logging
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.extraction import DocumentExtractionResult
from docling.datamodel.service.callbacks import ProcessedDocsItem
from docling.datamodel.service.responses import (
    DoclingTaskResult,
    DocumentArtifactItem,
    ExtractionDocumentResult,
    ExtractionTaskResult,
    PresignedArtifactResult,
    RemoteTargetResult,
    ResultType,
)
from docling.datamodel.service.targets import InBodyTarget

from docling_jobkit.config.target_config import PresignedConfig
from docling_jobkit.connectors.artifact_paths import build_task_scoped_key
from docling_jobkit.connectors.connector_factory import get_target_connector_factory
from docling_jobkit.connectors.target_processor_factory import get_target_processor
from docling_jobkit.convert.results import (
    CallbackMode,
    _build_failed_exportable_document,
    _build_processed_docs_item,
    _maybe_emit_document_completed,
    _maybe_emit_update_processed,
)
from docling_jobkit.datamodel.exportable_document import ExportableDocument
from docling_jobkit.datamodel.source_identity import SourceIdentity
from docling_jobkit.datamodel.task import Task

if TYPE_CHECKING:
    from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker

_log = logging.getLogger(__name__)


def to_result_item(
    result: DocumentExtractionResult, source: SourceIdentity
) -> ExtractionDocumentResult:
    return ExtractionDocumentResult(
        source_index=source.source_index,
        source_uri=source.source_uri,
        filename=result.input.file.name,
        status=result.status,
        errors=result.errors,
        items=result.items,
    )


def _callback_document(
    result: DocumentExtractionResult, source: SourceIdentity
) -> ExportableDocument:
    return ExportableDocument(
        file=result.input.file,
        document_type=result.input.format,
        status=result.status,
        errors=result.errors,
        source_index=source.source_index,
        source_uri=source.source_uri,
    )


def process_extraction_results(
    task: Task,
    extraction_results: Iterable[DocumentExtractionResult],
    identities: list[SourceIdentity],
    *,
    presigned_config: PresignedConfig | None = None,
    callback_invoker: "CallbackInvoker | None" = None,
    debug_error_details: bool = False,
    allow_external_plugins: bool = False,
    start_time: float | None = None,
) -> tuple[DoclingTaskResult, list[ProcessedDocsItem]]:
    """Build one result and emit authoritative per-document callbacks."""
    start_time = start_time if start_time is not None else time.monotonic()
    results = list(extraction_results)
    if not results:
        raise RuntimeError("No documents were extracted.")
    if len(results) != len(identities):
        raise RuntimeError("Extraction results do not match expanded sources.")
    targets = task.targets or []
    if len(targets) > 1:
        raise ValueError("Extraction supports exactly one target.")

    target = targets[0] if targets else InBodyTarget()
    target_factory = get_target_connector_factory(allow_external_plugins)
    target_mode = (
        target_factory.result_mode(target) if target_factory.supports(target) else None
    )
    if target_mode == "database":
        raise ValueError("Database/vector targets are not supported for extraction.")

    items: list[ExtractionDocumentResult] = []
    processed_docs: list[ProcessedDocsItem] = []
    presigned_documents: list[DocumentArtifactItem] = []
    with (
        tempfile.TemporaryDirectory(prefix="docling_extract_") as tmp,
        ExitStack() as stack,
    ):
        tmp_dir = Path(tmp)
        processor: Any = None
        target_error: Exception | None = None
        if target_mode in {"artifacts", "presigned"}:
            try:
                processor = stack.enter_context(
                    get_target_processor(
                        target,
                        allow_external_plugins=allow_external_plugins,
                        presigned_config=presigned_config,
                        task=task,
                    )
                )
            except Exception as exc:
                target_error = exc
        for index, (result, source) in enumerate(zip(results, identities)):
            item = to_result_item(result, source)
            callback_document = _callback_document(result, source)
            presigned_document = None
            if processor is not None or target_error is not None:
                stem = item.filename.rsplit(".", 1)[0] or item.filename
                target_filename = f"{stem}.json"
                try:
                    if target_error is not None:
                        raise target_error
                    if target_mode == "presigned":
                        json_path = tmp_dir / f"{index:06d}.json"
                        json_path.write_text(
                            item.model_dump_json(indent=2), encoding="utf-8"
                        )
                        processor.upload_artifact_file(
                            source=source,
                            artifact_type="json",
                            path=json_path,
                            target_filename=target_filename,
                            mime_type="application/json",
                        )
                    else:
                        processor.upload_object(
                            obj=item.model_dump_json(indent=2).encode("utf-8"),
                            target_filename=build_task_scoped_key(
                                key_prefix="",
                                date_partition_format="",
                                task=task,
                                source_uri=source.source_uri,
                                artifact_filename=target_filename,
                            ),
                            content_type="application/json",
                        )
                    if target_mode == "presigned":
                        presigned_document = processor.build_document_artifact_item(
                            source=source,
                            filename=item.filename,
                            status=item.status,
                            errors=item.errors,
                            timings={},
                        )
                except Exception as exc:
                    callback_document = _build_failed_exportable_document(
                        callback_document,
                        exc,
                        debug_error_details=debug_error_details,
                    )
                    item = item.model_copy(
                        update={
                            "status": callback_document.status,
                            "errors": callback_document.errors,
                        }
                    )
                    if target_mode == "presigned":
                        presigned_document = DocumentArtifactItem(
                            source_index=source.source_index,
                            source_uri=source.source_uri,
                            filename=item.filename,
                            status=item.status,
                            errors=item.errors,
                        )
            if target_mode == "presigned":
                if presigned_document is None:
                    raise RuntimeError("Presigned extraction target is unavailable.")
                presigned_documents.append(presigned_document)

            items.append(item)
            processed_docs.append(
                _build_processed_docs_item(
                    callback_document, debug_error_details=debug_error_details
                )
            )
            _maybe_emit_document_completed(
                callback_invoker=callback_invoker,
                callbacks=task.callbacks,
                task_id=task.task_id,
                exportable_document=callback_document,
                total_processed=index + 1,
                total_docs=len(results),
                callback_mode=CallbackMode.FULL,
                debug_error_details=debug_error_details,
            )

    num_succeeded = sum(i.status == ConversionStatus.SUCCESS for i in items)
    num_partially_succeeded = sum(
        i.status == ConversionStatus.PARTIAL_SUCCESS for i in items
    )
    num_failed = len(items) - num_succeeded - num_partially_succeeded
    _maybe_emit_update_processed(
        callback_invoker=callback_invoker,
        callbacks=task.callbacks,
        task_id=task.task_id,
        processed_docs=processed_docs,
        num_succeeded=num_succeeded,
        num_partially_succeeded=num_partially_succeeded,
        num_failed=num_failed,
        callback_mode=CallbackMode.FULL,
    )

    result_payload: ResultType
    if target_mode == "presigned":
        result_payload = PresignedArtifactResult(documents=presigned_documents)
    elif target_mode == "artifacts":
        result_payload = RemoteTargetResult()
    else:
        result_payload = ExtractionTaskResult(documents=items)

    processing_time = time.monotonic() - start_time
    _log.info("Extracted %s docs in %.2f seconds.", len(items), processing_time)
    return (
        DoclingTaskResult(
            result=result_payload,
            processing_time=processing_time,
            num_succeeded=num_succeeded,
            num_partially_succeeded=num_partially_succeeded,
            num_failed=num_failed,
            num_converted=len(items),
        ),
        processed_docs,
    )
