import logging
import shutil
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import httpx

from docling.datamodel.base_models import InputFormat, OutputFormat
from docling.datamodel.document import ConversionStatus
from docling.datamodel.service.callbacks import (
    DocumentCompletedItem,
    ProcessedDocsItem,
    ProgressDocumentCompleted,
    ProgressSetNumDocs,
    ProgressUpdateProcessed,
)
from docling.datamodel.service.targets import (
    InBodyTarget,
    PresignedUrlTarget,
    PutTarget,
    S3Target,
)
from docling_core.types.doc import ImageRefMode

from docling_jobkit.config.target_config import S3PresignedConfig
from docling_jobkit.connectors.artifact_paths import (
    hash_path_component,
)
from docling_jobkit.connectors.s3_presigned_target_processor import (
    S3PresignedTargetProcessor,
)
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.connectors.target_processor_factory import get_target_processor
from docling_jobkit.convert.export import (
    _cleanup_document_output_dir,
    _is_exportable_status,
    _materialize_document_exports,
    _release_exportable_document_references,
    _upload_exportable_document,
)
from docling_jobkit.datamodel.exportable_document import (
    ExportableDocument,
    source_to_public_uri,
)
from docling_jobkit.datamodel.result import (
    DoclingTaskResult,
    DocumentArtifactItem,
    DocumentResultItem,
    ExportDocumentResponse,
    PresignedArtifactResult,
    RemoteTargetResult,
    ResultType,
    ZipArchiveResult,
)
from docling_jobkit.datamodel.source_identity import SourceIdentity
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_targets import (
    GoogleDriveTarget,
    LocalPathTarget,
)
from docling_jobkit.public_errors import (
    TargetWriteError,
    build_public_error_item,
    render_public_error_list,
)

if TYPE_CHECKING:
    from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker

_log = logging.getLogger(__name__)


@dataclass
class _ProcessedExportResults:
    task_result: DoclingTaskResult
    processed_docs: list[ProcessedDocsItem]


class CallbackMode(str, Enum):
    """Internal callback emission mode for shared result processing."""

    FULL = "full"
    CHILD_ONLY = "child_only"


def _count_document_statuses(
    exportable_documents: list[ExportableDocument],
) -> tuple[int, int, int]:
    num_succeeded = sum(
        1 for doc in exportable_documents if doc.status == ConversionStatus.SUCCESS
    )
    num_partially_succeeded = sum(
        1
        for doc in exportable_documents
        if doc.status == ConversionStatus.PARTIAL_SUCCESS
    )
    num_failed = len(exportable_documents) - num_succeeded - num_partially_succeeded
    return num_succeeded, num_partially_succeeded, num_failed


def _build_processed_docs_item(
    exportable_document: ExportableDocument,
    *,
    debug_error_details: bool,
) -> ProcessedDocsItem:
    summary_error = render_public_error_list(
        exportable_document.errors,
        debug_enabled=debug_error_details,
    )
    return ProcessedDocsItem(
        source=str(exportable_document.file),
        status=exportable_document.status,
        error=summary_error
        or (
            "Unknown error"
            if not _is_exportable_status(exportable_document.status)
            else None
        ),
    )


def _build_failed_exportable_document(
    exportable_document: ExportableDocument,
    exc: Exception,
    *,
    debug_error_details: bool,
) -> ExportableDocument:
    return exportable_document.model_copy(
        update={
            "status": ConversionStatus.FAILURE,
            "errors": [
                *exportable_document.errors,
                build_public_error_item(exc),
            ],
            "document": None,
        }
    )


def _maybe_emit_set_num_docs(
    *,
    callback_invoker: Optional["CallbackInvoker"],
    callbacks: list,
    task_id: str,
    total_docs: int,
    callback_mode: CallbackMode,
) -> None:
    if (
        callback_invoker
        and callbacks
        and total_docs
        and callback_mode == CallbackMode.FULL
    ):
        callback_invoker.invoke_callbacks_async(
            callbacks=callbacks,
            task_id=task_id,
            progress=ProgressSetNumDocs(num_docs=total_docs),
        )


def _maybe_emit_document_completed(
    *,
    callback_invoker: Optional["CallbackInvoker"],
    callbacks: list,
    task_id: str,
    exportable_document: ExportableDocument,
    total_processed: int,
    total_docs: int,
    callback_mode: CallbackMode,
    debug_error_details: bool,
) -> None:
    if not callback_invoker or not callbacks:
        return

    processed_doc = _build_processed_docs_item(
        exportable_document,
        debug_error_details=debug_error_details,
    )
    callback_invoker.invoke_callbacks_async(
        callbacks=callbacks,
        task_id=task_id,
        progress=ProgressDocumentCompleted(
            document=_build_document_completed_item(
                exportable_document,
                error=processed_doc.error,
            ),
            total_processed=total_processed,
            total_docs=total_docs,
        ),
    )


def _maybe_emit_update_processed(
    *,
    callback_invoker: Optional["CallbackInvoker"],
    callbacks: list,
    task_id: str,
    processed_docs: list[ProcessedDocsItem],
    num_succeeded: int,
    num_partially_succeeded: int,
    num_failed: int,
    callback_mode: CallbackMode,
) -> None:
    if not callback_invoker or not callbacks or callback_mode != CallbackMode.FULL:
        return

    callback_invoker.invoke_callbacks_async(
        callbacks=callbacks,
        task_id=task_id,
        progress=ProgressUpdateProcessed(
            num_processed=len(processed_docs),
            num_succeeded=num_succeeded,
            num_partially_succeeded=num_partially_succeeded,
            num_failed=num_failed,
            docs=processed_docs,
        ),
    )


def _export_document_as_content(
    exportable_document: ExportableDocument,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    image_mode: ImageRefMode,
    md_page_break_placeholder: str,
) -> ExportDocumentResponse:
    document = ExportDocumentResponse(filename=exportable_document.file.name)

    if (
        _is_exportable_status(exportable_document.status)
        and exportable_document.document is not None
    ):
        new_doc = exportable_document.document._make_copy_with_refmode(
            Path(), image_mode, page_no=None
        )

        # Create the different formats
        if export_json:
            document.json_content = new_doc
        if export_html:
            document.html_content = new_doc.export_to_html(image_mode=image_mode)
        if export_txt:
            document.text_content = new_doc.export_to_markdown(
                strict_text=True,
                image_mode=image_mode,
            )
        if export_md:
            document.md_content = new_doc.export_to_markdown(
                image_mode=image_mode,
                page_break_placeholder=md_page_break_placeholder or None,
            )
        if export_doctags:
            document.doctags_content = new_doc.export_to_doctags()

    return document


def _build_document_completed_item(
    exportable_document: ExportableDocument,
    *,
    error: str | None,
) -> DocumentCompletedItem:
    document_type: InputFormat | None = exportable_document.document_type
    num_pages: int | None = None
    num_characters: int | None = None
    num_tables: int | None = None
    num_pictures: int | None = None
    if exportable_document.document is not None:
        num_pages = len(exportable_document.document.pages)
        markdown = exportable_document.document.export_to_markdown(
            image_mode=ImageRefMode.PLACEHOLDER
        )
        num_characters = len(markdown)
        num_tables = len(exportable_document.document.tables)
        num_pictures = len(exportable_document.document.pictures)

    return DocumentCompletedItem(
        source=str(exportable_document.file),
        status=exportable_document.status,
        document_type=document_type,
        num_pages=num_pages,
        num_characters=num_characters,
        num_tables=num_tables,
        num_pictures=num_pictures,
        processing_time=(
            sum(sum(item.times) for item in exportable_document.timings.values())
            if exportable_document.timings
            else None
        ),
        doc_hash=exportable_document.document_hash,
        error=error,
    )


def _export_documents_as_files(
    exportable_documents: Iterable[ExportableDocument],
    output_dir: Path,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    image_export_mode: ImageRefMode,
    md_page_break_placeholder: str,
):
    success_count = 0
    failure_count = 0

    for exportable_document in exportable_documents:
        if _materialize_document_exports(
            exportable_document,
            output_dir,
            export_json=export_json,
            export_html=export_html,
            export_md=export_md,
            export_txt=export_txt,
            export_doctags=export_doctags,
            image_export_mode=image_export_mode,
            md_page_break_placeholder=md_page_break_placeholder,
            bundle_resources=False,
        ):
            success_count += 1
        else:
            _log.warning(f"Document {exportable_document.file} failed to convert.")
            failure_count += 1

    _log.info(
        f"Processed {success_count + failure_count} docs, "
        f"of which {failure_count} failed"
    )
    return success_count, failure_count


def _upload_to_put_target(
    url: str,
    file_path: Path,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> None:
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            with file_path.open("rb") as file_data:
                r = httpx.put(url, files={"file": file_data})
                r.raise_for_status()
            return
        except Exception as exc:
            last_exc = exc
            _log.warning(
                "Upload to %s failed (attempt %d/%d): %s",
                url,
                attempt + 1,
                max_retries,
                exc,
            )
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
    raise TargetWriteError(
        f"Failed to upload to target URL after {max_retries} attempts."
    ) from last_exc


def _resolve_source_identity(
    task: Task,
    exportable_document: ExportableDocument,
    fallback_index: int,
) -> SourceIdentity:
    source_index = (
        exportable_document.source_index
        if exportable_document.source_index is not None
        else fallback_index
    )
    if exportable_document.source_uri is not None:
        return SourceIdentity(
            source_index=source_index,
            source_uri=exportable_document.source_uri,
            source_key=hash_path_component(exportable_document.source_uri),
        )

    if fallback_index < len(task.sources):
        source = task.sources[fallback_index]
        source_uri = source_to_public_uri(source) or str(exportable_document.file)
        return SourceIdentity(
            source_index=source_index,
            source_uri=source_uri,
            source_key=hash_path_component(source_uri),
        )

    source_uri = str(exportable_document.file)
    return SourceIdentity(
        source_index=source_index,
        source_uri=source_uri,
        source_key=hash_path_component(source_uri),
    )


def _upload_document_as_presigned_artifact(
    *,
    task: Task,
    exportable_document: ExportableDocument,
    response_index: int,
    output_dir: Path,
    target_processor: S3PresignedTargetProcessor,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    image_export_mode: ImageRefMode,
    md_page_break_placeholder: str,
) -> DocumentArtifactItem:
    source = _resolve_source_identity(task, exportable_document, response_index)
    document_dir = output_dir / f"{source.source_index:06d}"
    for artifact in _materialize_document_exports(
        exportable_document,
        document_dir,
        export_json=export_json,
        export_html=export_html,
        export_md=export_md,
        export_txt=export_txt,
        export_doctags=export_doctags,
        image_export_mode=image_export_mode,
        md_page_break_placeholder=md_page_break_placeholder,
        bundle_resources=True,
    ):
        target_processor.upload_artifact_file(
            source=source,
            artifact_type=artifact.artifact_type,
            path=artifact.path,
            target_filename=artifact.target_filename,
            mime_type=artifact.mime_type,
        )

    return target_processor.build_document_artifact_item(
        source=source,
        filename=exportable_document.file.name,
        status=exportable_document.status,
        errors=exportable_document.errors,
        timings=exportable_document.timings,
        confidence=exportable_document.confidence,
    )


def _upload_document_via_processor(
    *,
    task: Task,
    exportable_document: ExportableDocument,
    response_index: int,
    output_dir: Path,
    target_processor: BaseTargetProcessor,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    image_export_mode: ImageRefMode,
    md_page_break_placeholder: str,
    target_filename_fn: Callable[[SourceIdentity, str], str] | None = None,
) -> None:
    """Shared upload loop used by all non-presigned remote target processors.

    *target_filename_fn* is an optional callback
    ``(source, artifact_filename) -> target_filename`` that lets callers control
    the final target path.  When omitted the raw ``artifact.target_filename`` is
    used as-is.
    """
    source = _resolve_source_identity(task, exportable_document, response_index)
    document_dir = output_dir / f"{source.source_index:06d}"
    _upload_exportable_document(
        target_processor=target_processor,
        exportable_document=exportable_document,
        document_dir=document_dir,
        export_json=export_json,
        export_html=export_html,
        export_md=export_md,
        export_txt=export_txt,
        export_doctags=export_doctags,
        image_export_mode=image_export_mode,
        md_page_break_placeholder=md_page_break_placeholder,
        target_filename_fn=(
            (lambda fn: target_filename_fn(source, fn))
            if target_filename_fn is not None
            else (lambda fn: fn)
        ),
    )


def _upload_document_to_storage_target(
    *,
    task: Task,
    exportable_document: ExportableDocument,
    response_index: int,
    output_dir: Path,
    target_processor: BaseTargetProcessor,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    image_export_mode: ImageRefMode,
    md_page_break_placeholder: str,
) -> None:
    """Upload one document to any storage target via ``get_target_processor``.

    Used for every user-owned storage target (S3, local path, Google Drive, …):
    the per-source artifact layout (``<source_key>/<artifact>``) is identical, and
    each target processor applies its own prefix/root when writing.
    """
    _upload_document_via_processor(
        task=task,
        exportable_document=exportable_document,
        response_index=response_index,
        output_dir=output_dir,
        target_processor=target_processor,
        export_json=export_json,
        export_html=export_html,
        export_md=export_md,
        export_txt=export_txt,
        export_doctags=export_doctags,
        image_export_mode=image_export_mode,
        md_page_break_placeholder=md_page_break_placeholder,
        target_filename_fn=lambda source, artifact_filename: (
            f"{source.source_key}/{artifact_filename}"
        ),
    )


def _process_remote_document(
    *,
    task: Task,
    exportable_document: ExportableDocument,
    response_index: int,
    total_docs: int,
    output_dir: Path,
    callback_invoker: Optional["CallbackInvoker"],
    debug_error_details: bool,
    callback_mode: CallbackMode,
    upload_document: Callable[[SourceIdentity], Any],
    build_failure_result: Callable[[ExportableDocument, SourceIdentity], Any],
) -> tuple[ExportableDocument, ProcessedDocsItem, Any]:
    source = _resolve_source_identity(task, exportable_document, response_index)
    document_dir = output_dir / f"{source.source_index:06d}"
    final_document = exportable_document
    try:
        try:
            upload_result = upload_document(source)
        except Exception as exc:
            final_document = _build_failed_exportable_document(
                exportable_document,
                exc,
                debug_error_details=debug_error_details,
            )
            upload_result = build_failure_result(final_document, source)

        processed_doc = _build_processed_docs_item(
            final_document,
            debug_error_details=debug_error_details,
        )
        _maybe_emit_document_completed(
            callback_invoker=callback_invoker,
            callbacks=task.callbacks,
            task_id=task.task_id,
            exportable_document=final_document,
            total_processed=response_index + 1,
            total_docs=total_docs,
            callback_mode=callback_mode,
            debug_error_details=debug_error_details,
        )
        return final_document, processed_doc, upload_result
    finally:
        _release_exportable_document_references(exportable_document, final_document)
        _cleanup_document_output_dir(document_dir)


def _process_remote_exportable_results(
    *,
    task: Task,
    exportable_documents: Iterable[ExportableDocument],
    work_dir: Path,
    s3_presigned_config: S3PresignedConfig | None,
    callback_invoker: Optional["CallbackInvoker"],
    debug_error_details: bool,
    total_docs: int,
    callback_mode: CallbackMode,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    image_export_mode: ImageRefMode,
    md_page_break_placeholder: str,
    start_time: float,
) -> _ProcessedExportResults:
    output_dir = work_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    processed_docs: list[ProcessedDocsItem] = []
    num_succeeded = 0
    num_partially_succeeded = 0
    num_failed = 0

    def _record_status(exportable_document: ExportableDocument) -> None:
        nonlocal num_succeeded, num_partially_succeeded, num_failed
        if exportable_document.status == ConversionStatus.SUCCESS:
            num_succeeded += 1
        elif exportable_document.status == ConversionStatus.PARTIAL_SUCCESS:
            num_partially_succeeded += 1
        else:
            num_failed += 1

    if isinstance(task.target, PresignedUrlTarget):
        if s3_presigned_config is None:
            raise ValueError(
                "PresignedUrlTarget requires s3_presigned_config in orchestrator config"
            )

        presigned_documents: list[DocumentArtifactItem] = []
        with S3PresignedTargetProcessor(s3_presigned_config, task) as target_processor:
            for idx, exportable_document in enumerate(exportable_documents):
                final_document, processed_doc, artifact_item = _process_remote_document(
                    task=task,
                    exportable_document=exportable_document,
                    response_index=idx,
                    total_docs=total_docs,
                    output_dir=output_dir,
                    callback_invoker=callback_invoker,
                    debug_error_details=debug_error_details,
                    callback_mode=callback_mode,
                    upload_document=lambda _source: (
                        _upload_document_as_presigned_artifact(
                            task=task,
                            exportable_document=exportable_document,
                            response_index=idx,
                            output_dir=output_dir,
                            target_processor=target_processor,
                            export_json=export_json,
                            export_html=export_html,
                            export_md=export_md,
                            export_txt=export_txt,
                            export_doctags=export_doctags,
                            image_export_mode=image_export_mode,
                            md_page_break_placeholder=md_page_break_placeholder,
                        )
                    ),
                    build_failure_result=lambda failed_document, source: (
                        target_processor.build_document_artifact_item(
                            source=source,
                            filename=failed_document.file.name,
                            status=failed_document.status,
                            errors=failed_document.errors,
                            timings=failed_document.timings,
                        )
                    ),
                )
                processed_docs.append(processed_doc)
                presigned_documents.append(artifact_item)
                _record_status(final_document)

        if not presigned_documents and not processed_docs:
            raise RuntimeError("No documents were generated by Docling.")

        task_result: ResultType = PresignedArtifactResult(documents=presigned_documents)
    elif isinstance(task.target, (S3Target, LocalPathTarget, GoogleDriveTarget)):
        # Every user-owned storage target is handled identically through the
        # connector factory: the per-source artifact path is computed here and
        # the target processor applies its own prefix/root (S3 key_prefix, local
        # directory, Drive folder, …) when writing. Adding a new storage
        # connector therefore needs no change in this module.
        with get_target_processor(task.target) as target_processor:
            for idx, exportable_document in enumerate(exportable_documents):
                final_document, processed_doc, _ = _process_remote_document(
                    task=task,
                    exportable_document=exportable_document,
                    response_index=idx,
                    total_docs=total_docs,
                    output_dir=output_dir,
                    callback_invoker=callback_invoker,
                    debug_error_details=debug_error_details,
                    callback_mode=callback_mode,
                    upload_document=lambda _source: _upload_document_to_storage_target(
                        task=task,
                        exportable_document=exportable_document,
                        response_index=idx,
                        output_dir=output_dir,
                        target_processor=target_processor,
                        export_json=export_json,
                        export_html=export_html,
                        export_md=export_md,
                        export_txt=export_txt,
                        export_doctags=export_doctags,
                        image_export_mode=image_export_mode,
                        md_page_break_placeholder=md_page_break_placeholder,
                    ),
                    build_failure_result=lambda _failed_document, _source: None,
                )
                processed_docs.append(processed_doc)
                _record_status(final_document)

        if not processed_docs:
            raise RuntimeError("No documents were generated by Docling.")

        task_result = RemoteTargetResult()
    else:
        raise TypeError(f"Unsupported remote target type: {type(task.target)!r}")

    processing_time = time.monotonic() - start_time
    _log.info(f"Processed {len(processed_docs)} docs in {processing_time:.2f} seconds.")
    _maybe_emit_update_processed(
        callback_invoker=callback_invoker,
        callbacks=task.callbacks,
        task_id=task.task_id,
        processed_docs=processed_docs,
        num_succeeded=num_succeeded,
        num_partially_succeeded=num_partially_succeeded,
        num_failed=num_failed,
        callback_mode=callback_mode,
    )

    return _ProcessedExportResults(
        task_result=DoclingTaskResult(
            result=task_result,
            processing_time=processing_time,
            num_succeeded=num_succeeded,
            num_partially_succeeded=num_partially_succeeded,
            num_failed=num_failed,
            num_converted=len(processed_docs),
        ),
        processed_docs=processed_docs,
    )


def _process_exportable_results_internal(
    task: Task,
    exportable_documents: Iterable[ExportableDocument],
    work_dir: Path,
    s3_presigned_config: S3PresignedConfig | None = None,
    callback_invoker: Optional["CallbackInvoker"] = None,
    debug_error_details: bool = False,
    expected_doc_count: Optional[int] = None,
    start_time: Optional[float] = None,
    callback_mode: CallbackMode = CallbackMode.FULL,
) -> _ProcessedExportResults:
    conversion_options = task.convert_options
    if conversion_options is None:
        raise RuntimeError(
            "process_exportable_results called without task.convert_options"
        )

    start_time = start_time if start_time is not None else time.monotonic()
    total_docs = (
        expected_doc_count if expected_doc_count is not None else len(task.sources)
    )
    _maybe_emit_set_num_docs(
        callback_invoker=callback_invoker,
        callbacks=task.callbacks,
        task_id=task.task_id,
        total_docs=total_docs,
        callback_mode=callback_mode,
    )

    export_json = OutputFormat.JSON in conversion_options.to_formats
    export_html = OutputFormat.HTML in conversion_options.to_formats
    export_md = OutputFormat.MARKDOWN in conversion_options.to_formats
    export_txt = OutputFormat.TEXT in conversion_options.to_formats
    export_doctags = OutputFormat.DOCTAGS in conversion_options.to_formats

    if isinstance(
        task.target,
        (PresignedUrlTarget, S3Target, LocalPathTarget, GoogleDriveTarget),
    ):
        return _process_remote_exportable_results(
            task=task,
            exportable_documents=exportable_documents,
            work_dir=work_dir,
            s3_presigned_config=s3_presigned_config,
            callback_invoker=callback_invoker,
            debug_error_details=debug_error_details,
            total_docs=total_docs,
            callback_mode=callback_mode,
            export_json=export_json,
            export_html=export_html,
            export_md=export_md,
            export_txt=export_txt,
            export_doctags=export_doctags,
            image_export_mode=conversion_options.image_export_mode,
            md_page_break_placeholder=conversion_options.md_page_break_placeholder,
            start_time=start_time,
        )

    finalized_documents = list(exportable_documents)
    if len(finalized_documents) == 0:
        raise RuntimeError("No documents were generated by Docling.")

    task_result: ResultType
    processed_docs: list[ProcessedDocsItem] = []
    for idx, exportable_document in enumerate(finalized_documents):
        processed_docs.append(
            _build_processed_docs_item(
                exportable_document,
                debug_error_details=debug_error_details,
            )
        )
        _maybe_emit_document_completed(
            callback_invoker=callback_invoker,
            callbacks=task.callbacks,
            task_id=task.task_id,
            exportable_document=exportable_document,
            total_processed=idx + 1,
            total_docs=total_docs,
            callback_mode=callback_mode,
            debug_error_details=debug_error_details,
        )

    if len(finalized_documents) == 1 and isinstance(task.target, InBodyTarget):
        exportable_document = finalized_documents[0]

        content = _export_document_as_content(
            exportable_document,
            export_json=export_json,
            export_html=export_html,
            export_md=export_md,
            export_txt=export_txt,
            export_doctags=export_doctags,
            image_mode=conversion_options.image_export_mode,
            md_page_break_placeholder=conversion_options.md_page_break_placeholder,
        )
        task_result = DocumentResultItem(
            document=content,
            status=exportable_document.status,
            errors=exportable_document.errors,
            timings=exportable_document.timings,
            confidence=exportable_document.confidence,
        )
    else:
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        _export_documents_as_files(
            exportable_documents=finalized_documents,
            output_dir=output_dir,
            export_json=export_json,
            export_html=export_html,
            export_md=export_md,
            export_txt=export_txt,
            export_doctags=export_doctags,
            image_export_mode=conversion_options.image_export_mode,
            md_page_break_placeholder=conversion_options.md_page_break_placeholder,
        )

        files = list(output_dir.iterdir())
        if len(files) == 0:
            raise RuntimeError("No documents were exported.")

        file_path = work_dir / "converted_docs.zip"
        shutil.make_archive(
            base_name=str(file_path.with_suffix("")),
            format="zip",
            root_dir=output_dir,
        )

        if isinstance(task.target, PutTarget):
            _upload_to_put_target(str(task.target.url), file_path)
            task_result = RemoteTargetResult()
        else:
            task_result = ZipArchiveResult(content=file_path.read_bytes())

    processing_time = time.monotonic() - start_time
    _log.info(
        f"Processed {len(finalized_documents)} docs in {processing_time:.2f} seconds."
    )
    num_succeeded, num_partially_succeeded, num_failed = _count_document_statuses(
        finalized_documents
    )
    _maybe_emit_update_processed(
        callback_invoker=callback_invoker,
        callbacks=task.callbacks,
        task_id=task.task_id,
        processed_docs=processed_docs,
        num_succeeded=num_succeeded,
        num_partially_succeeded=num_partially_succeeded,
        num_failed=num_failed,
        callback_mode=callback_mode,
    )

    return _ProcessedExportResults(
        task_result=DoclingTaskResult(
            result=task_result,
            processing_time=processing_time,
            num_succeeded=num_succeeded,
            num_partially_succeeded=num_partially_succeeded,
            num_failed=num_failed,
            num_converted=len(finalized_documents),
        ),
        processed_docs=processed_docs,
    )


def process_exportable_results(
    task: Task,
    exportable_documents: Iterable[ExportableDocument],
    work_dir: Path,
    s3_presigned_config: S3PresignedConfig | None = None,
    callback_invoker: Optional["CallbackInvoker"] = None,
    debug_error_details: bool = False,
    expected_doc_count: Optional[int] = None,
    start_time: Optional[float] = None,
    callback_mode: CallbackMode = CallbackMode.FULL,
) -> DoclingTaskResult:
    processed = _process_exportable_results_internal(
        task=task,
        exportable_documents=exportable_documents,
        work_dir=work_dir,
        s3_presigned_config=s3_presigned_config,
        callback_invoker=callback_invoker,
        debug_error_details=debug_error_details,
        expected_doc_count=expected_doc_count,
        start_time=start_time,
        callback_mode=callback_mode,
    )

    return processed.task_result
