import logging
import shutil
import tempfile
import time
from collections.abc import Callable, Iterable
from contextlib import ExitStack
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from docling.datamodel.base_models import InputFormat, OutputFormat
from docling.datamodel.document import ConversionStatus
from docling.datamodel.service.callbacks import (
    DocumentCompletedItem,
    ProcessedDocsItem,
    ProgressDocumentCompleted,
    ProgressSetNumDocs,
    ProgressUpdateProcessed,
)
from docling.datamodel.service.chunking import BaseChunkerOptions
from docling.datamodel.service.targets import InBodyTarget
from docling_core.types.doc import ImageRefMode

from docling_jobkit.config.target_config import S3PresignedConfig
from docling_jobkit.connectors.artifact_paths import (
    hash_path_component,
)
from docling_jobkit.connectors.connector_factory import get_target_connector_factory
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.connectors.target_processor_factory import get_target_processor
from docling_jobkit.convert.export import (
    _cleanup_document_output_dir,
    _is_exportable_status,
    _materialize_document_exports,
    _processor_requires_chunks,
    _release_exportable_document_references,
    _upload_materialized_artifacts,
    stream_chunks_for_document,
    write_chunks_jsonl,
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
from docling_jobkit.public_errors import (
    build_public_error_item,
    render_public_error_list,
)

if TYPE_CHECKING:
    from docling_jobkit.convert.chunking import DocumentChunkerManager
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
    export_doclang: bool,
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
        if export_doclang:
            document.doclang_content = new_doc.export_to_doclang()

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
    export_doclang: bool,
    export_dclx: bool,
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
            export_doclang=export_doclang,
            export_dclx=export_dclx,
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
    target_processor: Any,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    export_doclang: bool,
    export_dclx: bool,
    image_export_mode: ImageRefMode,
    md_page_break_placeholder: str,
    chunker_manager: Optional["DocumentChunkerManager"] = None,
    chunking_options: Optional[BaseChunkerOptions] = None,
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
        export_doclang=export_doclang,
        export_dclx=export_dclx,
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

    # 5c: upload {stem}.chunks.jsonl as an additional presigned artifact so the
    # caller receives a presigned URL for it alongside the other format URLs.
    if (
        chunker_manager is not None
        and chunking_options is not None
        and _is_exportable_status(exportable_document.status)
        and exportable_document.document is not None
    ):
        chunks_filename = f"{exportable_document.file.stem}.chunks.jsonl"
        chunks_path = document_dir / chunks_filename
        write_chunks_jsonl(
            exportable_document=exportable_document,
            chunks_path=chunks_path,
            chunker_manager=chunker_manager,
            chunking_options=chunking_options,
        )
        target_processor.upload_artifact_file(
            source=source,
            artifact_type="chunks",
            path=chunks_path,
            target_filename=chunks_filename,
            mime_type="application/jsonl",
        )

    return target_processor.build_document_artifact_item(
        source=source,
        filename=exportable_document.file.name,
        status=exportable_document.status,
        errors=exportable_document.errors,
        timings=exportable_document.timings,
        confidence=exportable_document.confidence,
    )


def _resolve_target_modes(
    target_factory: Any,
    targets: list[Any],
) -> list[str | None]:
    """Result mode of every target, ``None`` for targets the factory doesn't own.

    ``None`` covers the in-process results (``InBodyTarget``, ``ZipTarget``)
    which are handled outside the remote-target pipeline.
    """
    return [
        target_factory.result_mode(t) if target_factory.supports(t) else None
        for t in targets
    ]


def _validate_target_modes(
    targets: list[Any],
    target_modes: list[str | None],
) -> None:
    """Reject target combinations the result pipeline cannot honour.

    ``artifacts`` and ``database`` targets fan out freely and in any mix — each
    one is driven by its own ``result_mode()``.  Every other mode produces a
    single task-level result object (a presigned URL list, a zip archive, an
    inline body) that has no meaning alongside a second target, so combining
    them is refused up front rather than silently dropping targets — which is
    what the previous ``targets[0]``-only routing did.
    """
    if len(targets) < 2:
        return
    fan_out_modes = {"artifacts", "database"}
    offenders = [
        (target, mode)
        for target, mode in zip(targets, target_modes)
        if mode not in fan_out_modes
    ]
    if offenders:
        names = ", ".join(
            f"{type(target).__name__} (mode={mode or 'in-process'})"
            for target, mode in offenders
        )
        raise ValueError(
            f"Target(s) {names} cannot be combined with other targets: only "
            f"storage and database targets support multi-target fan-out."
        )


def _fan_out_document_to_processors(
    *,
    exportable_document: ExportableDocument,
    source: SourceIdentity,
    document_dir: Path,
    processors: list[BaseTargetProcessor],
    doc_id: str,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    export_doclang: bool,
    export_dclx: bool,
    image_export_mode: ImageRefMode,
    md_page_break_placeholder: str,
) -> None:
    """Materialize one document's exports **once** and route them to every target.

    Each processor is driven according to *its own* ``result_mode()`` rather than
    a single mode inferred from ``targets[0]``:

    - ``database`` processors are bracketed by ``begin_document`` /
      ``end_document`` so all formats merge into one row, and receive the bare
      artifact filename (the row, not a path, is the addressable unit).
    - every other (storage) processor receives ``<source_key>/<artifact>``,
      the layout all user-owned storage targets already use.

    Deciding per target is what makes a mixed target list behave identically no
    matter which order the targets were declared in: with a single inferred mode,
    ``[s3, opensearch_doc]`` never opened a document bracket and silently indexed
    nothing, while ``[opensearch_doc, s3]`` wrote S3 objects with a flat layout.

    Exports are materialized once for all targets — the previous code re-ran the
    full export step per processor, paying the CPU and peak-allocation cost of
    every format N times for N targets.
    """
    artifacts = _materialize_document_exports(
        exportable_document,
        document_dir,
        export_json=export_json,
        export_html=export_html,
        export_md=export_md,
        export_txt=export_txt,
        export_doctags=export_doctags,
        export_doclang=export_doclang,
        export_dclx=export_dclx,
        image_export_mode=image_export_mode,
        md_page_break_placeholder=md_page_break_placeholder,
        bundle_resources=True,
    )
    if not artifacts:
        return

    # Resource bundles are zip archives of the other artifacts; database targets
    # store text/JSON formats and cannot decode them.
    db_artifacts = [a for a in artifacts if a.artifact_type != "resource_bundle"]

    def _storage_key(artifact_filename: str) -> str:
        return f"{source.source_key}/{artifact_filename}"

    def _database_key(artifact_filename: str) -> str:
        return artifact_filename

    for processor in processors:
        if processor.result_mode() == "database":
            processor.begin_document(doc_id)
            try:
                _upload_materialized_artifacts(
                    target_processor=processor,
                    artifacts=db_artifacts,
                    target_filename_fn=_database_key,
                )
            except BaseException:
                # Do not flush a half-populated row: the document failed, and
                # end_document() is what commits it.
                processor.abort_document(doc_id)
                raise
            processor.end_document(doc_id)
        else:
            _upload_materialized_artifacts(
                target_processor=processor,
                artifacts=artifacts,
                target_filename_fn=_storage_key,
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
    after_upload: Callable[[ExportableDocument, SourceIdentity], None] | None = None,
) -> tuple[ExportableDocument, ProcessedDocsItem, Any]:
    source = _resolve_source_identity(task, exportable_document, response_index)
    document_dir = output_dir / f"{source.source_index:06d}"
    final_document = exportable_document
    try:
        try:
            upload_result = upload_document(source)
            # Post-upload work (e.g. streaming chunk export) runs while the
            # document references are still alive — the finally block below
            # releases them.  It shares this document's error boundary: a chunk
            # target that is unreachable must fail *this document*, exactly like
            # a failed format upload, instead of aborting the whole task.
            if after_upload is not None:
                after_upload(final_document, source)
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


def _iter_remote_documents(
    *,
    task: Task,
    exportable_documents: Iterable[ExportableDocument],
    processors: list[BaseTargetProcessor],
    upload_document_fn: Callable[[ExportableDocument, int, SourceIdentity], Any],
    output_dir: Path,
    work_dir: Path,
    total_docs: int,
    callback_invoker: Optional["CallbackInvoker"],
    debug_error_details: bool,
    callback_mode: CallbackMode,
    chunks_in_formats: bool,
    chunker_manager: Optional["DocumentChunkerManager"],
    chunking_options: Optional[BaseChunkerOptions],
) -> tuple[list[ProcessedDocsItem], int, int, int]:
    """Shared document-iteration loop for the artifacts and database target modes.

    Iterates *exportable_documents*, calls *upload_document_fn* for each one,
    then runs the streaming chunk protocol when active.  Returns
    ``(processed_docs, num_succeeded, num_partially_succeeded, num_failed)``.
    """
    processed_docs: list[ProcessedDocsItem] = []
    num_succeeded = 0
    num_partially_succeeded = 0
    num_failed = 0

    chunk_active = (
        (chunks_in_formats or any(_processor_requires_chunks(p) for p in processors))
        and chunker_manager is not None
        and chunking_options is not None
    )

    for idx, exportable_document in enumerate(exportable_documents):
        _doc = exportable_document
        _idx = idx

        def _stream_chunks(
            _final_document: ExportableDocument,
            _source_identity: SourceIdentity,
            _ed: ExportableDocument = exportable_document,
        ) -> None:
            # Runs before _process_remote_document releases the document
            # references, so the chunker still sees a live DoclingDocument.
            if not chunk_active:
                return
            _stem = _ed.file.stem
            with tempfile.TemporaryDirectory(dir=work_dir) as _chunk_tmp:
                stream_chunks_for_document(
                    exportable_document=_final_document,
                    filename=str(_ed.file),
                    chunker_manager=chunker_manager,  # type: ignore[arg-type]
                    chunking_options=chunking_options,  # type: ignore[arg-type]
                    processors=processors,
                    chunks_in_formats=chunks_in_formats,
                    temp_dir=Path(_chunk_tmp),
                    chunk_target_key=f"{_source_identity.source_key}/{_stem}.chunks.jsonl",
                )

        final_document, processed_doc, _ = _process_remote_document(
            task=task,
            exportable_document=exportable_document,
            response_index=idx,
            total_docs=total_docs,
            output_dir=output_dir,
            callback_invoker=callback_invoker,
            debug_error_details=debug_error_details,
            callback_mode=callback_mode,
            upload_document=lambda _source: upload_document_fn(_doc, _idx, _source),
            build_failure_result=lambda _failed_document, _source: None,
            after_upload=_stream_chunks,
        )
        processed_docs.append(processed_doc)

        if final_document.status == ConversionStatus.SUCCESS:
            num_succeeded += 1
        elif final_document.status == ConversionStatus.PARTIAL_SUCCESS:
            num_partially_succeeded += 1
        else:
            num_failed += 1

    return processed_docs, num_succeeded, num_partially_succeeded, num_failed


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
    export_doclang: bool,
    export_dclx: bool,
    image_export_mode: ImageRefMode,
    md_page_break_placeholder: str,
    start_time: float,
    allow_external_plugins: bool,
    chunker_manager: Optional["DocumentChunkerManager"] = None,
    chunking_options: Optional[BaseChunkerOptions] = None,
) -> _ProcessedExportResults:
    output_dir = work_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Chunk activation: same two-signal rule as ResultsProcessor.
    chunks_in_formats: bool = "chunks" in (
        [f.value for f in (task.convert_options.to_formats or [])]
        if task.convert_options
        else []
    )

    first_target = task.targets[0] if task.targets else None
    all_targets = task.targets or []
    target_factory = get_target_connector_factory(allow_external_plugins)
    target_modes = _resolve_target_modes(target_factory, all_targets)
    _validate_target_modes(all_targets, target_modes)
    target_mode = target_modes[0] if target_modes else None

    if target_mode == "presigned":
        if s3_presigned_config is None:
            raise ValueError(
                "PresignedUrlTarget requires s3_presigned_config in orchestrator config"
            )

        presigned_documents: list[DocumentArtifactItem] = []
        processed_docs: list[ProcessedDocsItem] = []
        num_succeeded = num_partially_succeeded = num_failed = 0
        # Presigned is always a single target (the presigned config is global),
        # so we keep a single processor here.
        with get_target_processor(
            first_target,  # type: ignore[arg-type]
            allow_external_plugins=allow_external_plugins,
            s3_presigned_config=s3_presigned_config,
            task=task,
        ) as base_target_processor:
            target_processor: Any = base_target_processor
            chunk_active = chunks_in_formats and chunker_manager and chunking_options
            for idx, exportable_document in enumerate(exportable_documents):
                _doc = exportable_document
                _idx = idx

                def _upload_presigned(
                    _source: SourceIdentity,
                    _d: ExportableDocument = _doc,
                    _i: int = _idx,
                ) -> DocumentArtifactItem:
                    return _upload_document_as_presigned_artifact(
                        task=task,
                        exportable_document=_d,
                        response_index=_i,
                        output_dir=output_dir,
                        target_processor=target_processor,
                        export_json=export_json,
                        export_html=export_html,
                        export_md=export_md,
                        export_txt=export_txt,
                        export_doctags=export_doctags,
                        export_doclang=export_doclang,
                        export_dclx=export_dclx,
                        image_export_mode=image_export_mode,
                        md_page_break_placeholder=md_page_break_placeholder,
                        chunker_manager=chunker_manager if chunk_active else None,
                        chunking_options=chunking_options if chunk_active else None,
                    )

                def _build_presigned_failure(
                    failed_document: ExportableDocument, source: SourceIdentity
                ) -> DocumentArtifactItem:
                    return target_processor.build_document_artifact_item(
                        source=source,
                        filename=failed_document.file.name,
                        status=failed_document.status,
                        errors=failed_document.errors,
                        timings=failed_document.timings,
                    )

                final_document, processed_doc, artifact_item = _process_remote_document(
                    task=task,
                    exportable_document=exportable_document,
                    response_index=idx,
                    total_docs=total_docs,
                    output_dir=output_dir,
                    callback_invoker=callback_invoker,
                    debug_error_details=debug_error_details,
                    callback_mode=callback_mode,
                    upload_document=_upload_presigned,
                    build_failure_result=_build_presigned_failure,
                )
                processed_docs.append(processed_doc)
                presigned_documents.append(artifact_item)
                if final_document.status == ConversionStatus.SUCCESS:
                    num_succeeded += 1
                elif final_document.status == ConversionStatus.PARTIAL_SUCCESS:
                    num_partially_succeeded += 1
                else:
                    num_failed += 1

        if not presigned_documents:
            raise RuntimeError("No documents were generated by Docling.")

        task_result: ResultType = PresignedArtifactResult(documents=presigned_documents)
    else:
        # target_mode is "artifacts" or "database".  Both are handled by the same
        # loop: every target is opened simultaneously via ExitStack, and each
        # document is materialized once then routed to each processor according
        # to that processor's own result_mode() (see
        # _fan_out_document_to_processors).  Mixed lists therefore behave the
        # same in either declaration order.
        with ExitStack() as stack:
            processors = [
                stack.enter_context(
                    get_target_processor(
                        t, allow_external_plugins=allow_external_plugins
                    )
                )
                for t in all_targets
            ]

            def _upload_to_all_targets(
                doc: ExportableDocument, idx: int, source: SourceIdentity
            ) -> None:
                _fan_out_document_to_processors(
                    exportable_document=doc,
                    source=source,
                    document_dir=output_dir / f"{source.source_index:06d}",
                    processors=processors,
                    doc_id=doc.document_hash or f"doc_{idx}",
                    export_json=export_json,
                    export_html=export_html,
                    export_md=export_md,
                    export_txt=export_txt,
                    export_doctags=export_doctags,
                    export_doclang=export_doclang,
                    export_dclx=export_dclx,
                    image_export_mode=image_export_mode,
                    md_page_break_placeholder=md_page_break_placeholder,
                )

            processed_docs, num_succeeded, num_partially_succeeded, num_failed = (
                _iter_remote_documents(
                    task=task,
                    exportable_documents=exportable_documents,
                    processors=processors,
                    upload_document_fn=_upload_to_all_targets,
                    output_dir=output_dir,
                    work_dir=work_dir,
                    total_docs=total_docs,
                    callback_invoker=callback_invoker,
                    debug_error_details=debug_error_details,
                    callback_mode=callback_mode,
                    chunks_in_formats=chunks_in_formats,
                    chunker_manager=chunker_manager,
                    chunking_options=chunking_options,
                )
            )

        if not processed_docs:
            raise RuntimeError("No documents were generated by Docling.")

        task_result = RemoteTargetResult()

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
    allow_external_plugins: bool = False,
    chunker_manager: Optional["DocumentChunkerManager"] = None,
    chunking_options: Optional[BaseChunkerOptions] = None,
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
    export_doclang = OutputFormat.DOCLANG in conversion_options.to_formats
    export_dclx = OutputFormat.DCLX in conversion_options.to_formats
    chunks_in_formats: bool = "chunks" in [
        f.value for f in conversion_options.to_formats
    ]

    first_target = task.targets[0] if task.targets else None
    all_targets = task.targets or []
    target_factory = get_target_connector_factory(allow_external_plugins)
    target_modes = _resolve_target_modes(target_factory, all_targets)
    # Refuse unsupported combinations here too, so the failure is reported before
    # any conversion output is written rather than after the first target.
    _validate_target_modes(all_targets, target_modes)
    target_mode = target_modes[0] if target_modes else None
    if target_mode in {"artifacts", "presigned", "database"}:
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
            export_doclang=export_doclang,
            export_dclx=export_dclx,
            image_export_mode=conversion_options.image_export_mode,
            md_page_break_placeholder=conversion_options.md_page_break_placeholder,
            start_time=start_time,
            allow_external_plugins=allow_external_plugins,
            chunker_manager=chunker_manager,
            chunking_options=chunking_options,
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

    if len(finalized_documents) == 1 and isinstance(first_target, InBodyTarget):
        # 5a: "chunks" in to_formats with InBodyTarget — silently skip chunk
        # output; inline responses are not suited to returning chunk lists.
        exportable_document = finalized_documents[0]

        content = _export_document_as_content(
            exportable_document,
            export_json=export_json,
            export_html=export_html,
            export_md=export_md,
            export_txt=export_txt,
            export_doctags=export_doctags,
            export_doclang=export_doclang,
            image_mode=conversion_options.image_export_mode,
            md_page_break_placeholder=conversion_options.md_page_break_placeholder,
        )
        if chunks_in_formats:
            _log.debug(
                "Chunk export requested but target is InBodyTarget — skipped for task %s",
                task.task_id,
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
            export_doclang=export_doclang,
            export_dclx=export_dclx,
            image_export_mode=conversion_options.image_export_mode,
            md_page_break_placeholder=conversion_options.md_page_break_placeholder,
        )

        # 5b: write {stem}.chunks.jsonl into output_dir before archiving so
        # it ends up inside the zip alongside the other format files.
        if chunks_in_formats and chunker_manager and chunking_options:
            for exportable_document in finalized_documents:
                if not _is_exportable_status(exportable_document.status):
                    continue
                if exportable_document.document is None:
                    continue
                write_chunks_jsonl(
                    exportable_document=exportable_document,
                    chunks_path=(
                        output_dir / f"{exportable_document.file.stem}.chunks.jsonl"
                    ),
                    chunker_manager=chunker_manager,
                    chunking_options=chunking_options,
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

        if target_mode == "archive":
            with get_target_processor(
                first_target,  # type: ignore[arg-type]
                allow_external_plugins=allow_external_plugins,
            ) as target_processor:
                target_processor.upload_archive(file_path)
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
    allow_external_plugins: bool = False,
    chunker_manager: Optional["DocumentChunkerManager"] = None,
    chunking_options: Optional[BaseChunkerOptions] = None,
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
        allow_external_plugins=allow_external_plugins,
        chunker_manager=chunker_manager,
        chunking_options=chunking_options,
    )

    return processed.task_result
