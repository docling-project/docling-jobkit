import logging
import os
import shutil
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import httpx

from docling.datamodel.base_models import DocumentStream, InputFormat, OutputFormat
from docling.datamodel.document import ConversionStatus
from docling.datamodel.service.callbacks import (
    DocumentCompletedItem,
    FailedDocsItem,
    ProgressDocumentCompleted,
    ProgressSetNumDocs,
    ProgressUpdateProcessed,
    SucceededDocsItem,
)
from docling.datamodel.service.sources import FileSource, HttpSource, S3Coordinates
from docling.datamodel.service.targets import (
    InBodyTarget,
    PresignedUrlTarget,
    PutTarget,
    S3Target,
)
from docling_core.types.doc import ImageRefMode

from docling_jobkit.config.target_config import S3PresignedConfig
from docling_jobkit.connectors.artifact_paths import build_task_scoped_artifact_path
from docling_jobkit.connectors.s3_presigned_target_processor import (
    S3PresignedTargetProcessor,
)
from docling_jobkit.connectors.s3_target_processor import S3TargetProcessor
from docling_jobkit.datamodel.exportable_document import ExportableDocument
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
from docling_jobkit.datamodel.task import Task
from docling_jobkit.public_errors import render_public_error_list

if TYPE_CHECKING:
    from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker

_log = logging.getLogger(__name__)


@dataclass
class _ExportedArtifactFile:
    path: Path
    target_filename: str
    mime_type: str


def _is_exportable_status(status: ConversionStatus) -> bool:
    return status in (ConversionStatus.SUCCESS, ConversionStatus.PARTIAL_SUCCESS)


def _count_document_statuses(
    exportable_documents: list[ExportableDocument],
) -> tuple[int, int, int]:
    num_succeeded = sum(
        1 for doc in exportable_documents if doc.status == ConversionStatus.SUCCESS
    )
    num_partial_success = sum(
        1
        for doc in exportable_documents
        if doc.status == ConversionStatus.PARTIAL_SUCCESS
    )
    num_failed = len(exportable_documents) - num_succeeded - num_partial_success
    return num_succeeded, num_partial_success, num_failed


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
        markdown = exportable_document.document.export_to_markdown()
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
    raise RuntimeError(
        f"Failed to upload zip to target url after {max_retries} attempts."
    ) from last_exc


def _task_source_to_uri(task: Task, source_index: int, fallback_filename: str) -> str:
    if source_index >= len(task.sources):
        return fallback_filename

    source = task.sources[source_index]
    if isinstance(source, HttpSource):
        return str(source.url)
    if isinstance(source, FileSource):
        return source.filename
    if isinstance(source, S3Coordinates):
        key_prefix = source.key_prefix.lstrip("/")
        if key_prefix:
            return f"s3://{source.bucket}/{key_prefix}"
        return f"s3://{source.bucket}"
    if isinstance(source, DocumentStream):
        return source.name
    return fallback_filename


def _materialize_document_exports(
    exportable_document: ExportableDocument,
    output_dir: Path,
    *,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    image_export_mode: ImageRefMode,
    md_page_break_placeholder: str,
    bundle_resources: bool,
) -> list[_ExportedArtifactFile]:
    if not (
        _is_exportable_status(exportable_document.status)
        and exportable_document.document is not None
    ):
        return []

    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir = Path("artifacts")
    generated: list[_ExportedArtifactFile] = []
    doc_filename = exportable_document.file.stem

    if export_json:
        fname = output_dir / f"{doc_filename}.json"
        _log.info(f"writing JSON output to {fname}")
        exportable_document.document.save_as_json(
            filename=fname,
            image_mode=image_export_mode,
            artifacts_dir=artifacts_dir,
        )
        generated.append(
            _ExportedArtifactFile(
                path=fname,
                target_filename=fname.name,
                mime_type="application/json",
            )
        )

    if export_html:
        fname = output_dir / f"{doc_filename}.html"
        _log.info(f"writing HTML output to {fname}")
        exportable_document.document.save_as_html(
            filename=fname,
            image_mode=image_export_mode,
            artifacts_dir=artifacts_dir,
        )
        generated.append(
            _ExportedArtifactFile(
                path=fname,
                target_filename=fname.name,
                mime_type="text/html",
            )
        )

    if export_txt:
        fname = output_dir / f"{doc_filename}.txt"
        _log.info(f"writing TXT output to {fname}")
        exportable_document.document.save_as_markdown(
            filename=fname,
            strict_text=True,
            image_mode=ImageRefMode.PLACEHOLDER,
        )
        generated.append(
            _ExportedArtifactFile(
                path=fname,
                target_filename=fname.name,
                mime_type="text/plain",
            )
        )

    if export_md:
        fname = output_dir / f"{doc_filename}.md"
        _log.info(f"writing Markdown output to {fname}")
        exportable_document.document.save_as_markdown(
            filename=fname,
            artifacts_dir=artifacts_dir,
            image_mode=image_export_mode,
            page_break_placeholder=md_page_break_placeholder or None,
        )
        generated.append(
            _ExportedArtifactFile(
                path=fname,
                target_filename=fname.name,
                mime_type="text/markdown",
            )
        )

    if export_doctags:
        fname = output_dir / f"{doc_filename}.doctags"
        _log.info(f"writing Doc Tags output to {fname}")
        exportable_document.document.save_as_doctags(filename=fname)
        generated.append(
            _ExportedArtifactFile(
                path=fname,
                target_filename=fname.name,
                mime_type="text/plain",
            )
        )

    artifacts_path = output_dir / artifacts_dir
    if bundle_resources and artifacts_path.exists() and any(artifacts_path.iterdir()):
        bundle_path = output_dir / f"{doc_filename}_bundle.zip"
        shutil.make_archive(
            base_name=str(bundle_path.with_suffix("")),
            format="zip",
            root_dir=artifacts_path,
        )
        generated.append(
            _ExportedArtifactFile(
                path=bundle_path,
                target_filename=bundle_path.name,
                mime_type="application/zip",
            )
        )

    return generated


def _upload_documents_as_presigned_artifacts(
    *,
    task: Task,
    exportable_documents: list[ExportableDocument],
    output_dir: Path,
    target_processor: S3PresignedTargetProcessor,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    image_export_mode: ImageRefMode,
    md_page_break_placeholder: str,
) -> list[DocumentArtifactItem]:
    documents: list[DocumentArtifactItem] = []

    for source_index, exportable_document in enumerate(exportable_documents):
        source_uri = _task_source_to_uri(
            task,
            source_index,
            fallback_filename=exportable_document.file.name,
        )
        document_dir = output_dir / f"{source_index:06d}"
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
            target_processor.upload_file(
                filename=artifact.path,
                target_filename=artifact.target_filename,
                content_type=artifact.mime_type,
                source_index=source_index,
                source_uri=source_uri,
            )

        documents.append(
            target_processor.build_document_artifact_item(
                source_index=source_index,
                source_uri=source_uri,
                filename=exportable_document.file.name,
                status=exportable_document.status,
                errors=exportable_document.errors,
                timings=exportable_document.timings,
            )
        )

    return documents


def _upload_documents_to_s3_target(
    *,
    task: Task,
    exportable_documents: list[ExportableDocument],
    output_dir: Path,
    target_processor: S3TargetProcessor,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    image_export_mode: ImageRefMode,
    md_page_break_placeholder: str,
) -> None:
    for source_index, exportable_document in enumerate(exportable_documents):
        source_uri = _task_source_to_uri(
            task,
            source_index,
            fallback_filename=exportable_document.file.name,
        )
        document_dir = output_dir / f"{source_index:06d}"
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
            target_filename = build_task_scoped_artifact_path(
                task.task_id,
                source_index,
                source_uri,
                artifact.target_filename,
            )
            target_processor.upload_file(
                filename=artifact.path,
                target_filename=target_filename,
                content_type=artifact.mime_type,
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
) -> DoclingTaskResult:
    conversion_options = task.convert_options
    if conversion_options is None:
        raise RuntimeError(
            "process_exportable_results called without task.convert_options"
        )

    # Let's start by processing the documents
    start_time = start_time if start_time is not None else time.monotonic()

    # 1. Send ProgressSetNumDocs at start
    total_docs = (
        expected_doc_count if expected_doc_count is not None else len(task.sources)
    )
    if callback_invoker and task.callbacks and total_docs:
        callback_invoker.invoke_callbacks_async(
            callbacks=task.callbacks,
            task_id=task.task_id,
            progress=ProgressSetNumDocs(num_docs=total_docs),
        )

    # 2. Process documents and send ProgressDocumentCompleted for each
    # IMPORTANT: conv_results is a lazy iterator from convert_documents()
    # The actual conversion happens as we iterate through it
    documents_list = []
    docs_succeeded: list[SucceededDocsItem] = []
    docs_failed: list[FailedDocsItem] = []

    for idx, exportable_document in enumerate(exportable_documents):
        documents_list.append(exportable_document)

        # Track for final summary
        if _is_exportable_status(exportable_document.status):
            docs_succeeded.append(
                SucceededDocsItem(source=str(exportable_document.file))
            )
        else:
            docs_failed.append(
                FailedDocsItem(
                    source=str(exportable_document.file),
                    error=(
                        render_public_error_list(
                            exportable_document.errors,
                            debug_enabled=debug_error_details,
                        )
                        or "Unknown error"
                    ),
                )
            )

        # Send per-document callback (non-blocking)
        if callback_invoker and task.callbacks:
            document_info = _build_document_completed_item(
                exportable_document,
                error=render_public_error_list(
                    exportable_document.errors,
                    debug_enabled=debug_error_details,
                ),
            )

            callback_invoker.invoke_callbacks_async(
                callbacks=task.callbacks,
                task_id=task.task_id,
                progress=ProgressDocumentCompleted(
                    document=document_info,
                    total_processed=idx + 1,
                    total_docs=total_docs,
                ),
            )

    exportable_documents = documents_list
    # Task-level wall clock elapsed time across the whole request.
    processing_time = time.monotonic() - start_time

    _log.info(
        f"Processed {len(exportable_documents)} docs in {processing_time:.2f} seconds."
    )

    if len(exportable_documents) == 0:
        raise RuntimeError("No documents were generated by Docling.")

    num_succeeded, num_partial_success, num_failed = _count_document_statuses(
        exportable_documents
    )

    # 3. Send ProgressUpdateProcessed at end with final summary
    if callback_invoker and task.callbacks:
        callback_invoker.invoke_callbacks_async(
            callbacks=task.callbacks,
            task_id=task.task_id,
            progress=ProgressUpdateProcessed(
                num_processed=len(docs_succeeded) + len(docs_failed),
                num_succeeded=num_succeeded,
                num_partial_success=num_partial_success,
                num_failed=num_failed,
                docs_succeeded=docs_succeeded,
                docs_failed=docs_failed,
            ),
        )

    # We have some results, let's prepare the response
    task_result: ResultType

    # Booleans to know what to export
    export_json = OutputFormat.JSON in conversion_options.to_formats
    export_html = OutputFormat.HTML in conversion_options.to_formats
    export_md = OutputFormat.MARKDOWN in conversion_options.to_formats
    export_txt = OutputFormat.TEXT in conversion_options.to_formats
    export_doctags = OutputFormat.DOCTAGS in conversion_options.to_formats

    # Only 1 document was processed, and we are not returning it as a file
    if len(exportable_documents) == 1 and isinstance(task.target, InBodyTarget):
        exportable_document = exportable_documents[0]

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
        )

    elif isinstance(task.target, PresignedUrlTarget):
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        if s3_presigned_config is None:
            raise ValueError(
                "PresignedUrlTarget requires s3_presigned_config in orchestrator config"
            )

        with S3PresignedTargetProcessor(s3_presigned_config, task) as target_processor:
            documents = _upload_documents_as_presigned_artifacts(
                task=task,
                exportable_documents=exportable_documents,
                output_dir=output_dir,
                target_processor=target_processor,
                export_json=export_json,
                export_html=export_html,
                export_md=export_md,
                export_txt=export_txt,
                export_doctags=export_doctags,
                image_export_mode=conversion_options.image_export_mode,
                md_page_break_placeholder=conversion_options.md_page_break_placeholder,
            )

        task_result = PresignedArtifactResult(documents=documents)

    elif isinstance(task.target, S3Target):
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        with S3TargetProcessor(task.target) as target_processor:
            _upload_documents_to_s3_target(
                task=task,
                exportable_documents=exportable_documents,
                output_dir=output_dir,
                target_processor=target_processor,
                export_json=export_json,
                export_html=export_html,
                export_md=export_md,
                export_txt=export_txt,
                export_doctags=export_doctags,
                image_export_mode=conversion_options.image_export_mode,
                md_page_break_placeholder=conversion_options.md_page_break_placeholder,
            )
        task_result = RemoteTargetResult()

    # Multiple documents were processed, or we are forced returning as a file
    else:
        # Temporary directory to store the outputs
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Export the documents
        _export_documents_as_files(
            exportable_documents=exportable_documents,
            output_dir=output_dir,
            export_json=export_json,
            export_html=export_html,
            export_md=export_md,
            export_txt=export_txt,
            export_doctags=export_doctags,
            image_export_mode=conversion_options.image_export_mode,
            md_page_break_placeholder=conversion_options.md_page_break_placeholder,
        )

        files = os.listdir(output_dir)
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

    return DoclingTaskResult(
        result=task_result,
        processing_time=processing_time,
        num_succeeded=num_succeeded,
        num_partial_success=num_partial_success,
        num_failed=num_failed,
        num_converted=len(exportable_documents),
    )
