import logging
import os
import shutil
import time
from collections.abc import Iterable
from pathlib import Path

import httpx

from docling.datamodel.base_models import OutputFormat
from docling.datamodel.document import ConversionResult, ConversionStatus
from docling_core.types.doc import ImageRefMode

from docling_jobkit.datamodel.result import (
    DoclingTaskResult,
    ExportDocumentResponse,
    ExportResult,
    RemoteTargetResult,
    ResultType,
    ZipArchiveResult,
)
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_targets import InBodyTarget, PutTarget

_log = logging.getLogger(__name__)


def _export_document_as_content(
    conv_res: ConversionResult,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    image_mode: ImageRefMode,
    md_page_break_placeholder: str,
) -> ExportDocumentResponse:
    document = ExportDocumentResponse(filename=conv_res.input.file.name)

    if conv_res.status == ConversionStatus.SUCCESS:
        new_doc = conv_res.document._make_copy_with_refmode(
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


def _export_documents_as_files(
    conv_results: Iterable[ConversionResult],
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

    artifacts_dir = Path("artifacts/")  # will be relative to the fname

    for conv_res in conv_results:
        if conv_res.status == ConversionStatus.SUCCESS:
            success_count += 1
            doc_filename = conv_res.input.file.stem

            # Export JSON format:
            if export_json:
                fname = output_dir / f"{doc_filename}.json"
                _log.info(f"writing JSON output to {fname}")
                conv_res.document.save_as_json(
                    filename=fname,
                    image_mode=image_export_mode,
                    artifacts_dir=artifacts_dir,
                )

            # Export HTML format:
            if export_html:
                fname = output_dir / f"{doc_filename}.html"
                _log.info(f"writing HTML output to {fname}")
                conv_res.document.save_as_html(
                    filename=fname,
                    image_mode=image_export_mode,
                    artifacts_dir=artifacts_dir,
                )

            # Export Text format:
            if export_txt:
                fname = output_dir / f"{doc_filename}.txt"
                _log.info(f"writing TXT output to {fname}")
                conv_res.document.save_as_markdown(
                    filename=fname,
                    strict_text=True,
                    image_mode=ImageRefMode.PLACEHOLDER,
                )

            # Export Markdown format:
            if export_md:
                fname = output_dir / f"{doc_filename}.md"
                _log.info(f"writing Markdown output to {fname}")
                conv_res.document.save_as_markdown(
                    filename=fname,
                    artifacts_dir=artifacts_dir,
                    image_mode=image_export_mode,
                    page_break_placeholder=md_page_break_placeholder or None,
                )

            # Export Document Tags format:
            if export_doctags:
                fname = output_dir / f"{doc_filename}.doctags"
                _log.info(f"writing Doc Tags output to {fname}")
                conv_res.document.save_as_doctags(filename=fname)

        else:
            _log.warning(f"Document {conv_res.input.file} failed to convert.")
            failure_count += 1

    conv_result = (
        ConversionStatus.SUCCESS if failure_count == 0 else ConversionStatus.FAILURE
    )

    _log.info(
        f"Processed {success_count + failure_count} docs, "
        f"of which {failure_count} failed"
    )
    return success_count, failure_count, conv_result


def process_export_results(
    task: Task,
    conv_results: Iterable[ConversionResult],
    work_dir: Path,
) -> DoclingTaskResult:
    conversion_options = task.convert_options
    if conversion_options is None:
        raise RuntimeError("process_export_results called without task.convert_options")

    # Let's start by processing the documents
    start_time = time.monotonic()

    # Convert the iterator to a list to count the number of results and get timings
    # As it's an iterator (lazy evaluation), it will also start the conversion
    conv_results = list(conv_results)

    processing_time = time.monotonic() - start_time

    _log.info(f"Processed {len(conv_results)} docs in {processing_time:.2f} seconds.")

    if len(conv_results) == 0:
        raise RuntimeError("No documents were generated by Docling.")

    # We have some results, let's prepare the response
    task_result: ResultType
    num_succeeded = 0
    num_failed = 0

    # Booleans to know what to export
    export_json = OutputFormat.JSON in conversion_options.to_formats
    export_html = OutputFormat.HTML in conversion_options.to_formats
    export_md = OutputFormat.MARKDOWN in conversion_options.to_formats
    export_txt = OutputFormat.TEXT in conversion_options.to_formats
    export_doctags = OutputFormat.DOCTAGS in conversion_options.to_formats

    # Only 1 document was processed, and we are not returning it as a file
    if len(conv_results) == 1 and isinstance(task.target, InBodyTarget):
        conv_res = conv_results[0]

        content = _export_document_as_content(
            conv_res,
            export_json=export_json,
            export_html=export_html,
            export_md=export_md,
            export_txt=export_txt,
            export_doctags=export_doctags,
            image_mode=conversion_options.image_export_mode,
            md_page_break_placeholder=conversion_options.md_page_break_placeholder,
        )
        task_result = ExportResult(
            content=content,
            status=conv_res.status,
            # processing_time=processing_time,
            timings=conv_res.timings,
            errors=conv_res.errors,
        )

        num_succeeded = 1 if conv_res.status == ConversionStatus.SUCCESS else 0
        num_failed = 1 if conv_res.status != ConversionStatus.SUCCESS else 0

    # Multiple documents were processed, or we are forced returning as a file
    else:
        # Temporary directory to store the outputs
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Export the documents
        num_succeeded, num_failed, conv_result = _export_documents_as_files(
            conv_results=conv_results,
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
            try:
                with file_path.open("rb") as file_data:
                    r = httpx.put(str(task.target.url), files={"file": file_data})
                    r.raise_for_status()
                task_result = RemoteTargetResult()
            except Exception as exc:
                _log.error("An error occour while uploading zip to s3", exc_info=exc)
                raise RuntimeError(
                    "An error occour while uploading zip to the target url."
                )

        else:
            task_result = ZipArchiveResult(content=file_path.read_bytes())

    return DoclingTaskResult(
        result=task_result,
        processing_time=processing_time,
        num_succeeded=num_succeeded,
        num_failed=num_failed,
        num_converted=len(conv_results),
    )
