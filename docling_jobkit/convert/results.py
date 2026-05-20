import json
import logging
import os
import re
import shutil
import time
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import httpx
from urllib.parse import unquote, unquote_plus

from docling.datamodel.base_models import OutputFormat
from docling.datamodel.document import ConversionResult, ConversionStatus
from docling_core.types.doc import ImageRefMode

from docling_jobkit.datamodel.callback import (
    DocumentCompletedItem,
    FailedDocsItem,
    ProgressDocumentCompleted,
    ProgressSetNumDocs,
    ProgressUpdateProcessed,
    SucceededDocsItem,
)
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

if TYPE_CHECKING:
    from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker

_log = logging.getLogger(__name__)
PERSIST_ROOT = Path('/work/output')

_CJK_CHAR_RE = r"\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff"
_CJK_PUNCT_RE = r"，。！？；：、】【（）《》“”‘’、"
_CJK_CHAR_CLASS = f"[{_CJK_CHAR_RE}]"
_CJK_OR_PUNCT_CLASS = f"[{_CJK_CHAR_RE}{_CJK_PUNCT_RE}]"


def _merge_broken_cjk_lines(text: str) -> str:
    lines = text.splitlines()
    if len(lines) <= 1:
        return text

    merged: list[str] = []
    for raw_line in lines:
        line = raw_line.strip()
        if not merged:
            merged.append(line)
            continue
        if not line:
            merged.append(line)
            continue

        prev = merged[-1]
        if not prev:
            merged.append(line)
            continue

        is_new_block = bool(
            re.match(r"^(#{1,6}\s|[-*+]\s|\d+[.)、]\s*|[（(]?[一二三四五六七八九十]+[）)]\s*)", line)
        )
        prev_ends_sentence = bool(re.search(r"[。！？!?；;：:]$", prev))
        line_is_short_cjk = bool(re.fullmatch(rf"{_CJK_OR_PUNCT_CLASS}{{1,6}}", line))
        prev_ends_with_cjk = bool(re.search(rf"{_CJK_OR_PUNCT_CLASS}$", prev))
        line_starts_with_cjk = bool(re.match(rf"^{_CJK_OR_PUNCT_CLASS}", line))

        if not is_new_block and not prev_ends_sentence and (
            line_is_short_cjk or (prev_ends_with_cjk and line_starts_with_cjk)
        ):
            merged[-1] = prev + line
        else:
            merged.append(line)

    return "\n".join(merged)


def _normalize_cjk_spacing(text: str) -> str:
    text = _merge_broken_cjk_lines(text)
    text = re.sub(rf"(?<={_CJK_CHAR_CLASS})[ \\t]+(?={_CJK_CHAR_CLASS})", "", text)
    text = re.sub(rf"(?<={_CJK_OR_PUNCT_CLASS})[ \\t]+(?=[{_CJK_PUNCT_RE}])", "", text)
    text = re.sub(rf"(?<=[{_CJK_PUNCT_RE}])[ \\t]+(?={_CJK_OR_PUNCT_CLASS})", "", text)
    text = re.sub(rf"([（《“‘【])\s+(?={_CJK_OR_PUNCT_CLASS})", r"\1", text)
    text = re.sub(rf"(?<={_CJK_OR_PUNCT_CLASS})\s+([）》”’】])", r"\1", text)
    return text


def _normalize_export_json_payload(payload: object):
    if isinstance(payload, dict):
        return {k: _normalize_export_json_payload(v) for k, v in payload.items()}
    if isinstance(payload, list):
        return [_normalize_export_json_payload(v) for v in payload]
    if isinstance(payload, str):
        return _normalize_cjk_spacing(payload)
    return payload


def _persist_export_artifacts(temp_file: Path, doc_stem: str) -> tuple[Path, Path | None]:
    PERSIST_ROOT.mkdir(parents=True, exist_ok=True)
    persisted_file = PERSIST_ROOT / temp_file.name
    shutil.copy2(temp_file, persisted_file)

    temp_artifacts_dir = temp_file.parent / 'artifacts'
    # 🔧 解码中文文件名
    decoded_stem = doc_stem
    for decoder in [unquote, unquote_plus]:
        try:
            candidate = decoder(doc_stem)
            if '%' not in candidate and candidate != doc_stem:
                decoded_stem = candidate
                break
        except Exception:
            pass
    persisted_artifacts_dir = PERSIST_ROOT / f'{decoded_stem}_artifacts'
    if temp_artifacts_dir.exists():
        if persisted_artifacts_dir.exists():
            shutil.rmtree(persisted_artifacts_dir)
        shutil.copytree(temp_artifacts_dir, persisted_artifacts_dir)
        _log.warning('DIAG persisted artifacts temp=%s persisted=%s', temp_artifacts_dir, persisted_artifacts_dir)
        return persisted_file, persisted_artifacts_dir

    _log.warning('DIAG no temp artifacts dir for %s (expected %s)', temp_file, temp_artifacts_dir)
    return persisted_file, None


def _export_document_as_content(
    conv_res: ConversionResult,
    export_json: bool,
    export_html: bool,
    export_md: bool,
    export_txt: bool,
    export_doctags: bool,
    image_mode: ImageRefMode,
    md_page_break_placeholder: str,
    work_dir: Path | None = None,
) -> ExportDocumentResponse:
    document = ExportDocumentResponse(filename=conv_res.input.file.name)

    # 🔧 关键修复：如果 work_dir 是 None，强制使用 scratch
    if work_dir is None:
        try:
            from docling_serve.storage import get_scratch
            work_dir = get_scratch()
            import logging
            _log = logging.getLogger(__name__)
            _log.warning("DIAG fixed work_dir from None to scratch: %s", work_dir)
        except Exception as e:
            import logging
            _log = logging.getLogger(__name__)
            _log.warning("DIAG failed to get scratch, work_dir still None: %s", e)

    if conv_res.status == ConversionStatus.SUCCESS:
        new_doc = conv_res.document._make_copy_with_refmode(
            Path(), image_mode, page_no=None
        )

        output_dir = None
        artifacts_dir = Path('artifacts/')
        if work_dir is not None and image_mode == ImageRefMode.REFERENCED:
            output_dir = work_dir / 'output'
            output_dir.mkdir(parents=True, exist_ok=True)

        if export_json:
            if output_dir is not None:
                fname = output_dir / f'{conv_res.input.file.stem}.json'
                new_doc.save_as_json(
                    filename=fname,
                    image_mode=image_mode,
                    artifacts_dir=artifacts_dir,
                )
                persisted_file, persisted_artifacts = _persist_export_artifacts(fname, conv_res.input.file.stem)
                document.output_dir = str(PERSIST_ROOT)
                import logging; logging.getLogger(__name__).warning('DIAG json export output_dir=%s filename=%s persisted_file=%s artifacts=%s', document.output_dir, document.filename, persisted_file, persisted_artifacts)
                # 重新读取 JSON 内容（图片引用已经被替换）
                json_text = persisted_file.read_text(encoding='utf-8')
                json_payload = _normalize_export_json_payload(json.loads(json_text))
                persisted_file.write_text(
                    json.dumps(json_payload, ensure_ascii=False, indent=2),
                    encoding='utf-8',
                )
                document.json_content = type(new_doc).model_validate(json_payload)
            else:
                document.json_content = new_doc
        if export_html:
            if output_dir is not None:
                fname = output_dir / f'{conv_res.input.file.stem}.html'
                new_doc.save_as_html(
                    filename=fname,
                    image_mode=image_mode,
                    artifacts_dir=artifacts_dir,
                )
                persisted_file, _ = _persist_export_artifacts(fname, conv_res.input.file.stem)
                document.output_dir = str(PERSIST_ROOT)
                normalized_html = _normalize_cjk_spacing(
                    persisted_file.read_text(encoding='utf-8')
                )
                persisted_file.write_text(normalized_html, encoding='utf-8')
                document.html_content = normalized_html
            else:
                document.html_content = _normalize_cjk_spacing(
                    new_doc.export_to_html(image_mode=image_mode)
                )
        if export_txt:
            document.text_content = _normalize_cjk_spacing(
                new_doc.export_to_markdown(
                    strict_text=True,
                    image_mode=image_mode,
                )
            )
        if export_md:
            if output_dir is not None:
                fname = output_dir / f'{conv_res.input.file.stem}.md'
                new_doc.save_as_markdown(
                    filename=fname,
                    artifacts_dir=artifacts_dir,
                    image_mode=image_mode,
                    page_break_placeholder=md_page_break_placeholder or None,
                )
                persisted_file, _ = _persist_export_artifacts(fname, conv_res.input.file.stem)
                document.output_dir = str(PERSIST_ROOT)
                _log.warning('DIAG results output_dir=%s filename=%s persisted_file=%s', document.output_dir, document.filename, persisted_file)
                normalized_md = _normalize_cjk_spacing(
                    persisted_file.read_text(encoding='utf-8')
                )
                persisted_file.write_text(normalized_md, encoding='utf-8')
                document.md_content = normalized_md
            else:
                document.md_content = _normalize_cjk_spacing(
                    new_doc.export_to_markdown(
                        image_mode=image_mode,
                        page_break_placeholder=md_page_break_placeholder or None,
                    )
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

    artifacts_dir = Path('artifacts/')

    for conv_res in conv_results:
        if conv_res.status == ConversionStatus.SUCCESS:
            success_count += 1
            doc_filename = conv_res.input.file.stem

            if export_json:
                fname = output_dir / f'{doc_filename}.json'
                _log.info(f'writing JSON output to {fname}')
                conv_res.document.save_as_json(
                    filename=fname,
                    image_mode=image_export_mode,
                    artifacts_dir=artifacts_dir,
                )
                json_payload = json.loads(fname.read_text(encoding='utf-8'))
                json_payload = _normalize_export_json_payload(json_payload)
                fname.write_text(
                    json.dumps(json_payload, ensure_ascii=False, indent=2),
                    encoding='utf-8',
                )

            if export_html:
                fname = output_dir / f'{doc_filename}.html'
                _log.info(f'writing HTML output to {fname}')
                conv_res.document.save_as_html(
                    filename=fname,
                    image_mode=image_export_mode,
                    artifacts_dir=artifacts_dir,
                )
                fname.write_text(
                    _normalize_cjk_spacing(fname.read_text(encoding='utf-8')),
                    encoding='utf-8',
                )

            if export_txt:
                fname = output_dir / f'{doc_filename}.txt'
                _log.info(f'writing TXT output to {fname}')
                conv_res.document.save_as_markdown(
                    filename=fname,
                    strict_text=True,
                    image_mode=ImageRefMode.PLACEHOLDER,
                )
                fname.write_text(
                    _normalize_cjk_spacing(fname.read_text(encoding='utf-8')),
                    encoding='utf-8',
                )

            if export_md:
                fname = output_dir / f'{doc_filename}.md'
                _log.info(f'writing Markdown output to {fname}')
                conv_res.document.save_as_markdown(
                    filename=fname,
                    artifacts_dir=artifacts_dir,
                    image_mode=image_export_mode,
                    page_break_placeholder=md_page_break_placeholder or None,
                )
                fname.write_text(
                    _normalize_cjk_spacing(fname.read_text(encoding='utf-8')),
                    encoding='utf-8',
                )

            if export_doctags:
                fname = output_dir / f'{doc_filename}.doctags'
                _log.info(f'writing Doc Tags output to {fname}')
                conv_res.document.save_as_doctags(filename=fname)

        else:
            _log.warning(f'Document {conv_res.input.file} failed to convert.')
            failure_count += 1

    conv_result = (
        ConversionStatus.SUCCESS if failure_count == 0 else ConversionStatus.FAILURE
    )

    _log.info(
        f'Processed {success_count + failure_count} docs, '
        f'of which {failure_count} failed'
    )
    return success_count, failure_count, conv_result


def process_export_results(
    task: Task,
    conv_results: Iterable[ConversionResult],
    work_dir: Path,
    callback_invoker: Optional['CallbackInvoker'] = None,
) -> DoclingTaskResult:
    conversion_options = task.convert_options
    if conversion_options is None:
        raise RuntimeError('process_export_results called without task.convert_options')

    start_time = time.monotonic()
    total_docs = len(task.sources)
    if callback_invoker and task.callbacks and total_docs:
        callback_invoker.invoke_callbacks_async(
            callbacks=task.callbacks,
            task_id=task.task_id,
            progress=ProgressSetNumDocs(num_docs=total_docs),
        )

    conv_results_list = []
    docs_succeeded: list[SucceededDocsItem] = []
    docs_failed: list[FailedDocsItem] = []

    for idx, conv_res in enumerate(conv_results):
        conv_results_list.append(conv_res)
        if conv_res.status == ConversionStatus.SUCCESS:
            docs_succeeded.append(SucceededDocsItem(source=str(conv_res.input.file)))
        else:
            docs_failed.append(
                FailedDocsItem(
                    source=str(conv_res.input.file),
                    error=str(conv_res.errors) if conv_res.errors else 'Unknown error',
                )
            )
        if callback_invoker and task.callbacks:
            document_info = DocumentCompletedItem(
                source=str(conv_res.input.file),
                status=conv_res.status,
                num_pages=(len(conv_res.document.pages) if conv_res.document else None),
                processing_time=(
                    sum(sum(item.times) for item in conv_res.timings.values())
                    if conv_res.timings else None
                ),
                doc_hash=conv_res.input.document_hash,
                error=str(conv_res.errors) if conv_res.errors else None,
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

    conv_results = conv_results_list
    processing_time = time.monotonic() - start_time

    if len(conv_results) == 0:
        raise RuntimeError('No documents were generated by Docling.')

    if callback_invoker and task.callbacks:
        callback_invoker.invoke_callbacks_async(
            callbacks=task.callbacks,
            task_id=task.task_id,
            progress=ProgressUpdateProcessed(
                num_processed=len(docs_succeeded) + len(docs_failed),
                num_succeeded=len(docs_succeeded),
                num_failed=len(docs_failed),
                docs_succeeded=docs_succeeded,
                docs_failed=docs_failed,
            ),
        )

    task_result: ResultType
    num_succeeded = 0
    num_failed = 0

    export_json = OutputFormat.JSON in conversion_options.to_formats
    export_html = OutputFormat.HTML in conversion_options.to_formats
    export_md = OutputFormat.MARKDOWN in conversion_options.to_formats
    export_txt = OutputFormat.TEXT in conversion_options.to_formats
    export_doctags = OutputFormat.DOCTAGS in conversion_options.to_formats

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
            work_dir=work_dir,
        )
        task_result = ExportResult(
            content=content,
            status=conv_res.status,
            timings=conv_res.timings,
            errors=conv_res.errors,
        )
        num_succeeded = 1 if conv_res.status == ConversionStatus.SUCCESS else 0
        num_failed = 1 if conv_res.status != ConversionStatus.SUCCESS else 0
    else:
        output_dir = work_dir / 'output'
        output_dir.mkdir(parents=True, exist_ok=True)
        num_succeeded, num_failed, _conv_result = _export_documents_as_files(
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
            raise RuntimeError('No documents were exported.')
        file_path = work_dir / 'converted_docs.zip'
        shutil.make_archive(
            base_name=str(file_path.with_suffix('')),
            format='zip',
            root_dir=output_dir,
        )
        if isinstance(task.target, PutTarget):
            try:
                with file_path.open('rb') as file_data:
                    r = httpx.put(str(task.target.url), files={'file': file_data})
                    r.raise_for_status()
                task_result = RemoteTargetResult()
            except Exception as exc:
                _log.error('An error occour while uploading zip to s3', exc_info=exc)
                raise RuntimeError('An error occour while uploading zip to the target url.')
        else:
            task_result = ZipArchiveResult(content=file_path.read_bytes())

    return DoclingTaskResult(
        result=task_result,
        processing_time=processing_time,
        num_succeeded=num_succeeded,
        num_failed=num_failed,
        num_converted=len(conv_results),
    )
