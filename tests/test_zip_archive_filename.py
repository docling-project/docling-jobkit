from pathlib import Path
from unittest.mock import Mock

from docling.datamodel.base_models import ConversionStatus, OutputFormat
from docling.datamodel.service.chunking import HybridChunkerOptions
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.responses import ZipArchiveResult
from docling.datamodel.service.targets import ZipTarget
from docling.datamodel.service.tasks import TaskType
from docling_core.types.doc.document import DoclingDocument

from docling_jobkit.convert.chunking import (
    DocumentChunkerManager,
    process_chunkable_results,
)
from docling_jobkit.convert.results import process_exportable_results
from docling_jobkit.datamodel.exportable_document import ExportableDocument
from docling_jobkit.datamodel.task import Task


def _converted(name: str) -> ExportableDocument:
    return ExportableDocument(
        file=Path(name),
        status=ConversionStatus.SUCCESS,
        document=DoclingDocument(name=Path(name).stem),
    )


def _failed(name: str) -> ExportableDocument:
    return ExportableDocument(file=Path(name), status=ConversionStatus.FAILURE)


def _convert_zip(tmp_path: Path, docs: list[ExportableDocument]) -> ZipArchiveResult:
    task = Task(
        task_id="task-1",
        target=ZipTarget(),
        convert_options=ConvertDocumentsOptions(to_formats=[OutputFormat.MARKDOWN]),
    )
    task_result = process_exportable_results(
        task=task, exportable_documents=docs, work_dir=tmp_path
    )
    assert isinstance(task_result.result, ZipArchiveResult)
    return task_result.result


def _chunk_zip(tmp_path: Path, docs: list[ExportableDocument]) -> ZipArchiveResult:
    task = Task(
        task_id="task-1",
        task_type=TaskType.CHUNK,
        chunking_options=HybridChunkerOptions(),
        target=ZipTarget(),
        convert_options=ConvertDocumentsOptions(),
    )
    chunker_manager = Mock(spec=DocumentChunkerManager)
    chunker_manager.chunk_document.return_value = []
    task_result = process_chunkable_results(
        task=task,
        exportable_documents=docs,
        work_dir=tmp_path,
        chunker_manager=chunker_manager,
    )
    assert isinstance(task_result.result, ZipArchiveResult)
    return task_result.result


def test_convert_zip_named_after_single_document(tmp_path: Path):
    result = _convert_zip(tmp_path, [_converted("report.pdf")])
    assert result.filename == "report.zip"


def test_convert_zip_unnamed_for_multiple_documents(tmp_path: Path):
    result = _convert_zip(tmp_path, [_converted("a.pdf"), _converted("b.pdf")])
    assert result.filename is None


def test_convert_zip_unnamed_when_one_of_two_documents_fails(tmp_path: Path):
    result = _convert_zip(tmp_path, [_converted("a.pdf"), _failed("b.pdf")])
    assert result.filename is None


def test_chunk_zip_named_after_single_document(tmp_path: Path):
    result = _chunk_zip(tmp_path, [_converted("report.pdf")])
    assert result.filename == "report.zip"


def test_chunk_zip_unnamed_for_multiple_documents(tmp_path: Path):
    result = _chunk_zip(tmp_path, [_converted("a.pdf"), _failed("b.pdf")])
    assert result.filename is None
