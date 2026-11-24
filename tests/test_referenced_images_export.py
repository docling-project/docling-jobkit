"""Referenced images of an in-body result stay inside the task's work directory."""

from pathlib import Path

import pytest
from PIL import Image

from docling.datamodel.base_models import ConversionStatus, OutputFormat
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.responses import DocumentResultItem
from docling.datamodel.service.targets import InBodyTarget
from docling_core.types.doc import ImageRef, ImageRefMode
from docling_core.types.doc.document import DoclingDocument

from docling_jobkit.convert.results import process_exportable_results
from docling_jobkit.datamodel.exportable_document import ExportableDocument
from docling_jobkit.datamodel.task import Task


def test_in_body_referenced_images_stay_in_work_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    cwd = tmp_path / "cwd"
    work_dir = tmp_path / "work"
    cwd.mkdir()
    work_dir.mkdir()
    monkeypatch.chdir(cwd)

    doc = DoclingDocument(name="with-picture")
    doc.add_picture(image=ImageRef.from_pil(Image.new("RGB", (8, 8)), dpi=72))
    task = Task(
        task_id="task-1",
        target=InBodyTarget(),
        convert_options=ConvertDocumentsOptions(
            to_formats=[OutputFormat.MARKDOWN],
            image_export_mode=ImageRefMode.REFERENCED,
        ),
    )

    result = process_exportable_results(
        task=task,
        exportable_documents=[
            ExportableDocument(
                file=Path("doc.docx"), status=ConversionStatus.SUCCESS, document=doc
            )
        ],
        work_dir=work_dir,
    )

    assert list(cwd.iterdir()) == []
    [image] = (work_dir / "artifacts").iterdir()
    assert isinstance(result.result, DocumentResultItem)
    assert f"![Image]({image.name})" in (result.result.document.md_content or "")
