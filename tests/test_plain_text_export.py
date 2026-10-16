from pathlib import Path, PurePath

from docling.datamodel.base_models import ConversionStatus
from docling_core.types.doc import DocItemLabel, DoclingDocument, ImageRefMode

from docling_jobkit.convert.export import _materialize_document_exports
from docling_jobkit.convert.results import _export_document_as_content
from docling_jobkit.datamodel.exportable_document import ExportableDocument


def _document_with_markdown_elements() -> ExportableDocument:
    document = DoclingDocument(name="plain-text")
    document.add_picture()
    document.add_heading(text="Section heading", level=2)
    document.add_text(label=DocItemLabel.TEXT, text="Body text")
    return ExportableDocument(
        file=PurePath("plain-text.pdf"),
        status=ConversionStatus.SUCCESS,
        document=document,
    )


def test_text_content_uses_plain_text_serializer():
    response = _export_document_as_content(
        _document_with_markdown_elements(),
        export_json=False,
        export_html=False,
        export_md=False,
        export_txt=True,
        export_doctags=False,
        export_doclang=False,
        image_mode=ImageRefMode.PLACEHOLDER,
        md_page_break_placeholder="",
        md_compact_tables=False,
    )

    assert response.text_content == "Section heading\n\nBody text"


def test_text_artifact_uses_plain_text_serializer(tmp_path: Path):
    artifacts = _materialize_document_exports(
        _document_with_markdown_elements(),
        tmp_path,
        export_json=False,
        export_html=False,
        export_md=False,
        export_txt=True,
        export_doctags=False,
        export_doclang=False,
        export_dclx=False,
        image_export_mode=ImageRefMode.PLACEHOLDER,
        md_page_break_placeholder="",
        md_compact_tables=False,
        bundle_resources=False,
    )

    assert len(artifacts) == 1
    assert (tmp_path / "plain-text.txt").read_text(encoding="utf-8") == (
        "Section heading\n\nBody text"
    )
