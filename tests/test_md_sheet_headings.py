"""Worksheet names must reach the Markdown output on every export path.

Spreadsheet backends put each worksheet in a ``GroupLabel.SHEET`` group, but plain
``export_to_markdown`` drops the group name, so a multi-sheet workbook came out as
unlabeled tables (docling-serve#760).
"""

from pathlib import Path, PurePath

from docling.datamodel.base_models import ConversionStatus
from docling_core.types.doc import GroupLabel, ImageRefMode
from docling_core.types.doc.document import DoclingDocument, TableCell, TableData

from docling_jobkit.convert.export import (
    _export_markdown,
    _materialize_document_exports,
)
from docling_jobkit.convert.results import _export_document_as_content
from docling_jobkit.datamodel.exportable_document import ExportableDocument


def _table(rows: list[list[str]]) -> TableData:
    return TableData(
        num_rows=len(rows),
        num_cols=len(rows[0]),
        table_cells=[
            TableCell(
                text=text,
                row_span=1,
                col_span=1,
                start_row_offset_idx=r,
                end_row_offset_idx=r + 1,
                start_col_offset_idx=c,
                end_col_offset_idx=c + 1,
                column_header=r == 0,
            )
            for r, row in enumerate(rows)
            for c, text in enumerate(row)
        ],
    )


def _workbook() -> DoclingDocument:
    doc = DoclingDocument(name="workbook")
    for sheet, amount in (("Revenue", "10"), ("Costs", "4")):
        group = doc.add_group(label=GroupLabel.SHEET, name=sheet)
        doc.add_table(data=_table([["q", "amount"], ["Q1", amount]]), parent=group)
    return doc


def _plain_document() -> DoclingDocument:
    doc = DoclingDocument(name="plain")
    section = doc.add_group(label=GroupLabel.SECTION, name="not a sheet")
    doc.add_text(label="text", text="hello_world", parent=section)
    doc.add_table(data=_table([["a", "b"], ["1", "2"]]))
    return doc


def _exportable(doc: DoclingDocument) -> ExportableDocument:
    return ExportableDocument(
        file=PurePath("workbook.xlsx"),
        status=ConversionStatus.SUCCESS,
        document=doc,
    )


def _assert_sheet_headings(md: str) -> None:
    revenue, costs = md.index("## Revenue"), md.index("## Costs")
    assert revenue < md.index("10") < costs < md.index("4")


def test_md_content_has_sheet_headings():
    response = _export_document_as_content(
        _exportable(_workbook()),
        export_json=False,
        export_html=False,
        export_md=True,
        export_txt=False,
        export_doctags=False,
        export_doclang=False,
        image_mode=ImageRefMode.PLACEHOLDER,
        md_page_break_placeholder="",
        md_compact_tables=False,
        artifacts_dir=Path("artifacts"),
    )
    assert response.md_content is not None
    _assert_sheet_headings(response.md_content)


def test_markdown_file_has_sheet_headings(tmp_path: Path):
    artifacts = _materialize_document_exports(
        _exportable(_workbook()),
        tmp_path,
        export_json=False,
        export_html=False,
        export_md=True,
        export_txt=False,
        export_doctags=False,
        export_doclang=False,
        export_dclx=False,
        image_export_mode=ImageRefMode.PLACEHOLDER,
        md_page_break_placeholder="",
        md_compact_tables=False,
        bundle_resources=False,
    )
    assert len(artifacts) == 1
    _assert_sheet_headings((tmp_path / "workbook.md").read_text(encoding="utf-8"))


def test_sheet_export_matches_plain_export_apart_from_headings():
    # Guards that the sheet-aware path keeps every other export_to_markdown option.
    doc = _workbook()
    for compact in (False, True):
        kwargs = dict(
            image_mode=ImageRefMode.PLACEHOLDER,
            page_break_placeholder="<!-- page break -->",
            compact_tables=compact,
        )
        with_headings = _export_markdown(doc, **kwargs)
        without_headings = "\n\n".join(
            part for part in with_headings.split("\n\n") if not part.startswith("## ")
        )
        assert without_headings == doc.export_to_markdown(**kwargs)


def test_documents_without_sheets_are_unchanged():
    doc = _plain_document()
    for compact in (False, True):
        assert _export_markdown(doc, compact_tables=compact) == doc.export_to_markdown(
            compact_tables=compact
        )
