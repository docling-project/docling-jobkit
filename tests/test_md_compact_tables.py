"""``md_compact_tables`` must reach the markdown serializer on every export path.

The option is declared on ``ConvertDocumentsOptions`` in Docling, but the markdown body
is produced here: a request can therefore be accepted, validated and silently ignored.
The padding it controls is not cosmetic — ``tabulate`` pads every cell of a column out
to the widest cell in it, so one long cell multiplies by every row of the table.
"""

from pathlib import Path, PurePath

from docling.datamodel.base_models import ConversionStatus
from docling_core.types.doc import ImageRefMode
from docling_core.types.doc.document import DoclingDocument, TableCell, TableData

from docling_jobkit.convert.export import _materialize_document_exports
from docling_jobkit.convert.results import _export_document_as_content
from docling_jobkit.datamodel.exportable_document import ExportableDocument

_WIDE_CELL = "x" * 200


def _cell(text: str, row: int, col: int, header: bool = False) -> TableCell:
    return TableCell(
        text=text,
        row_span=1,
        col_span=1,
        start_row_offset_idx=row,
        end_row_offset_idx=row + 1,
        start_col_offset_idx=col,
        end_col_offset_idx=col + 1,
        column_header=header,
    )


def _document_with_wide_table() -> ExportableDocument:
    doc = DoclingDocument(name="wide-table")
    doc.add_table(
        data=TableData(
            num_rows=3,
            num_cols=2,
            table_cells=[
                _cell("id", 0, 0, header=True),
                _cell("note", 0, 1, header=True),
                _cell("1", 1, 0),
                _cell(_WIDE_CELL, 1, 1),
                _cell("2", 2, 0),
                _cell("y", 2, 1),
            ],
        )
    )
    return ExportableDocument(
        file=PurePath("wide-table.pdf"),
        status=ConversionStatus.SUCCESS,
        document=doc,
    )


def _export_content(md_compact_tables: bool) -> str:
    response = _export_document_as_content(
        _document_with_wide_table(),
        export_json=False,
        export_html=False,
        export_md=True,
        export_txt=False,
        export_doctags=False,
        export_doclang=False,
        image_mode=ImageRefMode.PLACEHOLDER,
        md_page_break_placeholder="",
        md_compact_tables=md_compact_tables,
    )
    assert response.md_content is not None
    return response.md_content


def test_md_content_is_padded_by_default():
    md = _export_content(md_compact_tables=False)
    assert f"| y{' ' * 100}" in md


def test_md_content_honors_compact_tables():
    padded = _export_content(md_compact_tables=False)
    compact = _export_content(md_compact_tables=True)

    assert "| y |" in compact
    assert _WIDE_CELL in compact
    assert len(compact) < len(padded)


def test_markdown_file_honors_compact_tables(tmp_path: Path):
    for compact in (False, True):
        output_dir = tmp_path / f"compact-{compact}"
        artifacts = _materialize_document_exports(
            _document_with_wide_table(),
            output_dir,
            export_json=False,
            export_html=False,
            export_md=True,
            export_txt=False,
            export_doctags=False,
            export_doclang=False,
            export_dclx=False,
            image_export_mode=ImageRefMode.PLACEHOLDER,
            md_page_break_placeholder="",
            md_compact_tables=compact,
            bundle_resources=False,
        )
        assert len(artifacts) == 1

    padded = (tmp_path / "compact-False" / "wide-table.md").read_text(encoding="utf-8")
    compacted = (tmp_path / "compact-True" / "wide-table.md").read_text(
        encoding="utf-8"
    )

    assert "| y |" in compacted
    assert _WIDE_CELL in compacted
    assert len(compacted) < len(padded)
