"""Client-requested DoclingDocument version down-projection."""

from pathlib import Path

import pytest

from docling.datamodel.base_models import ConversionStatus, InputFormat
from docling.datamodel.document import ConversionResult, InputDocument, _DummyBackend
from docling_core.types.doc.document import DoclingDocument

from docling_jobkit.datamodel.exportable_document import (
    ExportableDocument,
    project_document,
)


def _native_doc() -> DoclingDocument:
    return DoclingDocument(name="sample")


def test_factory_downprojects_serialized_document_to_floor(tmp_path: Path) -> None:
    input_path = tmp_path / "input.pdf"
    input_path.write_bytes(b"%PDF-1.4")
    result = ConversionResult(
        input=InputDocument(
            path_or_stream=input_path,
            format=InputFormat.PDF,
            backend=_DummyBackend,
        ),
        status=ConversionStatus.SUCCESS,
        document=_native_doc(),
    )

    exported = ExportableDocument.from_conversion_result(result, target_version="1.5.0")

    assert exported.document is not None
    assert exported.document.export_to_dict()["version"] == "1.5.0"


def test_noop_when_target_at_or_above_native() -> None:
    doc = _native_doc()
    assert project_document(doc, doc.version) is doc
    assert project_document(doc, "2.0.0") is doc
    assert project_document(doc, None) is doc


def test_hard_fail_below_floor() -> None:
    doc = _native_doc()
    with pytest.raises(ValueError):
        project_document(doc, "1.4.0")


def test_slice_assembly_downprojects_document() -> None:
    pytest.importorskip("ray")
    from docling_jobkit.orchestrators.ray.serve_deployment import (
        _assemble_slice_results,
    )

    assembled = _assemble_slice_results(
        [
            ExportableDocument(
                file=Path("input.pdf"),
                status=ConversionStatus.SUCCESS,
                document=_native_doc(),
                slice_index=0,
            )
        ],
        target_version="1.5.0",
    )

    assert assembled.document is not None
    assert assembled.document.version == "1.5.0"
