"""Client-requested DoclingDocument version down-projection."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from docling.datamodel.base_models import ConversionStatus, InputFormat
from docling.datamodel.document import ConversionResult, InputDocument, _DummyBackend
from docling.datamodel.service.targets import InBodyTarget
from docling_core.types.doc import ImageRefMode
from docling_core.types.doc.document import DoclingDocument
from docling_core.types.doc.labels import DocItemLabel

from docling_jobkit.convert.export import _materialize_document_exports
from docling_jobkit.datamodel.exportable_document import (
    ExportableDocument,
    project_document_dict,
)
from docling_jobkit.datamodel.task import Task


def _native_doc() -> DoclingDocument:
    doc = DoclingDocument(name="sample")
    doc.add_text(label=DocItemLabel.TEXT, text="hello")
    return doc


def _conversion_result(tmp_path: Path) -> ConversionResult:
    input_path = tmp_path / "input.pdf"
    input_path.write_bytes(b"%PDF-1.4")
    return ConversionResult(
        input=InputDocument(
            path_or_stream=input_path,
            format=InputFormat.PDF,
            backend=_DummyBackend,
        ),
        status=ConversionStatus.SUCCESS,
        document=_native_doc(),
    )


def test_projected_dict_keeps_target_version_and_drops_newer_fields() -> None:
    projected = project_document_dict(_native_doc(), "1.5.0")

    assert projected is not None
    assert projected["version"] == "1.5.0"
    # `meta` (schema 1.8) and the 1.10 field lists must not be re-added as defaults.
    serialized = json.dumps(projected)
    assert '"meta"' not in serialized
    assert "field_regions" not in projected
    assert projected["texts"][0]["text"] == "hello"


def test_no_projection_when_target_missing_or_not_older() -> None:
    doc = _native_doc()
    assert project_document_dict(doc, None) is None
    assert project_document_dict(doc, doc.version) is None
    assert project_document_dict(doc, "2.0.0") is None


def test_hard_fail_below_floor() -> None:
    with pytest.raises(ValueError):
        project_document_dict(_native_doc(), "1.4.0")


def test_task_rejects_malformed_target_version() -> None:
    task = Task(task_id="t", target=InBodyTarget(), target_document_version="1.8.0")
    assert task.target_document_version == "1.8.0"
    with pytest.raises(ValidationError):
        Task(task_id="t", target=InBodyTarget(), target_document_version="1.8")


def test_exportable_document_stays_native_and_carries_target(tmp_path: Path) -> None:
    exported = ExportableDocument.from_conversion_result(
        _conversion_result(tmp_path), target_document_version="1.5.0"
    )

    assert exported.target_document_version == "1.5.0"
    assert exported.document is not None
    assert exported.document.version == DoclingDocument(name="x").version


def test_json_file_export_is_projected(tmp_path: Path) -> None:
    exported = ExportableDocument.from_conversion_result(
        _conversion_result(tmp_path), target_document_version="1.5.0"
    )

    _materialize_document_exports(
        exported,
        tmp_path / "out",
        export_json=True,
        export_html=False,
        export_md=False,
        export_txt=False,
        export_doctags=False,
        export_doclang=False,
        export_dclx=False,
        image_export_mode=ImageRefMode.PLACEHOLDER,
        md_page_break_placeholder="",
        md_compact_tables=False,
        bundle_resources=False,
    )

    written = json.loads((tmp_path / "out" / "input.json").read_text())
    assert written["version"] == "1.5.0"
    assert "field_regions" not in written


def test_ray_task_metadata_round_trips_target_version() -> None:
    pytest.importorskip("ray")
    from docling_jobkit.orchestrators.ray.models import RedisTaskMetadata

    metadata = RedisTaskMetadata.from_redis_mapping(
        {
            "task_id": "t",
            "tenant_id": "default",
            "status": "pending",
            "task_type": "convert",
            "created_at": "2026-01-01T00:00:00+00:00",
            "last_update_at": "2026-01-01T00:00:00+00:00",
            "target_document_version": "1.8.0",
        }
    )

    assert metadata is not None
    assert metadata.to_task().target_document_version == "1.8.0"


def test_ray_source_chunk_path_carries_target_version(tmp_path: Path) -> None:
    pytest.importorskip("ray")
    from docling_jobkit.connectors.source_processor import DocumentChunk
    from docling_jobkit.orchestrators.ray.serve_deployment import (
        _to_exportable_documents_from_chunk,
    )

    task = Task(task_id="t", target=InBodyTarget(), target_document_version="1.5.0")
    chunk: DocumentChunk = DocumentChunk(source=None, refs=[], chunk_index=0)

    exportable = _to_exportable_documents_from_chunk(
        task, chunk, [_conversion_result(tmp_path)]
    )

    assert exportable[0].target_document_version == "1.5.0"
