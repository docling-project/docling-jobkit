"""Client-requested DoclingDocument version down-projection (project_document)."""

import pytest

from docling_core.types.doc.document import DoclingDocument

from docling_jobkit.datamodel.exportable_document import project_document


def _native_doc() -> DoclingDocument:
    return DoclingDocument(name="sample")


def test_downprojects_to_floor() -> None:
    doc = _native_doc()
    projected = project_document(doc, "1.5.0")
    assert projected.version == "1.5.0"


def test_noop_when_target_at_or_above_native() -> None:
    doc = _native_doc()
    # Same version and a falsy target both return the original doc untouched.
    assert project_document(doc, doc.version) is doc
    assert project_document(doc, None) is doc


def test_hard_fail_below_floor() -> None:
    doc = _native_doc()
    with pytest.raises(ValueError):
        project_document(doc, "1.4.0")
