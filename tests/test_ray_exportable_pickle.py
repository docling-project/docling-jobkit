"""Ray serialization guard for ExportableDocument.

Ray moves converter results through its object store via ``cloudpickle``; a
DoclingParse-backed ``ExportableDocument`` must survive that round-trip. This is
the one piece of unique coverage kept from the old (mostly redundant/outdated)
``test_ray_fanout.py`` suite.
"""

import os
from pathlib import Path

import pytest

pytest.importorskip("ray")
if os.getenv("CI"):
    pytest.skip("Skipping Ray tests in CI", allow_module_level=True)

import ray

from docling.backend.docling_parse_backend import DoclingParseDocumentBackend
from docling.datamodel.base_models import ConversionStatus, InputFormat
from docling.datamodel.document import ConversionResult, InputDocument
from docling_core.types.doc.document import DoclingDocument

from docling_jobkit.datamodel.exportable_document import ExportableDocument

TEST_PDF = Path(__file__).parent / "2206.01062v1-pg4.pdf"


def test_exportable_document_is_ray_pickleable_for_docling_parse() -> None:
    input_doc = InputDocument(
        path_or_stream=TEST_PDF,
        format=InputFormat.PDF,
        backend=DoclingParseDocumentBackend,
    )
    exportable_document = ExportableDocument.from_conversion_result(
        ConversionResult(
            input=input_doc,
            status=ConversionStatus.SUCCESS,
            document=DoclingDocument(name="pickle-safe"),
        )
    )

    payload = ray.cloudpickle.dumps(exportable_document)

    assert isinstance(payload, bytes)
