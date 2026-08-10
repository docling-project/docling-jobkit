from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.snowflake.models import (
    SnowflakeCoordinates,
    TaskSnowflakeSource,
)
from docling_jobkit.connectors.snowflake.source_processor import (
    SnowflakeFileIdentifier,
    SnowflakeSourceProcessor,
)
from docling_jobkit.convert.materialization import SourceLimitExceededError


@pytest.fixture
def coords() -> SnowflakeCoordinates:
    return SnowflakeCoordinates(
        account="xy12345",
        user="me",
        password="p",
        warehouse="WH",
        database="DB",
        db_schema="SCH",
        stage="STG",
    )


def test_check_dependencies_present():
    SnowflakeSourceProcessor.check_dependencies()


def test_get_config_types():
    assert SnowflakeSourceProcessor.get_config_types() == (TaskSnowflakeSource,)


def test_list_document_ids_respects_max_num_elements(coords):
    capped = coords.model_copy(update={"max_num_elements": 2})
    processor = SnowflakeSourceProcessor(capped)
    processor._connection = MagicMock()

    rows = [
        {"name": "STG/a.pdf", "size": 10, "last_modified": "t1"},
        {"name": "STG/b.pdf", "size": 20, "last_modified": "t2"},
        {"name": "STG/c.pdf", "size": 30, "last_modified": "t3"},
    ]
    with patch(
        "docling_jobkit.connectors.snowflake.source_processor.list_stage_files",
        return_value=iter(rows),
    ):
        ids = list(processor._list_document_ids())

    assert [i.relative_path for i in ids] == ["a.pdf", "b.pdf"]
    assert ids[0].size == 10
    assert ids[0].last_modified == "t1"


def test_count_documents_clips_to_max_num_elements(coords):
    capped = coords.model_copy(update={"max_num_elements": 2})
    processor = SnowflakeSourceProcessor(capped)
    processor._connection = MagicMock()

    rows = [{"name": f"STG/{i}.pdf"} for i in range(5)]
    with patch(
        "docling_jobkit.connectors.snowflake.source_processor.list_stage_files",
        return_value=iter(rows),
    ):
        assert processor._count_documents() == 2


def test_make_document_ref_uses_basename_and_snowflake_uri(coords):
    processor = SnowflakeSourceProcessor(coords)
    identifier = SnowflakeFileIdentifier(relative_path="sub/report.pdf", size=100)

    ref = processor._make_document_ref(identifier, source_index=0)

    assert ref.filename == "report.pdf"
    assert ref.source_uri == "snowflake://DB/SCH/STG/sub/report.pdf"


def test_fetch_document_by_id_rejects_oversized_before_download(coords):
    processor = SnowflakeSourceProcessor(coords)
    identifier = SnowflakeFileIdentifier(relative_path="big.pdf", size=10_000)

    with pytest.raises(SourceLimitExceededError, match="max_file_size=8000"):
        processor._fetch_document_by_id(identifier, max_file_size=8000)


def test_fetch_document_by_id_wraps_downloaded_bytes(coords):
    processor = SnowflakeSourceProcessor(coords)
    processor._connection = MagicMock()
    identifier = SnowflakeFileIdentifier(relative_path="a.pdf", size=10)

    with patch(
        "docling_jobkit.connectors.snowflake.source_processor.download_stage_file",
        return_value=(b"pdf-bytes", "a.pdf"),
    ):
        stream = processor._fetch_document_by_id(identifier)

    assert isinstance(stream, DocumentStream)
    assert stream.name == "a.pdf"
    assert stream.stream.read() == b"pdf-bytes"


def test_fetch_documents_iterates_list_then_fetch(coords):
    processor = SnowflakeSourceProcessor(coords)
    processor._connection = MagicMock()

    ids = [
        SnowflakeFileIdentifier(relative_path="a.pdf", size=1),
        SnowflakeFileIdentifier(relative_path="b.pdf", size=1),
    ]
    processor._list_document_ids = MagicMock(return_value=iter(ids))
    processor._fetch_document_by_id = MagicMock(
        side_effect=lambda ident, *, max_file_size=None: DocumentStream(
            name=ident.relative_path, stream=BytesIO(b"x")
        )
    )

    docs = list(processor._fetch_documents())

    assert [d.name for d in docs] == ["a.pdf", "b.pdf"]
