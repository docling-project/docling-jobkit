from unittest.mock import MagicMock, patch

import pytest

from docling_jobkit.connectors.snowflake.models import (
    SnowflakeChunkTarget,
    SnowflakeDocTarget,
)
from docling_jobkit.connectors.snowflake.target_processor import (
    SnowflakeTargetProcessor,
)
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem
from docling_jobkit.public_errors import TargetWriteError

_BASE = {
    "account": "xy12345",
    "user": "me",
    "password": "p",
    "warehouse": "WH",
    "database": "DB",
    "db_schema": "SCH",
}


@pytest.fixture
def doc_target() -> SnowflakeDocTarget:
    return SnowflakeDocTarget(**_BASE, table="DOCS")


@pytest.fixture
def chunk_target() -> SnowflakeChunkTarget:
    return SnowflakeChunkTarget(**_BASE, table="CHUNKS")


def _chunk(index: int, filename: str = "doc.pdf") -> ChunkedDocumentResultItem:
    return ChunkedDocumentResultItem(
        filename=filename,
        chunk_index=index,
        text=f"chunk {index}",
        headings=[],
        page_numbers=[],
        doc_items=[],
        metadata={},
    )


def test_check_dependencies_present():
    SnowflakeTargetProcessor.check_dependencies()


def test_get_config_types():
    assert SnowflakeTargetProcessor.get_config_types() == (
        SnowflakeDocTarget,
        SnowflakeChunkTarget,
    )


def test_initialize_wraps_connection_failure(doc_target):
    processor = SnowflakeTargetProcessor(doc_target)
    # Auth error wrapped by decorator, not raised directly
    from snowflake.connector.errors import DatabaseError

    with patch(
        "docling_jobkit.connectors.snowflake.target_processor.get_snowflake_connection",
        side_effect=DatabaseError("auth failed"),
    ):
        # Error is classified by the decorator as authentication error
        from docling_jobkit.connectors.errors import ConnectorAuthenticationError

        with pytest.raises(ConnectorAuthenticationError):
            processor._initialize()


def test_finalize_closes_session(doc_target):
    processor = SnowflakeTargetProcessor(doc_target)
    mock_session = MagicMock()
    processor._session = mock_session

    processor._finalize()

    mock_session.close.assert_called_once()
    assert processor._session is None


# --- whole-document path ---


def test_upsert_row_uses_mapped_id_field_value(doc_target):
    processor = SnowflakeTargetProcessor(doc_target)
    processor._session = MagicMock()

    with patch(
        "docling_jobkit.connectors.snowflake.target_processor.upsert_table_row"
    ) as mock_upsert:
        processor.upsert_row({"doc_id": "explicit-id", "content_text": "hi"})

    row = mock_upsert.call_args[0][3]
    assert row["doc_id"] == "explicit-id"


def test_upsert_row_falls_back_to_pending_doc_id(doc_target):
    processor = SnowflakeTargetProcessor(doc_target)
    processor._session = MagicMock()
    processor._pending_doc_id = "pending-id"

    with patch(
        "docling_jobkit.connectors.snowflake.target_processor.upsert_table_row"
    ) as mock_upsert:
        processor.upsert_row({"content_text": "hi"})

    row = mock_upsert.call_args[0][3]
    assert row["doc_id"] == "pending-id"


def test_upsert_row_falls_back_to_content_hash(doc_target):
    processor = SnowflakeTargetProcessor(doc_target)
    processor._session = MagicMock()

    with patch(
        "docling_jobkit.connectors.snowflake.target_processor.upsert_table_row"
    ) as mock_upsert:
        processor.upsert_row({"content_text": "hi"})

    row = mock_upsert.call_args[0][3]
    assert len(row["doc_id"]) == 64  # sha256 hex digest


def test_upsert_row_wraps_write_failure(doc_target):
    processor = SnowflakeTargetProcessor(doc_target)
    processor._session = MagicMock()

    with patch(
        "docling_jobkit.connectors.snowflake.target_processor.upsert_table_row",
        side_effect=RuntimeError("merge failed"),
    ):
        with pytest.raises(TargetWriteError, match="Failed to write document row"):
            processor.upsert_row({"doc_id": "x"})


def test_upsert_row_rejects_chunk_target(chunk_target):
    processor = SnowflakeTargetProcessor(chunk_target)
    processor._session = MagicMock()

    with pytest.raises(NotImplementedError, match="whole-document"):
        processor.upsert_row({"text": "hi"})


# --- streaming chunk protocol ---


def test_instance_requires_chunks(doc_target, chunk_target):
    assert SnowflakeTargetProcessor(doc_target).instance_requires_chunks() is False
    assert SnowflakeTargetProcessor(chunk_target).instance_requires_chunks() is True


def test_consume_chunk_buffers_without_writing(chunk_target):
    processor = SnowflakeTargetProcessor(chunk_target)
    processor._session = MagicMock()

    processor.begin_chunks(filename="doc.pdf", temp_dir=MagicMock())
    processor.consume_chunk(_chunk(0))
    processor.consume_chunk(_chunk(1))

    assert len(processor._chunk_buffer) == 2
    processor._session.sql.assert_not_called()


def test_consume_chunk_rejects_doc_target(doc_target):
    processor = SnowflakeTargetProcessor(doc_target)
    processor.begin_chunks(filename="doc.pdf", temp_dir=MagicMock())

    with pytest.raises(TypeError, match="SnowflakeChunkTarget"):
        processor.consume_chunk(_chunk(0))


def test_end_chunks_flushes_buffer_in_one_call(chunk_target):
    processor = SnowflakeTargetProcessor(chunk_target)
    processor._session = MagicMock()

    processor.begin_chunks(filename="doc.pdf", temp_dir=MagicMock())
    for i in range(5):
        processor.consume_chunk(_chunk(i))

    with patch(
        "docling_jobkit.connectors.snowflake.target_processor.upsert_table_rows"
    ) as mock_upsert:
        processor.end_chunks()

    mock_upsert.assert_called_once()
    rows = mock_upsert.call_args[0][3]
    assert len(rows) == 5
    assert processor._chunk_buffer == []
    assert processor._current_document_hash is None


def test_end_chunks_with_no_chunks_does_not_write(chunk_target):
    processor = SnowflakeTargetProcessor(chunk_target)
    processor._session = MagicMock()
    processor.begin_chunks(filename="doc.pdf", temp_dir=MagicMock())

    with patch(
        "docling_jobkit.connectors.snowflake.target_processor.upsert_table_rows"
    ) as mock_upsert:
        processor.end_chunks()

    mock_upsert.assert_not_called()


def test_end_chunks_wraps_failure_and_still_clears_buffer(chunk_target):
    processor = SnowflakeTargetProcessor(chunk_target)
    processor._session = MagicMock()
    processor.begin_chunks(filename="doc.pdf", temp_dir=MagicMock())
    processor.consume_chunk(_chunk(0))

    with patch(
        "docling_jobkit.connectors.snowflake.target_processor.upsert_table_rows",
        side_effect=RuntimeError("merge failed"),
    ):
        with pytest.raises(TargetWriteError, match="Failed to write chunk rows"):
            processor.end_chunks()

    assert processor._chunk_buffer == []
    assert processor._current_document_hash is None


def test_abort_chunks_discards_buffer_without_writing(chunk_target):
    processor = SnowflakeTargetProcessor(chunk_target)
    processor._session = MagicMock()

    processor.begin_chunks(filename="doc.pdf", temp_dir=MagicMock())
    processor.consume_chunk(_chunk(0))
    processor.consume_chunk(_chunk(1))

    processor.abort_chunks()

    assert processor._chunk_buffer == []
    processor._session.sql.assert_not_called()


def test_stable_chunk_id_is_deterministic_and_scoped_to_inputs():
    id1 = SnowflakeTargetProcessor._stable_chunk_id(
        binary_hash="abc123", filename="test.pdf", chunk_index=0
    )
    id2 = SnowflakeTargetProcessor._stable_chunk_id(
        binary_hash="abc123", filename="test.pdf", chunk_index=0
    )
    assert id1 == id2
    assert len(id1) == 64

    id_no_hash = SnowflakeTargetProcessor._stable_chunk_id(
        binary_hash=None, filename="test.pdf", chunk_index=0
    )
    assert id_no_hash != id1

    id_other_index = SnowflakeTargetProcessor._stable_chunk_id(
        binary_hash="abc123", filename="test.pdf", chunk_index=1
    )
    assert id_other_index != id1
