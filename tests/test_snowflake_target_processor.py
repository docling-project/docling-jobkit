from unittest.mock import MagicMock, patch

import pytest

from docling_jobkit.connectors.snowflake.models import SnowflakeDocTarget
from docling_jobkit.connectors.snowflake.target_processor import (
    SnowflakeTargetProcessor,
)
from docling_jobkit.public_errors import TargetWriteError


@pytest.fixture
def target() -> SnowflakeDocTarget:
    return SnowflakeDocTarget(
        account="xy12345",
        user="me",
        password="p",
        warehouse="WH",
        database="DB",
        db_schema="SCH",
        table="DOCS",
    )


def test_check_dependencies_present():
    SnowflakeTargetProcessor.check_dependencies()


def test_get_config_types(target):
    assert SnowflakeTargetProcessor.get_config_types() == (SnowflakeDocTarget,)


def test_initialize_wraps_connection_failure(target):
    processor = SnowflakeTargetProcessor(target)
    with patch(
        "docling_jobkit.connectors.snowflake.target_processor.get_snowflake_connection",
        side_effect=RuntimeError("boom"),
    ):
        with pytest.raises(TargetWriteError, match="Could not connect"):
            processor._initialize()


def test_finalize_closes_connection(target):
    processor = SnowflakeTargetProcessor(target)
    mock_conn = MagicMock()
    processor._connection = mock_conn

    processor._finalize()

    mock_conn.close.assert_called_once()
    assert processor._connection is None


def test_upsert_row_uses_mapped_id_field_value(target):
    processor = SnowflakeTargetProcessor(target)
    processor._connection = MagicMock()

    with patch(
        "docling_jobkit.connectors.snowflake.target_processor.upsert_document_row"
    ) as mock_upsert:
        processor.upsert_row({"doc_id": "explicit-id", "content_text": "hi"})

    row = mock_upsert.call_args[0][2]
    assert row["doc_id"] == "explicit-id"


def test_upsert_row_falls_back_to_pending_doc_id(target):
    processor = SnowflakeTargetProcessor(target)
    processor._connection = MagicMock()
    processor._pending_doc_id = "pending-id"

    with patch(
        "docling_jobkit.connectors.snowflake.target_processor.upsert_document_row"
    ) as mock_upsert:
        processor.upsert_row({"content_text": "hi"})

    row = mock_upsert.call_args[0][2]
    assert row["doc_id"] == "pending-id"


def test_upsert_row_falls_back_to_content_hash(target):
    processor = SnowflakeTargetProcessor(target)
    processor._connection = MagicMock()

    with patch(
        "docling_jobkit.connectors.snowflake.target_processor.upsert_document_row"
    ) as mock_upsert:
        processor.upsert_row({"content_text": "hi"})

    row = mock_upsert.call_args[0][2]
    assert len(row["doc_id"]) == 64  # sha256 hex digest


def test_upsert_row_wraps_write_failure(target):
    processor = SnowflakeTargetProcessor(target)
    processor._connection = MagicMock()

    with patch(
        "docling_jobkit.connectors.snowflake.target_processor.upsert_document_row",
        side_effect=RuntimeError("merge failed"),
    ):
        with pytest.raises(TargetWriteError, match="Failed to write document row"):
            processor.upsert_row({"doc_id": "x"})
