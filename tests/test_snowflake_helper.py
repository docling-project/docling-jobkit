import gzip
from unittest.mock import MagicMock, patch

import pytest

from docling_jobkit.connectors.snowflake.helper import (
    download_stage_file,
    get_snowflake_connection,
    is_snowflake_authentication_error,
    is_snowflake_unavailable_error,
    list_stage_files,
    relative_path_from_list_name,
    stage_ref,
    table_ref,
    upsert_table_row,
    upsert_table_rows,
)
from docling_jobkit.connectors.snowflake.models import (
    SnowflakeCoordinates,
    SnowflakeDocTarget,
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


# Error


def test_is_authentication_error_true_for_bare_database_and_forbidden_errors():
    from snowflake.connector.errors import DatabaseError, ForbiddenError

    assert is_snowflake_authentication_error(DatabaseError("bad creds"))
    assert is_snowflake_authentication_error(ForbiddenError(msg="forbidden"))


def test_is_authentication_error_false_for_programming_and_operational_errors():
    from snowflake.connector.errors import OperationalError, ProgrammingError

    assert not is_snowflake_authentication_error(ProgrammingError("bad sql"))
    assert not is_snowflake_authentication_error(OperationalError("timeout"))


def test_is_unavailable_error_true_for_operational_and_http_errors():
    from snowflake.connector.errors import HttpError, OperationalError

    assert is_snowflake_unavailable_error(OperationalError("timeout"))
    assert is_snowflake_unavailable_error(HttpError(msg="404"))


def test_is_unavailable_error_false_for_programming_error():
    from snowflake.connector.errors import ProgrammingError

    assert not is_snowflake_unavailable_error(ProgrammingError("bad sql"))


# Ref


def test_stage_ref_and_table_ref(coords, target):
    assert stage_ref(coords) == "DB.SCH.STG"
    assert table_ref(target) == "DB.SCH.DOCS"


def test_relative_path_from_list_name_strips_leading_segment():
    assert relative_path_from_list_name("stg/sub/file.pdf") == "sub/file.pdf"
    assert relative_path_from_list_name("file.pdf") == "file.pdf"


# Connection


def test_get_snowflake_connection_uses_password(coords):
    with patch("snowflake.snowpark.Session.builder") as mock_builder:
        mock_session = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session

        result = get_snowflake_connection(coords)

    config = mock_builder.configs.call_args[0][0]
    assert config["password"] == "p"
    assert "private_key" not in config
    assert result == mock_session


def test_get_snowflake_connection_uses_private_key():
    coords = SnowflakeCoordinates(
        account="xy12345",
        user="me",
        private_key="-----BEGIN PRIVATE KEY-----\nkey data\n-----END PRIVATE KEY-----",
        warehouse="WH",
        database="DB",
        db_schema="SCH",
        stage="STG",
    )
    with (
        patch("snowflake.snowpark.Session.builder") as mock_builder,
        patch(
            "docling_jobkit.connectors.snowflake.helper._load_private_key_der"
        ) as mock_load_key,
    ):
        mock_session = MagicMock()
        mock_builder.configs.return_value.create.return_value = mock_session
        mock_load_key.return_value = b"fake_der_bytes"

        result = get_snowflake_connection(coords)

    config = mock_builder.configs.call_args[0][0]
    assert config["private_key"] == b"fake_der_bytes"
    assert "password" not in config
    assert result == mock_session
    mock_load_key.assert_called_once_with(
        "-----BEGIN PRIVATE KEY-----\nkey data\n-----END PRIVATE KEY-----", None
    )


# Listing


def _mock_row(data: dict) -> MagicMock:
    """A Snowpark Row supports both row["name"] and row.as_dict()."""
    row = MagicMock()
    row.__getitem__.side_effect = data.__getitem__
    row.as_dict.return_value = data
    return row


def test_list_stage_files_builds_sql_with_prefix_and_pattern(coords):
    coords = coords.model_copy(update={"prefix": "in/", "pattern": ".*[.]pdf"})
    session = MagicMock()
    mock_row = _mock_row({"name": "STG/in/a.pdf"})
    session.sql.return_value.to_local_iterator.return_value = iter([mock_row])

    rows = list(list_stage_files(session, coords))

    sql = session.sql.call_args[0][0]
    assert "@DB.SCH.STG/in/" in sql
    assert "PATTERN = '.*[.]pdf'" in sql
    assert rows == [{"name": "STG/in/a.pdf"}]


def test_list_stage_files_applies_max_num_elements_without_exhausting_iterator(coords):
    coords = coords.model_copy(update={"max_num_elements": 2})
    session = MagicMock()
    mock_rows = [_mock_row({"name": f"STG/{i}.pdf"}) for i in range(5)]

    consumed = []

    def _tracking_iter():
        for row in mock_rows:
            consumed.append(row)
            yield row

    session.sql.return_value.to_local_iterator.return_value = _tracking_iter()

    rows = list(list_stage_files(session, coords))

    assert rows == [{"name": "STG/0.pdf"}, {"name": "STG/1.pdf"}]
    # Only pulled as many rows off the (lazy) iterator as it needed.
    assert len(consumed) == 2


def test_list_stage_files_skips_directory_markers(coords):
    session = MagicMock()
    mock_rows = [
        _mock_row({"name": "STG/subdir/"}),
        _mock_row({"name": "STG/subdir/a.pdf"}),
    ]
    session.sql.return_value.to_local_iterator.return_value = iter(mock_rows)

    rows = list(list_stage_files(session, coords))

    assert rows == [{"name": "STG/subdir/a.pdf"}]


def test_list_stage_files_rejects_prefix_breaking_out_of_sql_literal(coords):
    coords = coords.model_copy(update={"prefix": "in/'; DROP TABLE t; --"})
    session = MagicMock()

    with pytest.raises(ValueError, match="prefix"):
        list(list_stage_files(session, coords))

    session.sql.assert_not_called()


def test_list_stage_files_rejects_pattern_breaking_out_of_sql_literal(coords):
    coords = coords.model_copy(update={"pattern": "a' OR '1'='1"})
    session = MagicMock()

    with pytest.raises(ValueError, match="PATTERN"):
        list(list_stage_files(session, coords))

    session.sql.assert_not_called()


# Download + gzip


def test_download_stage_file_decompresses_gz(coords):
    session = MagicMock()
    mock_stream = MagicMock()
    mock_stream.read.return_value = gzip.compress(b"hello world")
    session.file.get_stream.return_value = mock_stream

    data, name = download_stage_file(session, coords, "sub/file.pdf.gz")

    assert data == b"hello world"
    assert name == "file.pdf"
    mock_stream.close.assert_called_once()


def test_download_stage_file_passes_through_uncompressed(coords):
    session = MagicMock()
    mock_stream = MagicMock()
    mock_stream.read.return_value = b"raw pdf bytes"
    session.file.get_stream.return_value = mock_stream

    data, name = download_stage_file(session, coords, "file.pdf")

    assert data == b"raw pdf bytes"
    assert name == "file.pdf"
    mock_stream.close.assert_called_once()


def test_download_stage_file_allows_decompressed_output_within_max_file_size(coords):
    session = MagicMock()
    mock_stream = MagicMock()
    mock_stream.read.return_value = gzip.compress(b"hello world")
    session.file.get_stream.return_value = mock_stream

    data, name = download_stage_file(
        session, coords, "sub/file.pdf.gz", max_file_size=len(b"hello world")
    )

    assert data == b"hello world"
    assert name == "file.pdf"


def test_download_stage_file_rejects_gzip_bomb_exceeding_max_file_size(coords):
    """A small compressed payload that expands past max_file_size must be
    caught during decompression, not after the whole thing is materialized."""
    session = MagicMock()
    mock_stream = MagicMock()
    huge = b"a" * (10 * 1024 * 1024)
    mock_stream.read.return_value = gzip.compress(huge)
    session.file.get_stream.return_value = mock_stream

    with pytest.raises(SourceLimitExceededError, match="Decompressed size exceeds"):
        download_stage_file(session, coords, "sub/file.pdf.gz", max_file_size=1024)


# MERGE builder


def test_upsert_table_row_builds_unquoted_merge_and_binds_values(target):
    session = MagicMock()

    row = {"doc_id": "abc", "content_json": {"a": 1}, "content_text": "hi"}
    upsert_table_row(session, target, "doc_id", row)

    args, kwargs = session.sql.call_args
    sql = args[0]
    assert "MERGE INTO DB.SCH.DOCS" in sql
    # Unquoted identifiers. Snowflake's own case-folding must resolve them,
    # matching how unquoted DDL upper-cases column names by default.
    assert '"doc_id"' not in sql
    assert (
        "WHEN MATCHED THEN UPDATE SET t.content_json = s.content_json, "
        "t.content_text = s.content_text" in sql
    )
    # Values are bound as query parameters, not formatted into the SQL text.
    assert "VALUES (?, ?, ?)" in sql
    assert kwargs["params"] == ["abc", '{"a": 1}', "hi"]


def test_upsert_table_rows_rejects_invalid_column_name(target):
    session = MagicMock()
    rows = [{"doc_id": "c1", "text; DROP TABLE t": "a"}]

    with pytest.raises(ValueError, match="Unsafe Snowflake column name"):
        upsert_table_rows(session, target, "doc_id", rows)

    session.sql.assert_not_called()


def test_upsert_table_row_omits_when_matched_with_no_extra_columns(target):
    session = MagicMock()

    upsert_table_row(session, target, "doc_id", {"doc_id": "abc"})

    sql = session.sql.call_args[0][0]
    assert "WHEN MATCHED" not in sql
    assert "WHEN NOT MATCHED THEN INSERT (doc_id) VALUES (s.doc_id)" in sql


def test_upsert_table_row_requires_id_field_in_row(target):
    session = MagicMock()
    with pytest.raises(ValueError, match="Row is missing id field"):
        upsert_table_row(session, target, "doc_id", {"content_text": "hi"})


def test_upsert_table_row_uses_id_field_param_not_a_fixed_attribute(target):
    """A chunk target's row is keyed on chunk_id_field, passed explicitly --
    upsert_table_row must key off the given id_field, not assume a fixed
    attribute name (chunk targets don't have `id_field`)."""
    session = MagicMock()

    upsert_table_row(session, target, "chunk_id", {"chunk_id": "c1", "text": "hi"})

    sql = session.sql.call_args[0][0]
    assert "ON t.chunk_id = s.chunk_id" in sql


# --- batched MERGE builder ---


def test_upsert_table_rows_builds_one_multi_row_merge(target):
    session = MagicMock()

    rows = [
        {"doc_id": "c1", "text": "a"},
        {"doc_id": "c2", "text": "b"},
        {"doc_id": "c3", "text": "c"},
    ]
    upsert_table_rows(session, target, "doc_id", rows)

    assert session.sql.call_count == 1
    args, kwargs = session.sql.call_args
    sql = args[0]
    # Values are bound as query parameters, not formatted into the SQL text.
    assert "USING (VALUES (?, ?), (?, ?), (?, ?)) AS s (doc_id, text)" in sql
    assert kwargs["params"] == ["c1", "a", "c2", "b", "c3", "c"]


def test_upsert_table_rows_empty_list_is_a_noop(target):
    session = MagicMock()

    upsert_table_rows(session, target, "doc_id", [])

    session.sql.assert_not_called()


def test_upsert_table_rows_rejects_mismatched_columns(target):
    session = MagicMock()
    rows = [{"doc_id": "c1", "text": "a"}, {"doc_id": "c2", "other": "b"}]

    with pytest.raises(ValueError, match="same columns"):
        upsert_table_rows(session, target, "doc_id", rows)


def test_upsert_table_row_delegates_to_batched_form(target):
    session = MagicMock()
    with patch(
        "docling_jobkit.connectors.snowflake.helper.upsert_table_rows"
    ) as mock_batched:
        upsert_table_row(session, target, "doc_id", {"doc_id": "abc"})

    mock_batched.assert_called_once_with(session, target, "doc_id", [{"doc_id": "abc"}])
