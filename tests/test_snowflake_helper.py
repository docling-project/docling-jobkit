import gzip
from pathlib import Path
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
    upsert_document_row,
)
from docling_jobkit.connectors.snowflake.models import (
    SnowflakeCoordinates,
    SnowflakeDocTarget,
)


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
    assert is_snowflake_authentication_error(ForbiddenError("forbidden"))


def test_is_authentication_error_false_for_programming_and_operational_errors():
    from snowflake.connector.errors import OperationalError, ProgrammingError

    assert not is_snowflake_authentication_error(ProgrammingError("bad sql"))
    assert not is_snowflake_authentication_error(OperationalError("timeout"))


def test_is_unavailable_error_true_for_operational_and_http_errors():
    from snowflake.connector.errors import HttpError, OperationalError

    assert is_snowflake_unavailable_error(OperationalError("timeout"))
    assert is_snowflake_unavailable_error(HttpError("404"))


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
    with patch("snowflake.connector.connect") as mock_connect:
        get_snowflake_connection(coords)

    kwargs = mock_connect.call_args.kwargs
    assert kwargs["password"] == "p"
    assert "private_key" not in kwargs


def test_get_snowflake_connection_uses_private_key():
    coords = SnowflakeCoordinates(
        account="xy12345",
        user="me",
        private_key="-----BEGIN PRIVATE KEY-----",
        warehouse="WH",
        database="DB",
        db_schema="SCH",
        stage="STG",
    )
    with patch(
        "docling_jobkit.connectors.snowflake.helper._load_private_key_der",
        return_value=b"der-bytes",
    ) as mock_load_key:
        with patch("snowflake.connector.connect") as mock_connect:
            get_snowflake_connection(coords)

    mock_load_key.assert_called_once_with("-----BEGIN PRIVATE KEY-----", None)
    assert mock_connect.call_args.kwargs["private_key"] == b"der-bytes"
    assert "password" not in mock_connect.call_args.kwargs


# Listing


def test_list_stage_files_builds_sql_with_prefix_and_pattern(coords):
    coords = coords.model_copy(update={"prefix": "in/", "pattern": ".*[.]pdf"})
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.__iter__.return_value = iter([{"name": "STG/in/a.pdf"}])

    rows = list(list_stage_files(conn, coords))

    sql = cur.execute.call_args[0][0]
    assert "@DB.SCH.STG/in/" in sql
    assert "PATTERN = '.*[.]pdf'" in sql
    assert rows == [{"name": "STG/in/a.pdf"}]


# Download + gzip


def _fake_get_execute_writing(filename: str, content: bytes):
    def fake_execute(sql: str) -> None:
        local_dir = Path(sql.split()[-1].removeprefix("file://"))
        (local_dir / filename).write_bytes(content)

    return fake_execute


def test_download_stage_file_decompresses_gz(coords):
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.execute.side_effect = _fake_get_execute_writing(
        "file.pdf.gz", gzip.compress(b"hello world")
    )

    data, name = download_stage_file(conn, coords, "sub/file.pdf.gz")

    assert data == b"hello world"
    assert name == "file.pdf"


def test_download_stage_file_passes_through_uncompressed(coords):
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.execute.side_effect = _fake_get_execute_writing("file.pdf", b"raw pdf bytes")

    data, name = download_stage_file(conn, coords, "file.pdf")

    assert data == b"raw pdf bytes"
    assert name == "file.pdf"


def test_download_stage_file_raises_when_get_produces_nothing(coords):
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.execute.side_effect = lambda sql: None

    with pytest.raises(FileNotFoundError):
        download_stage_file(conn, coords, "missing.pdf")


# MERGE builder


def test_upsert_document_row_builds_unquoted_merge_and_json_encodes_dicts(target):
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value

    row = {"doc_id": "abc", "content_json": {"a": 1}, "content_text": "hi"}
    upsert_document_row(conn, target, row)

    sql, values = cur.execute.call_args[0]
    assert "MERGE INTO DB.SCH.DOCS" in sql
    # Unquoted identifiers -- Snowflake's own case-folding must resolve them,
    # matching how unquoted DDL upper-cases column names by default.
    assert '"doc_id"' not in sql
    assert (
        "WHEN MATCHED THEN UPDATE SET t.content_json = s.content_json, "
        "t.content_text = s.content_text" in sql
    )
    assert values == ["abc", '{"a": 1}', "hi"]


def test_upsert_document_row_omits_when_matched_with_no_extra_columns(target):
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value

    upsert_document_row(conn, target, {"doc_id": "abc"})

    sql = cur.execute.call_args[0][0]
    assert "WHEN MATCHED" not in sql
    assert "WHEN NOT MATCHED THEN INSERT (doc_id) VALUES (s.doc_id)" in sql


def test_upsert_document_row_requires_id_field_in_row(target):
    conn = MagicMock()
    with pytest.raises(ValueError, match="Row is missing id field"):
        upsert_document_row(conn, target, {"content_text": "hi"})
