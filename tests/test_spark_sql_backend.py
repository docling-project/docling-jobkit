from unittest.mock import MagicMock


def _backend_with_cursor():
    from docling_jobkit.connectors.spark.backend_sql import DatabricksSqlBackend

    backend = DatabricksSqlBackend.__new__(DatabricksSqlBackend)
    backend._staging_token = "tok"  # __init__ (which sets the uuid) is skipped
    cur = MagicMock()
    ctx = MagicMock()
    ctx.__enter__.return_value = cur
    conn = MagicMock()
    conn.cursor.return_value = ctx
    backend._conn = conn

    return backend, cur


def test_delta_write_stages_then_merges():
    backend, cur = _backend_with_cursor()
    backend.write_rows(
        "cat.db.out",
        ["doc_id", "text"],
        set(),
        [{"doc_id": "d", "text": "x"}],
        key="doc_id",
        table_format="delta",
    )
    executed = " || ".join(c.args[0] for c in cur.execute.call_args_list)

    assert "CREATE TABLE IF NOT EXISTS `cat`.`db`.`out`" in executed
    assert "TRUNCATE TABLE" in executed
    assert "MERGE INTO `cat`.`db`.`out`" in executed

    cur.executemany.assert_called_once()  # staging INSERT, parameter-bound


def test_enumerate_row_keys_sql_uses_id_column_when_set():
    """When id_column is set, row_key is cast from it directly — no sha2 over
    content_column, so fetch_by_id can look the row up by the same value
    without content ever being hashed during enumeration."""
    backend, cur = _backend_with_cursor()
    cur.fetchmany.side_effect = [[("2024-01-01", "id1", None)], []]

    out = list(
        backend.enumerate_row_keys(
            "cat.db.docs", "content", "dt", None, None, id_column="doc_id"
        )
    )

    executed = cur.execute.call_args_list[0].args[0]
    assert "CAST(`doc_id` AS STRING) AS row_key" in executed
    assert "sha2(" not in executed
    assert out == [("2024-01-01", "id1", None)]


def test_enumerate_row_keys_sql_hashes_content_when_no_id_column():
    backend, cur = _backend_with_cursor()
    cur.fetchmany.side_effect = [[("2024-01-01", "h1", None)], []]

    list(backend.enumerate_row_keys("cat.db.docs", "content", "dt", None, None))

    executed = cur.execute.call_args_list[0].args[0]
    assert "sha2(`content`, 256) AS row_key" in executed


def test_non_delta_write_appends_without_merge():
    backend, cur = _backend_with_cursor()
    backend.write_rows(
        "cat.db.out",
        ["doc_id"],
        set(),
        [{"doc_id": "d"}],
        key="doc_id",
        table_format="parquet",
    )
    executed = " || ".join(c.args[0] for c in cur.execute.call_args_list)

    assert "MERGE INTO" not in executed

    cur.executemany.assert_called_once_with(
        "INSERT INTO `cat`.`db`.`out` (`doc_id`) VALUES (?)", [["d"]]
    )
