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
