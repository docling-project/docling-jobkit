from typing import cast
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")

from docling_jobkit.connectors.spark.backend_connect import SparkConnectBackend


@pytest.fixture(autouse=True)
def _stub_spark_functions(monkeypatch):
    import pyspark.sql.functions as F

    monkeypatch.setattr(F, "col", lambda *a, **k: MagicMock())
    monkeypatch.setattr(F, "sha2", lambda *a, **k: MagicMock())


def _chainable_df(rows) -> MagicMock:
    test_df = MagicMock()
    for op in ("filter", "select", "limit", "orderBy"):
        getattr(test_df, op).return_value = test_df
    test_df.toLocalIterator.return_value = iter(rows)
    test_df.count.return_value = len(rows)

    return test_df


def _backend(df=None, *, table_exists=False) -> SparkConnectBackend:
    backend = SparkConnectBackend.__new__(SparkConnectBackend)  # skip real session
    spark = MagicMock()
    if df is not None:
        spark.table.return_value = df
    spark.catalog.tableExists.return_value = table_exists
    backend._spark = spark
    return backend


def test_enumerate_row_keys_yields_partition_hash_filename():
    from pyspark.sql import Row

    test_df = _chainable_df(
        [
            Row(dt="2024-01-01", row_key="h1", fname="a.pdf"),
            Row(dt="2024-01-02", row_key="h2", fname=None),
        ]
    )
    out = list(
        _backend(test_df).enumerate_row_keys("t", "content", "dt", "doc_name", None)
    )

    assert out == [("2024-01-01", "h1", "a.pdf"), ("2024-01-02", "h2", None)]


def test_read_partition_yields_rowkey_bytes_name():
    from pyspark.sql import Row

    test_df = _chainable_df([Row(row_key="h1", c=b"PDF", fname="a.pdf")])
    out = list(
        _backend(test_df).read_partition("t", "content", "dt", "2024-01-01", "doc_name")
    )

    assert out == [("h1", b"PDF", "a.pdf")]


def test_stream_documents_yields_bytes_and_name():
    from pyspark.sql import Row

    test_df = _chainable_df([Row(c=b"PDF", fname="a.pdf")])
    out = list(_backend(test_df).stream_documents("t", "content", "doc_name", None))

    assert out == [(b"PDF", "a.pdf")]


def test_write_rows_delta_existing_merges(monkeypatch):
    merges = []
    monkeypatch.setattr(
        "docling_jobkit.connectors.spark.backend_connect.merge_with_retry",
        lambda *a, **k: merges.append(a),
    )
    monkeypatch.setattr(
        "docling_jobkit.connectors.spark.backend_connect.build_row_schema",
        lambda *a, **k: None,
    )
    backend = _backend(table_exists=True)
    backend.write_rows(
        "t",
        ["doc_id", "text"],
        set(),
        [{"doc_id": "d", "text": "x"}],
        key="doc_id",
        table_format="delta",
    )

    assert len(merges) == 1

    spark = cast(MagicMock, backend._spark)
    spark.createDataFrame.return_value.write.format.assert_not_called()


@pytest.mark.parametrize(
    "table_format, table_exists",
    [("delta", False), ("parquet", True)],
    ids=["delta_missing_appends", "parquet_existing_appends"],
)
def test_write_rows_appends_when_not_merging(monkeypatch, table_format, table_exists):
    monkeypatch.setattr(
        "docling_jobkit.connectors.spark.backend_connect.build_row_schema",
        lambda *a, **k: None,
    )
    merged = []
    monkeypatch.setattr(
        "docling_jobkit.connectors.spark.backend_connect.merge_with_retry",
        lambda *a, **k: merged.append(a),
    )
    backend = _backend(table_exists=table_exists)
    spark = cast(MagicMock, backend._spark)
    test_df = spark.createDataFrame.return_value
    backend.write_rows(
        "t",
        ["doc_id"],
        set(),
        [{"doc_id": "d"}],
        key="doc_id",
        table_format=table_format,
    )

    assert merged == []

    test_df.write.format.assert_called_once_with(table_format)
    test_df.write.format.return_value.mode.assert_called_once_with("append")
    test_df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
        "t"
    )
