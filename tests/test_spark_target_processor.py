from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")

from docling_jobkit.connectors.spark.target_processor import SparkTargetProcessor
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem
from docling_jobkit.datamodel.spark_coords import SparkChunkTarget, SparkDocTarget


def _doc_target(**overrides) -> SparkDocTarget:
    base = {
        "host": "h",
        "table": "cat.db.out",
        "mappings": {"MARKDOWN": "text", "JSON": "doc_json"},
        "port": 8080,
    }
    base.update(overrides)
    return SparkDocTarget(**base)


def _chunk_target(**overrides) -> SparkChunkTarget:
    base = {"host": "h", "table": "cat.db.chunks", "port": 8080}
    base.update(overrides)
    return SparkChunkTarget(**base)


@pytest.fixture
def make_proc():
    """Build a processor with its Spark session mocked out."""

    def _make(target, *, table_exists=False) -> SparkTargetProcessor:
        proc = SparkTargetProcessor(target)
        proc._spark = MagicMock()
        proc._spark.catalog.tableExists.return_value = table_exists
        return proc

    return _make


@pytest.fixture
def chunk_item() -> ChunkedDocumentResultItem:
    return ChunkedDocumentResultItem(
        filename="test.pdf",
        chunk_index=0,
        text="chunk text",
        headings=[],
        page_numbers=[],
        doc_items=[],
        metadata={"k": "v"},
    )


def test_config_types() -> None:
    assert SparkTargetProcessor.get_config_types() == (SparkDocTarget, SparkChunkTarget)


@pytest.mark.parametrize(
    "target_factory, expected",
    [(_chunk_target, True), (_doc_target, False)],
    ids=["chunk", "doc"],
)
def test_instance_requires_chunks(target_factory, expected) -> None:
    proc = SparkTargetProcessor(target_factory())
    assert proc.instance_requires_chunks() is expected


def test_doc_target_buffers_and_flushes_row(make_proc) -> None:
    # Delta default, flush_batch_size=1 -> flush on the single row. Table does not
    # exist -> create-if-missing (saveAsTable), no MERGE sql.
    proc = make_proc(_doc_target(flush_batch_size=1), table_exists=False)

    proc.begin_document("a.pdf")
    proc._pending_row["text"] = "hello"  # simulate accumulated MARKDOWN
    proc.end_document("a.pdf")

    proc._spark.createDataFrame.assert_called_once()  # flushed at batch size 1
    proc._spark.sql.assert_not_called()  # no MERGE when table missing
    row = proc._spark.createDataFrame.call_args.args[0][0]
    assert row["doc_id"] == "a.pdf"
    assert row["text"] == "hello"


def test_doc_target_parquet_flush_appends_and_skips_merge(make_proc) -> None:
    # Non-delta format: plain append (at-least-once), never a MERGE.
    # table_exists=True proves the non-delta path skips MERGE even when it exists.
    proc = make_proc(
        _doc_target(table_format="parquet", flush_batch_size=1), table_exists=True
    )
    spark_df = proc._spark.createDataFrame.return_value

    proc.begin_document("a.pdf")
    proc._pending_row["text"] = "hello"
    proc.end_document("a.pdf")

    spark_df.write.format.assert_called_once_with("parquet")
    spark_df.write.format.return_value.mode.assert_called_once_with("append")
    spark_df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
        "cat.db.out"
    )
    proc._spark.sql.assert_not_called()  # no MERGE on the non-delta path


def test_chunk_consume_normalizes_row(make_proc, chunk_item) -> None:
    # large flush_batch_size so the row stays buffered and inspectable.
    proc = make_proc(_chunk_target(flush_batch_size=1000))
    proc.begin_chunks("test.pdf", MagicMock(), document_hash="h123")

    proc.consume_chunk(chunk_item)

    # _add_row normalizes before appending, so _buffer[-1] is the normalized row
    # (metadata is a JSON string); the assertions below are on unaffected fields.
    row = proc._buffer[-1]
    assert row["text"] == "chunk text"
    assert row["doc_id"] == "test.pdf"
    assert row["chunk_index"] == 0
    assert len(row["chunk_id"]) == 64  # sha-256 hex


def test_chunk_id_is_deterministic_across_instances(make_proc, chunk_item) -> None:
    def _chunk_id() -> str:
        proc = make_proc(_chunk_target(flush_batch_size=1000))
        proc.begin_chunks("test.pdf", MagicMock(), document_hash="h123")
        proc.consume_chunk(chunk_item)
        return proc._buffer[-1]["chunk_id"]

    assert _chunk_id() == _chunk_id()  # identical inputs -> identical id


def test_finalize_flushes_and_does_not_stop(make_proc) -> None:
    proc = make_proc(_chunk_target(flush_batch_size=1000), table_exists=False)
    proc._buffer.append(
        {
            "chunk_id": "c",
            "text": "t",
            "metadata": "{}",
            "doc_id": "d",
            "chunk_index": 0,
        }
    )
    # _finalize sets self._spark = None, so grab the mock first to assert on it.
    spark = proc._spark

    proc._finalize()

    spark.createDataFrame.assert_called_once()  # flush happened
    spark.stop.assert_not_called()  # shared session must not be stopped
