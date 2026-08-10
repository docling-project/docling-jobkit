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
    """Build a processor with its backend mocked out."""

    def _make(target) -> SparkTargetProcessor:
        proc = SparkTargetProcessor(target)
        proc._backend = MagicMock()
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
    # Delta default, flush_batch_size=1 -> flush on the single row.
    proc = make_proc(_doc_target(flush_batch_size=1))

    proc.begin_document("a.pdf")
    proc._pending_row["text"] = "hello"  # simulate accumulated MARKDOWN
    proc.end_document("a.pdf")

    proc._backend.write_rows.assert_called_once()  # flushed at batch size 1
    call = proc._backend.write_rows.call_args
    rows = call.args[3]  # write_rows(table, columns, int_columns, rows, *, ...)
    assert rows[0]["doc_id"] == "a.pdf"
    assert rows[0]["text"] == "hello"
    assert call.kwargs["key"] == "doc_id"
    assert call.kwargs["table_format"] == "delta"


def test_doc_target_passes_table_format_through(make_proc) -> None:
    # The delta-vs-append decision lives in the backend; the processor just
    # forwards the configured table_format.
    proc = make_proc(_doc_target(table_format="parquet", flush_batch_size=1))

    proc.begin_document("a.pdf")
    proc._pending_row["text"] = "hello"
    proc.end_document("a.pdf")

    assert proc._backend.write_rows.call_args.kwargs["table_format"] == "parquet"


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


def test_chunk_target_document_bracket_writes_no_row(make_proc) -> None:
    """to verify that it doesnt write an extra row for doc bracket"""
    proc = make_proc(_chunk_target(flush_batch_size=1000))

    proc.begin_document("deadbeefhash")
    proc.end_document("deadbeefhash")

    assert proc._buffer == []


def test_chunk_id_is_deterministic_across_instances(make_proc, chunk_item) -> None:
    def _chunk_id() -> str:
        proc = make_proc(_chunk_target(flush_batch_size=1000))
        proc.begin_chunks("test.pdf", MagicMock(), document_hash="h123")
        proc.consume_chunk(chunk_item)
        return proc._buffer[-1]["chunk_id"]

    assert _chunk_id() == _chunk_id()  # identical inputs -> identical id


def test_finalize_flushes_buffer(make_proc) -> None:
    proc = make_proc(_chunk_target(flush_batch_size=1000))
    proc._buffer.append(
        {
            "chunk_id": "c",
            "text": "t",
            "metadata": "{}",
            "doc_id": "d",
            "chunk_index": 0,
        }
    )
    # _finalize sets self._backend = None, so grab the mock first to assert on it.
    backend = proc._backend

    proc._finalize()

    backend.write_rows.assert_called_once()  # flush happened
    assert proc._backend is None
