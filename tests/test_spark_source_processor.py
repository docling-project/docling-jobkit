import logging
from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.errors import SourceConnectorAuthenticationError
from docling_jobkit.connectors.source_processor import DocumentChunk, SourceDocumentRef
from docling_jobkit.connectors.spark import SparkRowID, SparkSourceProcessor
from docling_jobkit.convert.materialization import SourceLimitExceededError
from docling_jobkit.datamodel.spark_coords import TaskSparkSource


def _coords(**overrides) -> TaskSparkSource:
    base = {
        "host": "h",
        "port": 15002,
        "table": "cat.db.docs",
        "content_column": "content",
        "filename_column": "doc_name",
    }
    return TaskSparkSource(**{**base, **overrides})


def _proc(coords, *, stream=None, enumerate_rows=None) -> SparkSourceProcessor:
    """Processor with a mocked backend.

    `stream` feeds stream_documents (single-driver reads); `enumerate_rows`
    feeds enumerate_row_keys (distributed coordinator enumeration).
    """
    proc = SparkSourceProcessor(coords)
    backend = MagicMock()
    if stream is not None:
        backend.stream_documents.return_value = iter(stream)
    if enumerate_rows is not None:
        backend.enumerate_row_keys.return_value = iter(enumerate_rows)
    proc._backend = backend
    proc._cached_partition = None
    proc._partition_cache = {}
    proc._initialized = True
    return proc


def _partition_proc(coords, partition_rows) -> tuple[SparkSourceProcessor, MagicMock]:
    """Distributed processor whose backend.read_partition yields `partition_rows`.
    Returns (proc, backend) so callers can assert read counts."""
    proc = SparkSourceProcessor(coords)
    proc._initialized = True
    proc._cached_partition = None
    proc._partition_cache = {}
    backend = MagicMock()
    backend.read_partition.return_value = iter(partition_rows)
    proc._backend = backend
    return proc, backend


@pytest.mark.parametrize(
    "overrides, expected",
    [({}, False), ({"partition_column": "dt"}, True)],
    ids=["no_partition_column", "with_partition_column"],
)
def test_is_expandable(overrides, expected):
    assert SparkSourceProcessor.is_expandable(_coords(**overrides)) is expected


def test_fetch_yields_documentstreams_with_names_and_bytes():
    proc = _proc(_coords(), stream=[(b"PDF1", "a.pdf"), (b"PDF2", "b.pdf")])
    docs = list(proc._fetch_documents())

    assert all(isinstance(d, DocumentStream) for d in docs)
    assert [d.name for d in docs] == ["a.pdf", "b.pdf"]
    assert docs[0].stream.read() == b"PDF1"


def test_fetch_synthesizes_name_when_no_filename_column():
    proc = _proc(_coords(filename_column=None), stream=[(b"X", None)])
    (doc,) = proc._fetch_documents()

    assert doc.name == "row-0.bin"


def test_fetch_skips_null_content():
    proc = _proc(_coords(), stream=[(None, "empty.pdf"), (b"PDF", "ok.pdf")])
    docs = list(proc._fetch_documents())

    assert [d.name for d in docs] == ["ok.pdf"]  # null row dropped
    assert docs[0].stream.read() == b"PDF"


def test_fetch_rejects_oversized():
    proc = _proc(_coords(), stream=[(b"0123456789", "big.pdf")])
    with pytest.raises(SourceLimitExceededError):
        list(proc._fetch_documents(max_file_size=8))


def test_single_driver_logs_once(caplog):
    proc = _proc(_coords(), stream=[(b"PDF", "a.pdf")])
    with caplog.at_level(logging.INFO):
        list(proc._fetch_documents())

    assert any("partition_column" in r.message for r in caplog.records)


def test_initialize_maps_auth_error(monkeypatch):
    proc = SparkSourceProcessor(_coords())

    def _boom(_conn):
        raise RuntimeError("StatusCode.UNAUTHENTICATED: bad token")

    monkeypatch.setattr(
        "docling_jobkit.connectors.spark.source_processor.get_backend", _boom
    )
    with pytest.raises(SourceConnectorAuthenticationError):
        proc._initialize()


def test_list_document_ids_enumerates_partition_and_hash():
    proc = _proc(
        _coords(partition_column="dt"),
        enumerate_rows=[
            ("2024-01-01", "h1", "a.pdf"),
            ("2024-01-01", "h2", "b.pdf"),
            ("2024-01-02", "h3", None),
        ],
    )
    assert list(proc._list_document_ids()) == [
        ("2024-01-01", "h1", "a.pdf"),
        ("2024-01-01", "h2", "b.pdf"),
        ("2024-01-02", "h3", None),
    ]


def test_make_document_ref_keys_on_partition_and_row_key():
    proc = SparkSourceProcessor(_coords(partition_column="dt"))
    ref = proc._make_document_ref(SparkRowID("2024-01-01", "h1", "a.pdf"), 0)

    assert isinstance(ref, SourceDocumentRef)
    assert ref.id == SparkRowID("2024-01-01", "h1", "a.pdf")
    assert ref.source_uri == "cat.db.docs#2024-01-01"
    assert ref.filename == "a.pdf"


@pytest.mark.parametrize(
    "row, expected_name",
    [
        (("2024-01-01", "h1", "a.pdf"), "a.pdf"),
        (("2024-01-02", "h3", None), "h3.bin"),
    ],
    ids=["explicit_filename", "synthesized_from_row_key"],
)
def test_make_document_ref_filename(row, expected_name):
    proc = SparkSourceProcessor(_coords(partition_column="dt"))

    assert proc._make_document_ref(row, 0).filename == expected_name


def test_iterate_document_chunks_one_per_partition():
    proc = _proc(
        _coords(partition_column="dt"),
        enumerate_rows=[
            ("2024-01-01", "h1", "a.pdf"),
            ("2024-01-01", "h2", "b.pdf"),
            ("2024-01-02", "h3", "c.pdf"),
        ],
    )
    chunks = list(proc.iterate_document_chunks(chunk_size=1000))

    assert all(isinstance(c, DocumentChunk) for c in chunks)
    assert len(chunks) == 2  # two distinct partitions
    assert [len(c.refs) for c in chunks] == [2, 1]


def test_iterate_document_chunks_raises_without_partition_column():
    """multiproc must fail loudly, not degrade silently."""
    proc = _proc(_coords())

    with pytest.raises(Exception, match="partition_column"):
        list(proc.iterate_document_chunks(chunk_size=1000))


def test_fetch_document_by_id_reads_partition_once_and_caches():
    proc, backend = _partition_proc(
        _coords(partition_column="dt"),
        [("h1", b"PDF1", "a.pdf"), ("h2", b"PDF2", "b.pdf")],
    )

    d1 = proc._fetch_document_by_id(SparkRowID("2024-01-01", "h1"))
    assert isinstance(d1, DocumentStream)
    assert d1.name == "a.pdf"
    assert d1.stream.read() == b"PDF1"
    assert proc._cached_partition == "2024-01-01"
    assert backend.read_partition.call_count == 1  # one pruned read

    d2 = proc._fetch_document_by_id(SparkRowID("2024-01-01", "h2"))
    assert d2.stream.read() == b"PDF2"
    assert backend.read_partition.call_count == 1  # cache hit — no re-read


def test_fetch_document_by_id_enforces_max_file_size():
    proc, _ = _partition_proc(
        _coords(partition_column="dt"),
        [("h1", b"0123456789", "big.pdf")],
    )

    with pytest.raises(SourceLimitExceededError):
        proc._fetch_document_by_id(SparkRowID("2024-01-01", "h1"), max_file_size=8)
