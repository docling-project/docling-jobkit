import pytest

from docling.datamodel.service.sources import HttpSource
from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.http.source_processor import HttpSourceProcessor
from docling_jobkit.convert.materialization import SourceLimitExceededError
from docling_jobkit.datamodel.http_inputs import FileSource


def test_http_file_source_chunking():
    """Test chunking functionality with FileSource."""
    # Create a FileSource with base64 encoded content
    import base64

    content = b"Test PDF content"
    base64_content = base64.b64encode(content).decode("utf-8")

    file_source = FileSource(base64_string=base64_content, filename="test.pdf")

    with HttpSourceProcessor(file_source) as processor:
        # Test chunking with single file
        chunks = list(processor.iterate_document_chunks(chunk_size=1))

        assert len(chunks) == 1, "Expected exactly one chunk for single file"
        assert chunks[0].index == 0, "First chunk should have index 0"
        assert len(chunks[0].ids) == 1, "Chunk should contain one identifier"

        # Verify document can be fetched from the chunk refs via the processor
        docs = [processor.fetch_converter_source_by_ref(ref) for ref in chunks[0].refs]
        assert len(docs) == 1
        assert isinstance(docs[0], DocumentStream)
        assert docs[0].name == "test.pdf"

        # Verify content
        fetched_content = docs[0].stream.read()
        assert fetched_content == content


def test_http_file_source_list_and_fetch():
    """Test _list_document_ids and _fetch_document_by_id for FileSource."""
    import base64

    content = b"Another test content"
    base64_content = base64.b64encode(content).decode("utf-8")

    file_source = FileSource(base64_string=base64_content, filename="document.pdf")

    with HttpSourceProcessor(file_source) as processor:
        # Test listing document IDs
        doc_ids = list(processor._list_document_ids())
        assert len(doc_ids) == 1
        assert doc_ids[0].source == file_source
        assert doc_ids[0].size is None
        assert doc_ids[0].etag is None

        # Test fetching by ID
        doc = processor._fetch_document_by_id(doc_ids[0])
        assert isinstance(doc, DocumentStream)
        assert doc.name == "document.pdf"
        assert doc.stream.read() == content


def _stub_head(monkeypatch, *, size=None, etag=None):
    """Avoid real network HEAD probes in _list_document_ids."""
    monkeypatch.setattr(
        HttpSourceProcessor,
        "_try_head_request",
        lambda self, source: (size, etag),
    )


def test_http_source_ref_returns_converter_url_and_headers(monkeypatch):
    _stub_head(monkeypatch)
    http_source = HttpSource(
        url="https://example.com/document.pdf",
        headers={"Authorization": "Bearer token"},
    )

    with HttpSourceProcessor(http_source) as processor:
        chunk = next(processor.iterate_document_chunks(chunk_size=1))
        ref = chunk.refs[0]

        assert processor.fetch_converter_source_by_ref(ref) == (
            "https://example.com/document.pdf"
        )
        assert processor.headers_for_ref(ref) == {"Authorization": "Bearer token"}
        assert ref.source_uri == "https://example.com/document.pdf"


@pytest.mark.parametrize("head_size", [None, 4])
def test_http_source_ref_passthrough_when_within_limit(monkeypatch, head_size):
    """A limit alone must NOT materialize: an unknown or in-limit size passes the
    raw URL through to the converter. Also covers the sys.maxsize sentinel."""
    import sys

    _stub_head(monkeypatch, size=head_size)
    http_source = HttpSource(url="https://example.com/document.pdf")

    with HttpSourceProcessor(http_source) as processor:
        chunk = next(processor.iterate_document_chunks(chunk_size=1))
        ref = chunk.refs[0]
        assert (
            processor.fetch_converter_source_by_ref(ref, max_file_size=8)
            == "https://example.com/document.pdf"
        )
        assert (
            processor.fetch_converter_source_by_ref(ref, max_file_size=sys.maxsize)
            == "https://example.com/document.pdf"
        )


def test_http_source_ref_rejects_from_head_size(monkeypatch):
    """A HEAD-advertised Content-Length over the limit rejects without a fetch."""
    _stub_head(monkeypatch, size=9)
    http_source = HttpSource(url="https://example.com/document.pdf")

    with HttpSourceProcessor(http_source) as processor:
        chunk = next(processor.iterate_document_chunks(chunk_size=1))
        ref = chunk.refs[0]
        with pytest.raises(SourceLimitExceededError, match="max_file_size=8"):
            processor.fetch_converter_source_by_ref(ref, max_file_size=8)


def test_http_source_iterate_documents_not_supported(monkeypatch):
    """HttpSource is passthrough-only; direct byte retrieval is unsupported."""
    _stub_head(monkeypatch)
    http_source = HttpSource(url="https://example.com/document.pdf")

    with HttpSourceProcessor(http_source) as processor:
        with pytest.raises(NotImplementedError):
            list(processor.iterate_documents())


def test_http_file_source_iterate_documents():
    """Test iterate_documents for FileSource."""
    import base64

    content = b"Iterate test content"
    base64_content = base64.b64encode(content).decode("utf-8")

    file_source = FileSource(base64_string=base64_content, filename="iterate.pdf")

    with HttpSourceProcessor(file_source) as processor:
        docs = list(processor.iterate_documents())

        assert len(docs) == 1
        assert isinstance(docs[0], DocumentStream)
        assert docs[0].name == "iterate.pdf"
        assert docs[0].stream.read() == content
