from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.http_source_processor import HttpSourceProcessor
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

        # Verify document can be fetched from chunk
        docs = list(chunks[0].iter_documents())
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
        assert doc_ids[0]["index"] == 0
        assert doc_ids[0]["source"] == file_source

        # Test fetching by ID
        doc = processor._fetch_document_by_id(doc_ids[0])
        assert isinstance(doc, DocumentStream)
        assert doc.name == "document.pdf"
        assert doc.stream.read() == content


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


# Made with Bob
