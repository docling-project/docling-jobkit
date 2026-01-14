from io import BytesIO
from typing import Generator, Iterator, List

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
)

# -------------------------------------------------------------------
# Mock processor that mimics lazy streaming behavior
# -------------------------------------------------------------------


class MockSourceProcessor(BaseSourceProcessor):
    def __init__(self, ids: List[str]):
        super().__init__()
        self._all_ids = ids
        self._list_called = 0

    def _initialize(self):
        pass

    def _finalize(self):
        pass

    # ---- Lazy ID generator (counts how many times it's created) ----
    def _list_document_ids(self) -> Generator[str, None, None]:
        self._list_called += 1
        for x in self._all_ids:
            yield x

    def _count_documents(self) -> int:
        return len(self._all_ids)

    # ---- Simulated fetch ----
    def _fetch_document_by_id(self, doc_id: str) -> DocumentStream:
        return DocumentStream(name=doc_id, stream=BytesIO(b"content"))

    # ---- Only used for full streaming ----
    def _fetch_documents(self) -> Iterator[DocumentStream]:
        for x in self._list_document_ids():
            yield self._fetch_document_by_id(x)


# -------------------------------------------------------------------
# Tests
# -------------------------------------------------------------------


def test_streaming_chunks_consumes_one_generator():
    ids = [f"id_{i}" for i in range(10)]
    chunk_size = 3

    with MockSourceProcessor(ids) as p:
        chunks = list(p.iterate_document_chunks(chunk_size))

        # Ensure chunk size correctness
        assert len(chunks) == 4
        assert chunks[0].ids == ["id_0", "id_1", "id_2"]
        assert chunks[1].ids == ["id_3", "id_4", "id_5"]
        assert chunks[2].ids == ["id_6", "id_7", "id_8"]
        assert chunks[3].ids == ["id_9"]

        # Ensure _list_document_ids was called exactly once
        assert p._list_called == 1


def test_chunks_can_fetch_documents_lazily():
    ids = ["a", "b", "c"]
    total_docs = len(ids)
    with MockSourceProcessor(ids) as p:
        chunks = list(p.iterate_document_chunks(chunk_size=2))
        first_chunk = chunks[0]

        docs = list(first_chunk.iter_documents())

        assert docs[0].name == "a"
        assert docs[1].name == "b"

        # Verify chunk sizes
        total_ids_in_chunks = sum(len(chunk.ids) for chunk in chunks)
        assert total_ids_in_chunks == total_docs, (
            f"Total IDs in chunks ({total_ids_in_chunks}) doesn't match "
            f"total documents ({total_docs})"
        )


def test_chunk_indices_are_sequential():
    """Test that chunks have correct sequential indices."""
    ids = [f"id_{i}" for i in range(7)]
    chunk_size = 2

    with MockSourceProcessor(ids) as p:
        chunks = list(p.iterate_document_chunks(chunk_size))

        # Verify chunk indices are sequential starting from 0
        for i, chunk in enumerate(chunks):
            assert chunk.index == i, f"Chunk at position {i} has index {chunk.index}"

        # Should have 4 chunks (7 docs / 2 per chunk = 3.5 -> 4 chunks)
        assert len(chunks) == 4


def test_chunking_with_edge_case_sizes():
    """Test chunking with various edge case chunk sizes."""
    ids = [f"id_{i}" for i in range(5)]
    total_docs = len(ids)

    with MockSourceProcessor(ids) as p:
        # Test chunk_size = 1 (one document per chunk)
        chunks = list(p.iterate_document_chunks(chunk_size=1))
        assert len(chunks) == total_docs
        for chunk in chunks:
            assert len(chunk.ids) == 1

        # Test chunk_size = total_docs (all in one chunk)
        chunks = list(p.iterate_document_chunks(chunk_size=total_docs))
        assert len(chunks) == 1
        assert len(chunks[0].ids) == total_docs

        # Test chunk_size > total_docs (still one chunk)
        chunks = list(p.iterate_document_chunks(chunk_size=total_docs + 10))
        assert len(chunks) == 1
        assert len(chunks[0].ids) == total_docs
