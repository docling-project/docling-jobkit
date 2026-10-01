import pickle
from io import BytesIO
from typing import Generator, Iterator, List

from pydantic import BaseModel

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    DocumentChunk,
    SourceDocumentRef,
)

# -------------------------------------------------------------------
# Mock processor that mimics lazy streaming behavior
# -------------------------------------------------------------------


class MockSourceProcessor(BaseSourceProcessor):
    def __init__(self, ids: List[str]):
        super().__init__(ids)
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
    def _fetch_document_by_id(
        self, identifier: str, *, max_file_size: int | None = None
    ) -> DocumentStream:
        del max_file_size
        return DocumentStream(name=identifier, stream=BytesIO(b"content"))

    # ---- Only used for full streaming ----
    def _fetch_documents(
        self, *, max_file_size: int | None = None
    ) -> Iterator[DocumentStream]:
        for x in self._list_document_ids():
            yield self._fetch_document_by_id(x, max_file_size=max_file_size)


# -------------------------------------------------------------------
# Tests
# -------------------------------------------------------------------


def test_streaming_chunks_consumes_one_generator():
    ids = [f"id_{i}" for i in range(10)]
    chunk_size = 3

    with MockSourceProcessor(ids) as p:
        assert p.source == ids
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

        # Documents are fetched from the chunk refs via the source processor.
        docs = [p.fetch_converter_source_by_ref(ref) for ref in first_chunk.refs]

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


class _FakeFileIdentifier(BaseModel):
    """Stand-in for a connector's own identifier model (e.g. S3FileIdentifier).

    Picking a BaseModel here (not just `str`) matters: pickling a chunk whose
    refs/source are plain builtins never exercised the bug, since the failure
    is specifically about Pydantic's dynamically-parametrized generic classes
    -- SourceDocumentRef[_FakeFileIdentifier] and
    DocumentChunk[List[str], _FakeFileIdentifier] -- not about the field
    values they carry.
    """

    key: str
    size: int


def test_document_chunk_and_refs_survive_pickling():
    """Regression test for a real production bug (Ray KeyError: 'type').

    SourceDocumentRef[...] and DocumentChunk[...] are dynamically-parametrized
    Pydantic generics, which are not picklable via plain `pickle` by default:
    the class pickle needs to locate by module+qualname was never registered
    as a real module attribute. In production this surfaced when Ray sent a
    DocumentChunk from the coordinator to a converter actor (S3 fan-out):
    cloudpickle on the send side tolerated it, but the pickle5 out-of-band
    receive side fell back to stdlib pickle.loads and raised KeyError: 'type'
    deep inside Ray's own deserialization -- an unrelated-looking symptom of
    the same underlying problem. See _PicklableGenericModel in
    source_processor.py for the fix and the full explanation.
    """
    ref = SourceDocumentRef[_FakeFileIdentifier](
        id=_FakeFileIdentifier(key="docs/report.pdf", size=123),
        source_index=0,
        source_uri="mock://bucket/docs/report.pdf",
        filename="report.pdf",
    )
    chunk = DocumentChunk[List[str], _FakeFileIdentifier](
        source=["docs/report.pdf"],
        refs=[ref],
        chunk_index=0,
    )

    # The actual failure mode: pickle.dumps() itself raised PicklingError
    # before the fix, independent of any cross-process concern.
    restored_ref = pickle.loads(pickle.dumps(ref))
    restored_chunk = pickle.loads(pickle.dumps(chunk))

    assert restored_ref == ref
    assert restored_ref.id == _FakeFileIdentifier(key="docs/report.pdf", size=123)
    assert restored_chunk == chunk
    assert restored_chunk.refs[0].filename == "report.pdf"
    assert restored_chunk.ids == [_FakeFileIdentifier(key="docs/report.pdf", size=123)]


def test_document_chunks_from_real_iteration_survive_pickling():
    """Same bug, via the actual production code path (iterate_document_chunks)
    rather than constructing SourceDocumentRef/DocumentChunk directly."""
    ids = ["a", "b", "c"]
    with MockSourceProcessor(ids) as p:
        chunks = list(p.iterate_document_chunks(chunk_size=2))
        for chunk in chunks:
            restored = pickle.loads(pickle.dumps(chunk))
            assert restored == chunk


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
