from unittest.mock import MagicMock, patch

import pytest

from docling_jobkit.connectors.astradb_helper import build_records_from_chunks
from docling_jobkit.convert.embedding import EmbeddingError
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem


@pytest.fixture
def mock_chunks() -> list[ChunkedDocumentResultItem]:
    """mocks the chunks from docling"""
    return [
        ChunkedDocumentResultItem(
            filename="doc.pdf", chunk_index=0, text="hello world", doc_items=[]
        )
    ]


def test_build_records_include_vector(
    mock_chunks: list[ChunkedDocumentResultItem],
) -> None:
    """The final record constructed shoulkd include a vector field"""
    mock_emb_model = MagicMock()
    mock_emb_model.encode.return_value.tolist.return_value = [[0.1, 0.2, 0.3]]

    records = build_records_from_chunks(
        mock_chunks, "doc1", "source.pdf", mock_emb_model
    )

    assert "$vector" in records[0]


def test_build_records_propagates_embedding_error(
    mock_chunks: list[ChunkedDocumentResultItem],
) -> None:
    """embedding failure should raise EmbeddingError"""
    with patch(
        "docling_jobkit.connectors.astradb_helper.generate_text_embedding",
        side_effect=EmbeddingError("fail"),
    ):
        with pytest.raises(EmbeddingError):
            build_records_from_chunks(mock_chunks, "doc1", "source.pdf", MagicMock())
