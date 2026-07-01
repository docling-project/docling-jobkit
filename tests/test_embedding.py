import pytest
from sentence_transformers import SentenceTransformer

from docling_jobkit.convert.embedding import EmbeddingError, generate_text_embedding


@pytest.fixture(scope="module")
def emb_model() -> SentenceTransformer:
    """creates the mbedding model to be used across all tests in this file"""
    return SentenceTransformer(
        "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"
    )


def test_embeds_single_text(emb_model: SentenceTransformer) -> None:
    """checks that one input text results in one result and its a float (vector) embedding"""
    result = generate_text_embedding(emb_model, ["hello world"])

    assert len(result) == 1
    assert all(isinstance(emb, float) for emb in result[0])


def test_embeds_multiple_text(emb_model: SentenceTransformer) -> None:
    """checks that multiple input text results in correct number of (vector) embeddings"""
    result = generate_text_embedding(emb_model, ["hello world", "goodbye world"])

    assert len(result) == 2
    assert all(isinstance(emb, float) for emb in result[0])
    assert all(isinstance(emb, float) for emb in result[1])


@pytest.mark.parametrize("bad_input", [[None], [0]])
def test_raises_embedding_error_on_bad_input(
    emb_model: SentenceTransformer, bad_input: list[int | None]
) -> None:
    """bad inputs should raise an embedding error"""
    with pytest.raises(EmbeddingError):
        generate_text_embedding(emb_model, bad_input)
