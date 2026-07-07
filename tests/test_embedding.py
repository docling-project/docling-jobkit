from unittest.mock import patch

import pytest

from docling_jobkit.convert.embedding import (
    EmbeddingError,
    _batch_texts,
    generate_text_embedding,
)

MODEL = "ollama/nomic-embed-text"

# Note: With the new external embedding provider, to test embeddings we need a shared openai/watsonx api key
# or run ollama in testcontainers


def test_batch_texts_splits_oversized_text() -> None:
    batches = _batch_texts(["hello world"], MODEL, max_tokens=1)

    assert len(batches) > 1
    assert all(len(b) == 1 for b in batches)


@pytest.mark.parametrize(argnames="batch_text", argvalues=["hello world", "goodbye"])
def test_batch_texts_doesnt_split_less_equal(batch_text: str) -> None:
    """It shouldn't split text into multiple batches if text length is less than or equal to max_tokens"""
    batches = _batch_texts([batch_text], MODEL, max_tokens=10)

    assert len(batches) == 1


def test_raises_embedding_error_on_litellm_failure() -> None:
    with patch(
        "docling_jobkit.convert.embedding.litellm.embedding",
        side_effect=Exception("api down"),
    ):
        with pytest.raises(EmbeddingError):
            generate_text_embedding(MODEL, ["hello"])
