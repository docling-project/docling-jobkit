from sentence_transformers import SentenceTransformer


class EmbeddingError(Exception):
    pass


def generate_text_embedding(
    emb_model: SentenceTransformer, texts: list[str]
) -> list[list[float]]:
    """
    take a list of text inputs and uses an embedding model to generate text embeddings

    Raises:
        EmbeddingError when embedding the batch fails
    """
    try:
        return emb_model.encode(texts).tolist()
    except Exception as e:
        raise EmbeddingError(f"failed to embed {len(texts)} texts") from e
