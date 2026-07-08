import litellm
from tiktoken import encoding_for_model, get_encoding


class EmbeddingError(Exception):
    pass


def _batch_texts(texts: list[str], model: str, max_tokens: int) -> list[list[str]]:
    """split texts into batches with each batch's total token staying within max_tokens"""
    # tiktoken doesn't recognise ollama/watsonx model names so we fall back to general-purpose encoding
    try:
        enc = encoding_for_model(model)
    except KeyError:
        enc = get_encoding("cl100k_base")

    batches: list[list[str]] = []
    current_batch: list[str] = []
    current_tokens = 0

    for text in texts:
        tokens = enc.encode(text)
        count = len(tokens)

        if count > max_tokens:
            if current_batch:
                batches.append(current_batch)
                current_batch, current_tokens = [], 0
            for i in range(0, len(tokens), max_tokens):
                batches.append([enc.decode(tokens[i : i + max_tokens])])
        elif current_tokens + count > max_tokens:
            batches.append(current_batch)
            current_batch, current_tokens = [text], count
        else:
            current_batch.append(text)
            current_tokens += count

    if current_batch:
        batches.append(current_batch)
    return batches


def generate_text_embedding(
    model: str, texts: list[str], max_tokens: int | None = None, **kwargs
) -> list[list[float]]:
    """
    Embed texts using litellm. When max_tokens is set, batches calls so no
    single request exceeds the model's token limit.

    Raises:
        EmbeddingError when embedding the batch fails
    """
    try:
        batches = _batch_texts(texts, model, max_tokens) if max_tokens else [texts]
        embeddings: list[list[float]] = []
        for batch in batches:
            resp = litellm.embedding(model=model, input=batch, **kwargs)
            # ollama returns raw dicts while openai returns objects
            embeddings.extend(
                d["embedding"] if isinstance(d, dict) else d.embedding
                for d in resp.data
            )
        return embeddings
    except Exception as e:
        raise EmbeddingError(f"failed to embed {len(texts)} texts") from e
