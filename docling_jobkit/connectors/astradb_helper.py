import logging
import time
from typing import Callable, TypeVar

import httpx
from astrapy import Collection
from astrapy.constants import VectorMetric
from astrapy.exceptions import (
    CollectionInsertManyException,
    DataAPIHttpException,
    DataAPITimeoutException,
)
from astrapy.info import CollectionDefinition

from docling_jobkit.convert.embedding import EmbeddingError, generate_text_embedding
from docling_jobkit.datamodel.astradb_coords import AstraDBCoordinates
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem

_BATCH_SIZE = 20  # max astra can do in one insert_many
_MAX_RETRIES = 3
_RETRYABLE_4XX_STATUS = frozenset(
    {408, 429}
)  # 429 rate limit, 408 request timeout are transient 4xx errors
_BACKOFF_BASE = 1.0  # min time that exponential backoff waits

T = TypeVar("T")


# I dont think we need to add jitter/randomness for the cli case but maybe for distributed version
# TODO: add logic to extract retry after header sent in http header by external api on 429 telling us
# after how much time the rate limit will be dropped and we are good to retry
def _with_exponential_retry(fn: Callable[[], T], operation: str) -> T:
    """helper for exponential retries on transient errors"""
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            return fn()
        except DataAPITimeoutException as exc:  # Transient error: network/conn timeout
            last_exc = exc
        except DataAPIHttpException as exc:
            status = exc.httpx_error.response.status_code
            if (
                status < 500 and status not in _RETRYABLE_4XX_STATUS
            ):  # 4xx errors are permanent so we bail immediately
                raise
            last_exc = exc
        except (
            httpx.TransportError
        ) as exc:  # connection refused / DNS / reset / protocol transient errors
            last_exc = exc
        if attempt < _MAX_RETRIES:
            wait = _BACKOFF_BASE * (2**attempt)
            logging.warning(
                "AstraDB: %s transient error, retry %d/%d in %.1fs",
                operation,
                attempt + 1,
                _MAX_RETRIES,
                wait,
            )
            time.sleep(wait)

    logging.error("AstraDB: %s failed after %d attempts", operation, _MAX_RETRIES + 1)
    raise last_exc  # type: ignore[misc]


def get_collection(coords: AstraDBCoordinates, emb_dim: int) -> Collection:
    """
    fetch the collection from AstraDB to save the document chunks to
    If the specified collection doesn't exist, it will create the collection on AstraDB
    """
    from astrapy import DataAPIClient

    client = DataAPIClient(token=coords.token.get_secret_value())
    db = client.get_database(
        str(coords.api_endpoint),
        keyspace=coords.keyspace,
    )

    # idempotent, if collection exists it will just return status ok
    collection = _with_exponential_retry(
        lambda: db.create_collection(
            coords.collection_name,
            definition=(
                CollectionDefinition.builder()
                .with_vector_dimension(emb_dim)
                .with_vector_metric(VectorMetric.COSINE)
                .build()
            ),
        ),
        "create_collection",
    )

    logging.info(
        "AstraDB: ready — collection '%s', keyspace '%s'",
        coords.collection_name,
        coords.keyspace,
    )
    return collection


def build_records_from_chunks(
    chunks: list[ChunkedDocumentResultItem],
    doc_id: str,
    source_name: str,
    emb_model: str,
    emb_kwargs: dict,
    emb_max_tokens: int | None = None,
) -> list[dict]:
    """Convert pre-built chunk items into AstraDB insertion records."""
    # one batch chunk text embedding call
    texts = [chunk.text for chunk in chunks]
    try:
        embeddings = generate_text_embedding(
            emb_model, texts, emb_max_tokens, **emb_kwargs
        )
    except EmbeddingError:
        # since embeddings are required before writing to AstraDB, fail entire document
        # instead of inserting chunks with no embeddings
        logging.exception("AstraDB: embedding failed for '%s'", source_name)
        raise

    records = []
    for i, chunk in enumerate(chunks):
        records.append(
            {
                # TODO: Think more on id generation
                "_id": f"{doc_id}:chunk:{chunk.chunk_index}",
                "doc_id": doc_id,
                "source_name": source_name,
                "filename": chunk.filename,
                "chunk_index": chunk.chunk_index,
                "text": chunk.text,
                "$vector": embeddings[i],
                "num_tokens": chunk.num_tokens,
                "headings": chunk.headings or [],
                "captions": chunk.captions or [],
                "page_numbers": chunk.page_numbers or [],
                "doc_items": chunk.doc_items or [],
                "metadata": chunk.metadata or {},
            }
        )
    return records


def insert_records(collection, records: list[dict], source_name: str) -> None:
    """Upsert records into AstraDB, _BATCH_SIZE at a time.

    Optimistically attempts insert_many first. On duplicate _id conflicts,
    falls back to update_one (upsert=True) for only the records
    that failed, leaving new records inserted in bulk.
    """
    if not records:
        return

    total_batches = (len(records) + _BATCH_SIZE - 1) // _BATCH_SIZE
    for i in range(0, len(records), _BATCH_SIZE):
        batch = records[i : i + _BATCH_SIZE]
        try:
            _with_exponential_retry(
                lambda: collection.insert_many(batch, ordered=False), "insert_many"
            )
        except CollectionInsertManyException as exc:
            inserted = set(exc.inserted_ids)
            for record in batch:
                if record["_id"] not in inserted:
                    _with_exponential_retry(
                        lambda: collection.update_one(
                            {"_id": record["_id"]},
                            {
                                "$set": {k: v for k, v in record.items() if k != "_id"}
                            },  # can't update stable-id
                            upsert=True,
                        ),
                        "update_one",
                    )
        logging.debug(
            "AstraDB: upserted batch %d/%d (%d records) for '%s'",
            i // _BATCH_SIZE + 1,
            total_batches,
            len(batch),
            source_name,
        )

    logging.info("AstraDB: upserted %d chunks for '%s'", len(records), source_name)
