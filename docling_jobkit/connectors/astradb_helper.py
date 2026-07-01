import logging

from astrapy import Collection
from astrapy.constants import VectorMetric
from astrapy.exceptions import CollectionInsertManyException
from astrapy.info import CollectionDefinition
from sentence_transformers import SentenceTransformer

from docling_jobkit.convert.embedding import EmbeddingError, generate_text_embedding
from docling_jobkit.datamodel.astradb_coords import AstraDBCoordinates
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem

_BATCH_SIZE = 20  # max astra can do in one insert_many


def get_collection(coords: AstraDBCoordinates) -> Collection:
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
    collection = db.create_collection(
        coords.collection_name,
        definition=(
            CollectionDefinition.builder()
            .with_vector_dimension(768)
            .with_vector_metric(VectorMetric.COSINE)
            .build()
        ),
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
    emb_model: SentenceTransformer,
) -> list[dict]:
    """Convert pre-built chunk items into AstraDB insertion records."""
    # one batch chunk text embedding call
    texts = [chunk.text for chunk in chunks]
    try:
        embeddings = generate_text_embedding(emb_model, texts)
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
    falls back to find_one_and_replace (upsert=True) for only the records
    that failed, leaving new records inserted in bulk.
    """
    if not records:
        return

    total_batches = (len(records) + _BATCH_SIZE - 1) // _BATCH_SIZE
    for i in range(0, len(records), _BATCH_SIZE):
        batch = records[i : i + _BATCH_SIZE]
        try:
            collection.insert_many(batch, ordered=False)
        except CollectionInsertManyException as exc:
            inserted = set(exc.inserted_ids)
            for record in batch:
                if record["_id"] not in inserted:
                    collection.find_one_and_replace(
                        {"_id": record["_id"]},
                        record,
                        upsert=True,
                    )
        logging.debug(
            "AstraDB: upserted batch %d/%d (%d records) for '%s'",
            i // _BATCH_SIZE + 1,
            total_batches,
            len(batch),
            source_name,
        )

    logging.info("AstraDB: upserted %d chunks for '%s'", len(records), source_name)
