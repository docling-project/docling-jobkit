import logging
from pathlib import Path

from docling_jobkit.datamodel.astradb_coords import AstraDBCoordinates

_BATCH_SIZE = 20


def get_collection(coords: AstraDBCoordinates):
    from astrapy import DataAPIClient

    client = DataAPIClient(token=coords.token.get_secret_value())
    db = client.get_database(
        str(coords.api_endpoint),
        keyspace=coords.keyspace,
    )
    # create_collection may be better
    collection = db.get_collection(coords.collection_name)
    logging.info(
        "AstraDB: ready — collection '%s', keyspace '%s'",
        coords.collection_name,
        coords.keyspace,
    )
    return collection


def build_chunk_records(raw: bytes, source_name: str) -> list[dict]:
    """Parse a DoclingDocument from raw JSON, chunk it, and return one dict
    per chunk ready for insertion into AstraDB.

    POC Note:
        The convert pipeline serialised the DoclingDocument to JSON; we parse
        it back here so we can hand it to DocumentChunkerManager. We should
        eliminate this step by receiving the live DoclingDocument object
        directly from process_chunkable_results() before any serialisation.
        Requires larger changes to the CLI.
    """
    from docling_core.types.doc.document import DoclingDocument

    from docling_jobkit.convert.chunking import DocumentChunkerManager
    from docling_jobkit.datamodel.chunking import HybridChunkerOptions

    doc = DoclingDocument.model_validate_json(raw)

    # TODO: add chunking_options on AstraDBCoordinates
    options = HybridChunkerOptions()
    chunks = list(
        DocumentChunkerManager().chunk_document(
            document=doc,
            # filename is used only for the filename field on each chunk item.
            filename=Path(source_name).name,
            options=options,
        )
    )

    if not chunks:
        logging.warning("AstraDB: no chunks produced for '%s'", source_name)
        return []

    doc_id = str(doc.origin.binary_hash) if doc.origin else source_name

    records = []
    for chunk in chunks:
        records.append(
            {
                # TODO: Think more on id generation
                "_id": f"{doc_id}:chunk:{chunk.chunk_index}",
                "doc_id": doc_id,
                "source_name": source_name,
                "filename": chunk.filename,
                "chunk_index": chunk.chunk_index,
                "text": chunk.text,
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
    """Batch-insert records into AstraDB, _BATCH_SIZE at a time."""
    if not records:
        return

    total_batches = (len(records) + _BATCH_SIZE - 1 ) // _BATCH_SIZE
    for i in range(0, len(records), _BATCH_SIZE):
        batch = records[i : i + _BATCH_SIZE]
        collection.insert_many(batch, ordered=False)
        logging.debug(
            "AstraDB: inserted batch %d/%d (%d records) for '%s'",
            i // _BATCH_SIZE + 1,
            total_batches,
            len(batch),
            source_name,
        )

    logging.info(
        "AstraDB: inserted %d chunks for '%s'", len(records), source_name
    )
