from unittest.mock import MagicMock, patch

import pytest

from docling.datamodel.document import ConversionStatus

from docling_jobkit.connectors.astradb_target_processor import AstraDBTargetProcessor
from docling_jobkit.datamodel.astradb_coords import AstraDBCoordinates
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem
from docling_jobkit.datamodel.task_targets import AstraDBTarget


@pytest.fixture
def coords() -> AstraDBCoordinates:
    return AstraDBCoordinates.model_validate(
        {
            "api_endpoint": "https://abc123.apps.astra.datastax.com",
            "token": "AstraCS:test_token",
            "keyspace": "test_keyspace",
            "collection_name": "test_collection",
            "enable_external_provider": False,
            "external_provider_config": {
                "provider": "ollama",
                "ollama": {
                    "endpoint": "http://localhost:11434",
                    "embedding_model": "nomic-embed-text",
                },
                "openai": {
                    "api_key": "sk-test",
                    "embedding_model": "text-embedding-3-small",
                },
                "watsonx": {
                    "api_key": "wx-test",
                    "endpoint": "https://us-south.ml.cloud.ibm.com",
                    "project_id": "proj-id",
                    "embedding_model": "ibm/granite",
                },
            },
        }
    )


@pytest.fixture
def mock_chunks() -> list[ChunkedDocumentResultItem]:
    return [
        ChunkedDocumentResultItem(
            filename="doc.pdf", chunk_index=0, text="hello world", doc_items=[]
        )
    ]


def _make_initialized_processor(coords: AstraDBCoordinates) -> AstraDBTargetProcessor:
    processor = AstraDBTargetProcessor(coords)
    processor._embedding_model = MagicMock()
    processor._collection = MagicMock()
    processor._chunker_manager = MagicMock()
    processor._initialized = True
    return processor


# factory check


def test_get_config_types_returns_astradb_target(coords: AstraDBCoordinates) -> None:
    """The factory key must be AstraDBTarget so the connector registry resolves it."""
    types = AstraDBTargetProcessor.get_config_types()
    assert AstraDBTarget in types


def test_context_manager_calls_initialize_and_finalize(
    coords: AstraDBCoordinates,
) -> None:
    """__enter__ must run _initialize; __exit__ must run _finalize."""
    processor = AstraDBTargetProcessor(coords)

    with (
        patch.object(processor, "_initialize") as mock_init,
        patch.object(processor, "_finalize") as mock_fin,
    ):
        with processor:
            mock_init.assert_called_once()
            mock_fin.assert_not_called()
        mock_fin.assert_called_once()


def test_finalize_clears_collection_and_chunker(coords: AstraDBCoordinates) -> None:
    """_finalize should set collection and chunker_manager to None."""
    processor = _make_initialized_processor(coords)
    processor._finalize()
    assert processor._collection is None
    assert processor._chunker_manager is None


# upload_chunks


def test_upload_chunks_happy_path(
    coords: AstraDBCoordinates,
    mock_chunks: list[ChunkedDocumentResultItem],
) -> None:
    """upload_chunks should call build_records_from_chunks then insert_records."""
    processor = _make_initialized_processor(coords)
    mock_records = [{"_id": "doc:chunk:0", "$vector": [0.1]}]

    with (
        patch(
            "docling_jobkit.connectors.astradb_helper.build_records_from_chunks",
            return_value=mock_records,
        ) as mock_build,
        patch("docling_jobkit.connectors.astradb_helper.insert_records") as mock_insert,
    ):
        processor.upload_chunks(mock_chunks, doc_id="doc1", source_name="doc.pdf")

    mock_build.assert_called_once_with(
        mock_chunks,
        doc_id="doc1",
        source_name="doc.pdf",
        emb_model=processor._embedding_model,
        emb_kwargs=processor._embedding_kwargs,
        emb_max_tokens=processor._max_tokens,
    )
    mock_insert.assert_called_once_with(
        processor._collection, mock_records, source_name="doc.pdf"
    )


def test_upload_chunks_empty_list_skips_helpers(
    coords: AstraDBCoordinates,
) -> None:
    """upload_chunks with no chunks should return early without calling helpers."""
    processor = _make_initialized_processor(coords)

    with (
        patch(
            "docling_jobkit.connectors.astradb_helper.build_records_from_chunks"
        ) as mock_build,
        patch("docling_jobkit.connectors.astradb_helper.insert_records") as mock_insert,
    ):
        processor.upload_chunks([], doc_id="doc1", source_name="doc.pdf")

    mock_build.assert_not_called()
    mock_insert.assert_not_called()


def test_upload_chunks_before_initialize_raises(coords: AstraDBCoordinates) -> None:
    """Calling upload_chunks without _initialize should raise RuntimeError."""
    processor = AstraDBTargetProcessor(coords)
    with pytest.raises(RuntimeError, match="not initialized"):
        processor.upload_chunks([MagicMock()], doc_id="doc1", source_name="doc.pdf")


# chunk_and_upload


def test_chunk_and_upload_skips_non_exportable_status(
    coords: AstraDBCoordinates,
) -> None:
    """chunk_and_upload should do nothing when the document status is FAILURE."""
    processor = _make_initialized_processor(coords)
    exportable = MagicMock()
    exportable.status = ConversionStatus.FAILURE

    with patch.object(processor, "upload_chunks") as mock_upload:
        processor.chunk_and_upload(exportable)

    mock_upload.assert_not_called()


def test_chunk_and_upload_delegates_to_upload_chunks(
    coords: AstraDBCoordinates,
    mock_chunks: list[ChunkedDocumentResultItem],
) -> None:
    """chunk_and_upload should chunk the document and forward results to upload_chunks."""
    processor = _make_initialized_processor(coords)
    processor._chunker_manager.chunk_document.return_value = iter(mock_chunks)

    exportable = MagicMock()
    exportable.status = ConversionStatus.SUCCESS
    exportable.file.name = "doc.pdf"
    exportable.document.origin.binary_hash = "abc123"

    with patch.object(processor, "upload_chunks") as mock_upload:
        processor.chunk_and_upload(exportable)

    mock_upload.assert_called_once_with(
        mock_chunks, doc_id="abc123", source_name="doc.pdf"
    )


def test_chunk_and_upload_skips_when_document_is_none(
    coords: AstraDBCoordinates,
) -> None:
    """chunk_and_upload returns early when document is None."""
    processor = _make_initialized_processor(coords)
    exportable = MagicMock()
    exportable.status = ConversionStatus.SUCCESS
    exportable.document = None

    with patch.object(processor, "upload_chunks") as mock_upload:
        processor.chunk_and_upload(exportable)

    mock_upload.assert_not_called()


def test_chunk_and_upload_skips_when_no_chunks_produced(
    coords: AstraDBCoordinates,
) -> None:
    """chunk_and_upload returns early without calling upload_chunks when chunking yields nothing."""
    processor = _make_initialized_processor(coords)
    processor._chunker_manager.chunk_document.return_value = iter([])

    exportable = MagicMock()
    exportable.status = ConversionStatus.SUCCESS
    exportable.file.name = "doc.pdf"
    exportable.document.origin.binary_hash = "abc123"

    with patch.object(processor, "upload_chunks") as mock_upload:
        processor.chunk_and_upload(exportable)

    mock_upload.assert_not_called()


# _initialize


def test_initialize_without_external_provider(coords: AstraDBCoordinates) -> None:
    """_initialize with enable_external_provider=False calls get_collection without emb_dim."""
    processor = AstraDBTargetProcessor(coords)
    mock_collection = MagicMock()

    with (
        patch(
            "docling_jobkit.connectors.astradb_helper.get_collection",
            return_value=mock_collection,
        ) as mock_get_coll,
        patch("docling_jobkit.convert.chunking.DocumentChunkerManager"),
    ):
        processor._initialize()

    mock_get_coll.assert_called_once_with(coords)
    assert processor._collection is mock_collection
    assert processor._embedding_model is None


def test_initialize_with_external_provider_missing_config_raises() -> None:
    """When enable_external_provider=True but no external_provider_config, raises RuntimeError."""
    coords_no_config = AstraDBCoordinates.model_validate(
        {
            "api_endpoint": "https://abc123.apps.astra.datastax.com",
            "token": "AstraCS:test_token",
            "keyspace": "test_keyspace",
            "collection_name": "test_collection",
            "enable_external_provider": True,
        }
    )
    processor = AstraDBTargetProcessor(coords_no_config)
    with pytest.raises(RuntimeError, match="external_provider_config is required"):
        processor._initialize()


def test_initialize_with_external_provider_ollama(coords: AstraDBCoordinates) -> None:
    """_initialize with ollama sets the right embedding model, endpoint kwargs, and max_tokens."""
    coords_enabled = coords.model_copy(update={"enable_external_provider": True})
    processor = AstraDBTargetProcessor(coords_enabled)
    mock_collection = MagicMock()

    with (
        patch(
            "docling_jobkit.convert.embedding.generate_text_embedding",
            return_value=[[0.1] * 768],
        ),
        patch(
            "docling_jobkit.connectors.astradb_helper.get_collection",
            return_value=mock_collection,
        ),
        patch("docling_jobkit.convert.chunking.DocumentChunkerManager"),
    ):
        processor._initialize()

    assert processor._embedding_model == "ollama/nomic-embed-text"
    assert processor._embedding_kwargs == {"api_base": "http://localhost:11434"}
    assert processor._max_tokens == 2000
    assert processor._collection is mock_collection
