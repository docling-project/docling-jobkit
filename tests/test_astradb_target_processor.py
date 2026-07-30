from unittest.mock import Mock, patch

import pytest

from docling_jobkit.connectors.astradb.models import AstraDBChunkTarget
from docling_jobkit.connectors.astradb.target_processor import (
    AstraDBTargetProcessor,
)
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem
from docling_jobkit.public_errors import TargetWriteError


@pytest.fixture
def astradb_target():
    return AstraDBChunkTarget(
        api_endpoint="https://test-db.apps.astra.datastax.com",
        token="AstraCS:test_token",
        keyspace="test_keyspace",
        collection_name="test_collection",
        vectorize_provider="openai",
        vectorize_model="text-embedding-3-small",
    )


@pytest.fixture
def mock_astrapy(monkeypatch):
    import astrapy
    import astrapy.info

    mock_client = Mock()
    mock_db = Mock()
    mock_collection = Mock()

    mock_client.get_database.return_value = mock_db
    mock_client.close = Mock()
    mock_db.create_collection.return_value = mock_collection

    mock_client_cls = Mock(return_value=mock_client)
    mock_vectorize_cls = Mock()

    monkeypatch.setattr(astrapy, "DataAPIClient", mock_client_cls)
    monkeypatch.setattr(
        astrapy.info, "CollectionVectorServiceOptions", mock_vectorize_cls
    )

    yield {
        "client_cls": mock_client_cls,
        "client": mock_client,
        "db": mock_db,
        "collection": mock_collection,
        "vectorize_cls": mock_vectorize_cls,
    }


def test_check_dependencies_present():
    AstraDBTargetProcessor.check_dependencies()


def test_get_config_types():
    config_types = AstraDBTargetProcessor.get_config_types()
    assert len(config_types) == 1
    assert config_types[0] is AstraDBChunkTarget


def test_initialization(astradb_target, mock_astrapy):
    processor = AstraDBTargetProcessor(astradb_target)

    with processor:
        mock_astrapy["client_cls"].assert_called_once()

        mock_astrapy["client"].get_database.assert_called_once_with(
            str(astradb_target.api_endpoint),
            keyspace=astradb_target.keyspace,
        )

        mock_astrapy["db"].create_collection.assert_called_once()
        call_args = mock_astrapy["db"].create_collection.call_args
        assert call_args[0][0] == astradb_target.collection_name
        assert call_args[1]["check_exists"] is True


def test_initialization_failure(astradb_target, mock_astrapy):
    mock_astrapy["client"].get_database.side_effect = Exception("Connection failed")

    processor = AstraDBTargetProcessor(astradb_target)

    with pytest.raises(TargetWriteError, match="Could not connect to AstraDB"):
        with processor:
            pass


def test_instance_requires_chunks(astradb_target):
    processor = AstraDBTargetProcessor(astradb_target)
    assert processor.instance_requires_chunks() is True


def test_stable_chunk_id():
    # Same inputs produce same ID
    chunk_id_1 = AstraDBTargetProcessor._stable_chunk_id(
        binary_hash="abc123",
        filename="test.pdf",
        chunk_index=0,
    )
    chunk_id_2 = AstraDBTargetProcessor._stable_chunk_id(
        binary_hash="abc123",
        filename="test.pdf",
        chunk_index=0,
    )
    assert chunk_id_1 == chunk_id_2

    # Falls back to filename when binary_hash is None
    chunk_id_3 = AstraDBTargetProcessor._stable_chunk_id(
        binary_hash=None,
        filename="test.pdf",
        chunk_index=0,
    )
    assert chunk_id_3 != chunk_id_1

    # Different chunk index produces different ID
    chunk_id_4 = AstraDBTargetProcessor._stable_chunk_id(
        binary_hash="abc123",
        filename="test.pdf",
        chunk_index=1,
    )
    assert chunk_id_4 != chunk_id_1

    assert len(chunk_id_1) == 64
    assert all(c in "0123456789abcdef" for c in chunk_id_1)


def test_chunk_streaming_protocol(astradb_target, mock_astrapy):
    processor = AstraDBTargetProcessor(astradb_target)

    with processor:
        processor.begin_chunks(
            filename="test.pdf",
            temp_dir=Mock(),
            document_hash="test_hash_123",
        )
        assert processor._current_document_hash == "test_hash_123"

        chunk = ChunkedDocumentResultItem(
            filename="test.pdf",
            chunk_index=0,
            text="This is test chunk text",
            headings=["Section 1"],
            page_numbers=[1, 2],
            doc_items=["#/texts/0"],
            metadata={"test": "value"},
        )

        with patch(
            "docling_jobkit.connectors.astradb.helper.upsert_record_with_retry"
        ) as mock_upsert:
            processor.consume_chunk(chunk)

            mock_upsert.assert_called_once()
            call_args = mock_upsert.call_args

            record = call_args[1]["record"]
            assert "_id" in record
            assert record["text"] == "This is test chunk text"
            assert record["metadata"] == {"test": "value"}
            assert record["doc_id"] == "test.pdf"
            assert record["chunk_index"] == 0

        processor.end_chunks()
        assert processor._current_document_hash is None


def test_abort_chunks(astradb_target, mock_astrapy):
    processor = AstraDBTargetProcessor(astradb_target)

    with processor:
        processor.begin_chunks(
            filename="test.pdf",
            temp_dir=Mock(),
            document_hash="test_hash",
        )
        assert processor._current_document_hash == "test_hash"

        processor.abort_chunks()
        assert processor._current_document_hash is None


def test_upsert_row_not_implemented(astradb_target, mock_astrapy):
    processor = AstraDBTargetProcessor(astradb_target)

    with processor:
        with pytest.raises(NotImplementedError, match="only supports chunks"):
            processor.upsert_row({"test": "data"})


def test_consume_chunk_with_retry_failure(astradb_target, mock_astrapy):
    processor = AstraDBTargetProcessor(astradb_target)

    chunk = ChunkedDocumentResultItem(
        filename="test.pdf",
        chunk_index=0,
        text="Test text",
        headings=[],
        page_numbers=[],
        doc_items=[],
        metadata={},
    )

    with processor:
        processor.begin_chunks("test.pdf", Mock(), document_hash="test_hash")

        with patch(
            "docling_jobkit.connectors.astradb.helper.upsert_record_with_retry"
        ) as mock_upsert:
            mock_upsert.side_effect = Exception("Upsert failed")

            with pytest.raises(TargetWriteError, match="Failed to upsert chunk"):
                processor.consume_chunk(chunk)
