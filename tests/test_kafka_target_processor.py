from pathlib import Path
from unittest.mock import Mock

import pytest

from docling_jobkit.connectors.kafka.models import KafkaChunkTarget
from docling_jobkit.connectors.kafka.target_processor import KafkaTargetProcessor
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem
from docling_jobkit.public_errors import TargetWriteError


@pytest.fixture
def kafka_target():
    return KafkaChunkTarget(
        bootstrap_servers=["localhost:9092"],
        topic="test.chunks",
        key_mode="doc_id",
        acks="all",
    )


@pytest.fixture
def mock_producer(monkeypatch):
    """Mock confluent_kafka.Producer."""
    mock_producer_instance = Mock()
    mock_producer_instance.produce = Mock()
    mock_producer_instance.poll = Mock()
    mock_producer_instance.flush = Mock(return_value=0)

    mock_producer_cls = Mock(return_value=mock_producer_instance)

    import sys
    from unittest.mock import MagicMock

    # Mock the confluent_kafka module
    mock_confluent_kafka = MagicMock()
    mock_confluent_kafka.Producer = mock_producer_cls
    sys.modules["confluent_kafka"] = mock_confluent_kafka

    yield {
        "producer_cls": mock_producer_cls,
        "producer": mock_producer_instance,
    }


def test_check_dependencies_present():
    KafkaTargetProcessor.check_dependencies()


def test_get_config_types():
    config_types = KafkaTargetProcessor.get_config_types()
    assert len(config_types) == 1
    assert config_types[0] is KafkaChunkTarget


def test_initialization(kafka_target, mock_producer):
    processor = KafkaTargetProcessor(kafka_target)

    with processor:
        # Producer should be initialized
        mock_producer["producer_cls"].assert_called_once()
        config = mock_producer["producer_cls"].call_args[0][0]
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["acks"] == "all"


def test_instance_requires_chunks(kafka_target):
    processor = KafkaTargetProcessor(kafka_target)
    assert processor.instance_requires_chunks() is True


def test_consume_chunk_doc_id_key(kafka_target, mock_producer):
    processor = KafkaTargetProcessor(kafka_target)
    chunk = ChunkedDocumentResultItem(
        filename="test.pdf",
        chunk_index=0,
        text="test chunk text",
        doc_items=["#/texts/0"],
        metadata={},
    )

    with processor:
        processor.begin_chunks(
            filename="test.pdf",
            temp_dir=Path("/tmp"),
            document_hash="abc123",
        )
        processor.consume_chunk(chunk)

        # Check produce was called with doc_id as key
        mock_producer["producer"].produce.assert_called_once()
        call_kwargs = mock_producer["producer"].produce.call_args[1]
        assert call_kwargs["topic"] == "test.chunks"
        assert call_kwargs["key"] == b"test.pdf"
        assert b"test chunk text" in call_kwargs["value"]


def test_consume_chunk_chunk_id_key(mock_producer):
    target = KafkaChunkTarget(
        bootstrap_servers=["localhost:9092"],
        topic="test.chunks",
        key_mode="chunk_id",
    )
    processor = KafkaTargetProcessor(target)
    chunk = ChunkedDocumentResultItem(
        filename="test.pdf",
        chunk_index=0,
        text="test chunk text",
        doc_items=["#/texts/0"],
        metadata={},
    )

    with processor:
        processor.begin_chunks(
            filename="test.pdf",
            temp_dir=Path("/tmp"),
            document_hash="abc123",
        )
        processor.consume_chunk(chunk)

        # Check produce was called with chunk_id as key (SHA-256 hash)
        call_kwargs = mock_producer["producer"].produce.call_args[1]
        key = call_kwargs["key"].decode("utf-8")
        assert len(key) == 64  # SHA-256 hex digest length


def test_consume_chunk_no_key(mock_producer):
    target = KafkaChunkTarget(
        bootstrap_servers=["localhost:9092"],
        topic="test.chunks",
        key_mode="none",
    )
    processor = KafkaTargetProcessor(target)
    chunk = ChunkedDocumentResultItem(
        filename="test.pdf",
        chunk_index=0,
        text="test chunk text",
        doc_items=["#/texts/0"],
        metadata={},
    )

    with processor:
        processor.begin_chunks(
            filename="test.pdf",
            temp_dir=Path("/tmp"),
            document_hash="abc123",
        )
        processor.consume_chunk(chunk)

        # Check produce was called with no key
        call_kwargs = mock_producer["producer"].produce.call_args[1]
        assert call_kwargs["key"] is None


def test_end_chunks_flush_timeout(kafka_target, mock_producer):
    # Mock flush to return non-zero (timeout)
    mock_producer["producer"].flush.return_value = 5

    processor = KafkaTargetProcessor(kafka_target)
    chunk = ChunkedDocumentResultItem(
        filename="test.pdf",
        chunk_index=0,
        text="test chunk text",
        doc_items=["#/texts/0"],
        metadata={},
    )

    with pytest.raises(TargetWriteError, match="flush timeout"):
        with processor:
            processor.begin_chunks(
                filename="test.pdf",
                temp_dir=Path("/tmp"),
                document_hash="abc123",
            )
            processor.consume_chunk(chunk)
            processor.end_chunks()


def test_end_chunks_delivery_error(kafka_target, mock_producer):
    processor = KafkaTargetProcessor(kafka_target)
    chunk = ChunkedDocumentResultItem(
        filename="test.pdf",
        chunk_index=0,
        text="test chunk text",
        doc_items=["#/texts/0"],
        metadata={},
    )

    with pytest.raises(TargetWriteError, match="delivery failed"):
        with processor:
            processor.begin_chunks(
                filename="test.pdf",
                temp_dir=Path("/tmp"),
                document_hash="abc123",
            )
            processor.consume_chunk(chunk)

            # Simulate delivery error
            processor._delivery_errors.append(Exception("broker down"))

            processor.end_chunks()


def test_buffer_error_retry(kafka_target, mock_producer):
    # First produce raises BufferError, second succeeds
    mock_producer["producer"].produce.side_effect = [BufferError(), None]

    processor = KafkaTargetProcessor(kafka_target)
    chunk = ChunkedDocumentResultItem(
        filename="test.pdf",
        chunk_index=0,
        text="test chunk text",
        doc_items=["#/texts/0"],
        metadata={},
    )

    with processor:
        processor.begin_chunks(
            filename="test.pdf",
            temp_dir=Path("/tmp"),
            document_hash="abc123",
        )
        processor.consume_chunk(chunk)

        # Should have called produce twice (initial + retry)
        assert mock_producer["producer"].produce.call_count == 2


def test_buffer_error_exhausted(kafka_target, mock_producer):
    # Both attempts raise BufferError
    mock_producer["producer"].produce.side_effect = BufferError()

    processor = KafkaTargetProcessor(kafka_target)
    chunk = ChunkedDocumentResultItem(
        filename="test.pdf",
        chunk_index=0,
        text="test chunk text",
        doc_items=["#/texts/0"],
        metadata={},
    )

    with pytest.raises(TargetWriteError, match="queue full after retry"):
        with processor:
            processor.begin_chunks(
                filename="test.pdf",
                temp_dir=Path("/tmp"),
                document_hash="abc123",
            )
            processor.consume_chunk(chunk)
