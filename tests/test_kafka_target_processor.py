import sys
import types
from pathlib import Path
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from docling_jobkit.connectors.kafka.helper import build_producer_config
from docling_jobkit.connectors.kafka.models import KafkaChunkTarget, KafkaSaslAuth
from docling_jobkit.connectors.kafka.target_processor import KafkaTargetProcessor
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem
from docling_jobkit.public_errors import TargetWriteError


class FakeKafkaException(Exception):
    """Stand-in for confluent_kafka.KafkaException."""


@pytest.fixture
def kafka_target():
    return KafkaChunkTarget(
        bootstrap_servers=["localhost:9092"],
        topic="test.chunks",
        key_mode="doc_id",
        acks="all",
    )


@pytest.fixture
def sample_chunk():
    return ChunkedDocumentResultItem(
        filename="test.pdf",
        chunk_index=0,
        text="test chunk text",
        doc_items=["#/texts/0"],
        metadata={},
    )


@pytest.fixture
def mock_producer(monkeypatch):
    """Install a fake ``confluent_kafka`` module for the duration of one test.

    ``monkeypatch.setitem`` restores whatever was in ``sys.modules`` before —
    including nothing at all on a checkout without the ``kafka`` extra — so the
    fake never leaks into later tests.
    """
    mock_producer_instance = Mock()
    mock_producer_instance.produce = Mock()
    mock_producer_instance.poll = Mock()
    mock_producer_instance.flush = Mock(return_value=0)

    mock_producer_cls = Mock(return_value=mock_producer_instance)

    fake_confluent_kafka = types.ModuleType("confluent_kafka")
    fake_confluent_kafka.Producer = mock_producer_cls  # type: ignore[attr-defined]
    fake_confluent_kafka.KafkaException = FakeKafkaException  # type: ignore[attr-defined]

    # _verify_topic() does `from confluent_kafka.admin import AdminClient`; the
    # fake AdminClient returns a cluster-metadata mock with a non-empty topics dict
    # so the topic-exists check passes without a real broker.
    fake_topic_meta = Mock()
    fake_topic_meta.error = None
    fake_cluster_meta = Mock()
    fake_cluster_meta.topics = {"test.chunks": fake_topic_meta}
    fake_admin_instance = Mock()
    fake_admin_instance.list_topics = Mock(return_value=fake_cluster_meta)
    fake_admin_cls = Mock(return_value=fake_admin_instance)

    fake_admin_mod = types.ModuleType("confluent_kafka.admin")
    fake_admin_mod.AdminClient = fake_admin_cls  # type: ignore[attr-defined]
    fake_confluent_kafka.admin = fake_admin_mod  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "confluent_kafka", fake_confluent_kafka)
    monkeypatch.setitem(sys.modules, "confluent_kafka.admin", fake_admin_mod)

    yield {
        "producer_cls": mock_producer_cls,
        "producer": mock_producer_instance,
    }


def test_check_dependencies_present():
    pytest.importorskip("confluent_kafka", reason="requires the 'kafka' extra")
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


# Producer configuration


def test_producer_config_bounds_the_queue(kafka_target):
    config = build_producer_config(kafka_target)

    # librdkafka's own defaults are 1 GiB / 100 000 messages of native,
    # client-side memory; these must be bounded well below that.
    assert config["queue.buffering.max.kbytes"] == 65536
    assert config["queue.buffering.max.messages"] == 10_000
    assert config["compression.type"] == "lz4"
    assert config["client.id"] == "docling-jobkit"
    assert config["security.protocol"] == "PLAINTEXT"
    # key_mode='doc_id' only orders chunks if the producer is idempotent.
    assert config["enable.idempotence"] is True


def test_producer_config_honours_queue_overrides():
    target = KafkaChunkTarget(
        bootstrap_servers=["localhost:9092"],
        topic="test.chunks",
        queue_max_kbytes=1024,
        queue_max_messages=50,
        compression_type="zstd",
    )
    config = build_producer_config(target)

    assert config["queue.buffering.max.kbytes"] == 1024
    assert config["queue.buffering.max.messages"] == 50
    assert config["compression.type"] == "zstd"


@pytest.mark.parametrize("acks", ["0", "1"])
def test_producer_config_drops_idempotence_for_weak_acks(acks):
    # librdkafka refuses enable.idempotence together with acks != 'all'.
    target = KafkaChunkTarget(
        bootstrap_servers=["localhost:9092"],
        topic="test.chunks",
        acks=acks,
    )
    config = build_producer_config(target)

    assert "enable.idempotence" not in config


@pytest.mark.parametrize(("raw", "expected"), [(1, "1"), (0, "0"), ("all", "all")])
def test_acks_accepts_yaml_integers(raw, expected):
    # YAML parses `acks: 1` as an int; pydantic does not coerce it to a string
    # Literal on its own.
    target = KafkaChunkTarget(
        bootstrap_servers=["localhost:9092"],
        topic="test.chunks",
        acks=raw,
    )
    assert target.acks == expected


def test_sasl_defaults_to_sasl_ssl():
    target = KafkaChunkTarget(
        bootstrap_servers=["localhost:9092"],
        topic="test.chunks",
        auth=KafkaSaslAuth(username="u", password="p"),
    )
    config = build_producer_config(target)

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.username"] == "u"
    assert config["sasl.password"] == "p"
    assert config["enable.ssl.certificate.verification"] is True


def test_sasl_plaintext_is_expressible():
    target = KafkaChunkTarget(
        bootstrap_servers=["localhost:9092"],
        topic="test.chunks",
        security_protocol="SASL_PLAINTEXT",
        auth=KafkaSaslAuth(mechanism="SCRAM-SHA-512", username="u", password="p"),
    )
    config = build_producer_config(target)

    assert config["security.protocol"] == "SASL_PLAINTEXT"
    assert config["sasl.mechanism"] == "SCRAM-SHA-512"
    # Not an SSL protocol, so no TLS verification knob is emitted.
    assert "enable.ssl.certificate.verification" not in config


def test_sasl_protocol_without_auth_is_rejected():
    with pytest.raises(ValidationError, match="requires an 'auth' block"):
        KafkaChunkTarget(
            bootstrap_servers=["localhost:9092"],
            topic="test.chunks",
            security_protocol="SASL_SSL",
        )


def test_auth_rejects_unknown_keys():
    # A misspelled key must fail at config time, not as a connection timeout
    # minutes into a job.
    with pytest.raises(ValidationError):
        KafkaSaslAuth(username="u", password="p", mechanisms="PLAIN")


# Message production


def test_consume_chunk_doc_id_key(kafka_target, mock_producer, sample_chunk):
    processor = KafkaTargetProcessor(kafka_target)

    with processor:
        processor.begin_chunks(
            filename="test.pdf",
            temp_dir=Path("/tmp"),
            document_hash="abc123",
        )
        processor.consume_chunk(sample_chunk)

        # Check produce was called with doc_id as key
        mock_producer["producer"].produce.assert_called_once()
        call_kwargs = mock_producer["producer"].produce.call_args[1]
        assert call_kwargs["topic"] == "test.chunks"
        assert call_kwargs["key"] == b"test.pdf"
        assert b"test chunk text" in call_kwargs["value"]


def test_consume_chunk_chunk_id_key(mock_producer, sample_chunk):
    target = KafkaChunkTarget(
        bootstrap_servers=["localhost:9092"],
        topic="test.chunks",
        key_mode="chunk_id",
    )
    processor = KafkaTargetProcessor(target)

    with processor:
        processor.begin_chunks(
            filename="test.pdf",
            temp_dir=Path("/tmp"),
            document_hash="abc123",
        )
        processor.consume_chunk(sample_chunk)

        # Check produce was called with chunk_id as key (SHA-256 hash)
        call_kwargs = mock_producer["producer"].produce.call_args[1]
        key = call_kwargs["key"].decode("utf-8")
        assert len(key) == 64  # SHA-256 hex digest length


def test_consume_chunk_no_key(mock_producer, sample_chunk):
    target = KafkaChunkTarget(
        bootstrap_servers=["localhost:9092"],
        topic="test.chunks",
        key_mode="none",
    )
    processor = KafkaTargetProcessor(target)

    with processor:
        processor.begin_chunks(
            filename="test.pdf",
            temp_dir=Path("/tmp"),
            document_hash="abc123",
        )
        processor.consume_chunk(sample_chunk)

        # Check produce was called with no key
        call_kwargs = mock_producer["producer"].produce.call_args[1]
        assert call_kwargs["key"] is None


def test_chunk_id_is_in_the_message_value(kafka_target, mock_producer, sample_chunk):
    import json

    processor = KafkaTargetProcessor(kafka_target)

    with processor:
        processor.begin_chunks(
            filename="test.pdf",
            temp_dir=Path("/tmp"),
            document_hash="abc123",
        )
        processor.consume_chunk(sample_chunk)

        call_kwargs = mock_producer["producer"].produce.call_args[1]
        payload = json.loads(call_kwargs["value"].decode("utf-8"))
        expected_id = KafkaTargetProcessor._stable_chunk_id("abc123", "test.pdf", 0)

        # Kafka is append-only: consumers can only dedupe if the ID is in the
        # body, not just the header.
        assert payload["chunk_id"] == expected_id
        headers = dict(call_kwargs["headers"])
        assert headers["chunk_id"] == expected_id


def test_consume_chunk_does_not_block_on_poll(
    kafka_target, mock_producer, sample_chunk
):
    processor = KafkaTargetProcessor(kafka_target)

    with processor:
        processor.begin_chunks(
            filename="test.pdf",
            temp_dir=Path("/tmp"),
            document_hash="abc123",
        )
        processor.consume_chunk(sample_chunk)
        processor.end_chunks()

    # flush() serves delivery callbacks itself, so every poll on the happy path
    # must be non-blocking.
    assert all(
        call.args[0] == 0 for call in mock_producer["producer"].poll.call_args_list
    )


def test_produce_kafka_exception_is_wrapped(kafka_target, mock_producer, sample_chunk):
    # librdkafka rejects an oversized message synchronously from produce().
    mock_producer["producer"].produce.side_effect = FakeKafkaException(
        "KafkaError{code=MSG_SIZE_TOO_LARGE}"
    )

    processor = KafkaTargetProcessor(kafka_target)

    with pytest.raises(TargetWriteError, match=r"message\.max\.bytes") as excinfo:
        with processor:
            processor.begin_chunks(
                filename="test.pdf",
                temp_dir=Path("/tmp"),
                document_hash="abc123",
            )
            processor.consume_chunk(sample_chunk)

    # The original cause must survive for the logs.
    assert isinstance(excinfo.value.__cause__, FakeKafkaException)
    # An oversized message is not retryable.
    assert mock_producer["producer"].produce.call_count == 1


def test_end_chunks_flush_timeout(kafka_target, mock_producer, sample_chunk):
    # Mock flush to return non-zero (timeout)
    mock_producer["producer"].flush.return_value = 5

    processor = KafkaTargetProcessor(kafka_target)

    with pytest.raises(TargetWriteError, match="flush timeout"):
        with processor:
            processor.begin_chunks(
                filename="test.pdf",
                temp_dir=Path("/tmp"),
                document_hash="abc123",
            )
            processor.consume_chunk(sample_chunk)
            processor.end_chunks()


def test_end_chunks_delivery_error(kafka_target, mock_producer, sample_chunk):
    processor = KafkaTargetProcessor(kafka_target)

    with pytest.raises(TargetWriteError, match="delivery failed"):
        with processor:
            processor.begin_chunks(
                filename="test.pdf",
                temp_dir=Path("/tmp"),
                document_hash="abc123",
            )
            processor.consume_chunk(sample_chunk)

            # Simulate delivery error
            processor._delivery_errors.append(Exception("broker down"))

            processor.end_chunks()


def test_buffer_error_retry(kafka_target, mock_producer, sample_chunk):
    # First produce raises BufferError, second succeeds
    mock_producer["producer"].produce.side_effect = [BufferError(), None]

    processor = KafkaTargetProcessor(kafka_target)

    with processor:
        processor.begin_chunks(
            filename="test.pdf",
            temp_dir=Path("/tmp"),
            document_hash="abc123",
        )
        processor.consume_chunk(sample_chunk)

        # Should have called produce twice (initial + retry)
        assert mock_producer["producer"].produce.call_count == 2


def test_buffer_error_exhausted(mock_producer, sample_chunk):
    # Every attempt raises BufferError until the deadline passes.
    mock_producer["producer"].produce.side_effect = BufferError()

    target = KafkaChunkTarget(
        bootstrap_servers=["localhost:9092"],
        topic="test.chunks",
        queue_full_timeout_seconds=0.0,
    )
    processor = KafkaTargetProcessor(target)

    with pytest.raises(TargetWriteError, match="queue still full"):
        with processor:
            processor.begin_chunks(
                filename="test.pdf",
                temp_dir=Path("/tmp"),
                document_hash="abc123",
            )
            processor.consume_chunk(sample_chunk)


def test_finalize_reports_undelivered_messages(kafka_target, mock_producer, caplog):
    # A path that never reached end_chunks() leaves messages queued.
    mock_producer["producer"].flush.return_value = 3

    processor = KafkaTargetProcessor(kafka_target)
    with caplog.at_level("ERROR"):
        with processor:
            processor.begin_chunks(
                filename="test.pdf",
                temp_dir=Path("/tmp"),
                document_hash="abc123",
            )

    assert "3 message(s) still queued" in caplog.text
