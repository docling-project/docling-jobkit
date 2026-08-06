from collections import defaultdict, deque
from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaException

import docling_jobkit.connectors.kafka.helper as kafka_helper
from docling_jobkit.connectors.errors import (
    KafkaConfigError,
    validate_kafka_kind_pairing,
)
from docling_jobkit.connectors.kafka.helper import (
    build_consumer,
    pop_correlation_id,
    publish,
    retry_with_backoff,
    stash_correlation_id,
    subscribe_or_replay,
)


@pytest.fixture(autouse=True)
def _reset_correlation_ids():
    original = kafka_helper._correlation_ids
    kafka_helper._correlation_ids = defaultdict(deque)
    yield
    kafka_helper._correlation_ids = original


def test_build_consumer_applies_config_invariants(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        kafka_helper, "Consumer", lambda cfg: captured.update(cfg) or MagicMock()
    )

    build_consumer({"bootstrap.servers": "broker:9092", "group.id": "grp"})

    assert captured["enable.auto.commit"] == "false"
    assert captured["auto.offset.reset"] == "earliest"


def test_subscribe_or_replay_without_timestamp():
    consumer = MagicMock()
    subscribe_or_replay(consumer, "my-topic")
    consumer.subscribe.assert_called_once_with(["my-topic"])
    consumer.assign.assert_not_called()


def test_subscribe_or_replay_with_timestamp():
    consumer = MagicMock()
    consumer.list_topics.return_value.topics = {
        "my-topic": MagicMock(partitions={0: MagicMock(), 1: MagicMock()})
    }
    tp0, tp1 = MagicMock(offset=100), MagicMock(offset=200)
    consumer.offsets_for_times.return_value = [tp0, tp1]

    subscribe_or_replay(consumer, "my-topic", start_timestamp=1_700_000_000.0)

    consumer.subscribe.assert_not_called()
    consumer.offsets_for_times.assert_called_once()
    consumer.assign.assert_called_once_with([tp0, tp1])


def test_publish_raises_on_delivery_error():
    producer = MagicMock()
    producer.flush.return_value = 0

    def _produce_with_error(topic, value, key, on_delivery=None):
        if on_delivery:
            on_delivery(MagicMock(), None)

    producer.produce.side_effect = _produce_with_error

    with pytest.raises(KafkaException):
        publish(producer, "my-topic", b"payload")


def test_publish_raises_on_flush_timeout():
    producer = MagicMock()
    producer.flush.return_value = 1  # unflushed messages remain

    with pytest.raises(KafkaException, match="not confirmed"):
        publish(producer, "my-topic", b"payload")


def test_retry_with_backoff_succeeds_first_try(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _: None)
    fn = MagicMock(return_value="ok")
    assert retry_with_backoff(fn) == "ok"
    assert fn.call_count == 1


def test_retry_with_backoff_retries_on_kafka_exception(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _: None)
    call_count = [0]

    def fn():
        call_count[0] += 1
        if call_count[0] < 3:
            raise KafkaException(MagicMock())
        return "done"

    assert retry_with_backoff(fn, max_retries=4) == "done"
    assert call_count[0] == 3


def test_retry_with_backoff_raises_non_kafka_exception_immediately(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _: None)
    call_count = [0]

    def fn():
        call_count[0] += 1
        raise ValueError("not kafka")

    with pytest.raises(ValueError, match="not kafka"):
        retry_with_backoff(fn, max_retries=4)

    assert call_count[0] == 1


def test_stash_and_pop_correlation_id_roundtrip():
    stash_correlation_id("report.pdf", "corr-1")
    assert pop_correlation_id("report.pdf") == "corr-1"
    assert pop_correlation_id("report.pdf") is None


def test_stash_same_basename_uses_queue():
    stash_correlation_id("report.pdf", "corr-a")
    stash_correlation_id("report.pdf", "corr-b")
    assert pop_correlation_id("report.pdf") == "corr-a"
    assert pop_correlation_id("report.pdf") == "corr-b"
    assert pop_correlation_id("report.pdf") is None


@pytest.mark.parametrize(
    "source_kinds, target_kind, raises",
    [
        (["kafka"], "kafka", False),
        (["s3"], "s3", False),
        (["kafka"], "s3", True),
        (["s3"], "kafka", True),
        (["kafka", "s3"], "kafka", True),
    ],
)
def test_validate_kafka_kind_pairing(source_kinds, target_kind, raises):
    sources = [MagicMock(kind=k) for k in source_kinds]
    target = MagicMock(kind=target_kind)
    if raises:
        with pytest.raises(KafkaConfigError):
            validate_kafka_kind_pairing(sources, target)
    else:
        validate_kafka_kind_pairing(sources, target)
