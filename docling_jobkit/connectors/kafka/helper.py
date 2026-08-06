import logging
import time
from collections import defaultdict, deque
from typing import Callable, Optional, TypeVar

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)

_log = logging.getLogger(__name__)

T = TypeVar("T")


def build_consumer(client_config: dict[str, str]) -> Consumer:
    """Creates Kafka consumer with default enable.auto.commit and injected user configs"""
    cfg = dict(client_config)
    # disabling autocommit so it only commits offsets after a msg is fully processed
    cfg["enable.auto.commit"] = "false"
    cfg.setdefault("auto.offset.reset", "earliest")

    return Consumer(cfg)


def build_producer(client_config: dict[str, str]) -> Producer:
    """Creates a Kafka producer with user defined configs"""
    return Producer(dict(client_config))


def subscribe_or_replay(
    consumer: Consumer, topic: str, *, start_timestamp: Optional[float] = None
) -> None:
    """Normal group subscribe, or assign + seek to atimestamp for replay"""
    if start_timestamp is None:
        consumer.subscribe([topic])
        return

    # replay support
    partitions = consumer.list_topics(topic, timeout=10.0).topics[topic].partitions
    resolved = consumer.offsets_for_times(
        [TopicPartition(topic, p, int(start_timestamp * 1000)) for p in partitions],
        timeout=10.0,
    )

    # for the case where all the newest msgs predate timestamp
    if all(partition_offset.offset < 0 for partition_offset in resolved):
        _log.warning(
            "Kafka: start_timestamp=%s is past the last message in every partition "
            "of topic %r; nothing to replay so far",
            start_timestamp,
            topic,
        )

    consumer.assign(resolved)


def test_connection(client: Consumer | Producer, timeout: float = 5.0) -> None:
    """This checks if broker is reacahable

    Raises:
        KafkaException if unreachable
    """
    client.list_topics(timeout=timeout)


def publish(
    producer: Producer,
    topic: str,
    value: bytes,
    key: str | bytes | None = None,
    timeout: float = 10.0,
) -> None:
    """Produce a single message and block until delivery is confirmed.

    Registers a delivery callback, then flushes the producer so the call is
    synchronous: it returns only once the broker has acknowledged the message.

    Raises:
        KafkaException: if the flush does not drain within ``timeout`` seconds
            (message unconfirmed), or if the broker reported a delivery error.
    """
    delivery_error: list[KafkaError] = []

    def _on_delivery(err: Optional[KafkaError], _msg) -> None:
        if err is not None:
            delivery_error.append(err)

    producer.produce(topic, value, key, on_delivery=_on_delivery)
    remaining = producer.flush(timeout)
    if remaining > 0:
        raise KafkaException(f"publish to {topic} not confirmed within {timeout}s")
    if delivery_error:
        raise KafkaException(delivery_error[0])


def retry_with_backoff(
    fn: Callable[[], T],
    *,
    max_retries: int = 4,
    base_delay: float = 0.5,
    max_delay: float = 8.0,
) -> T:
    """Retry with exponential backoff for transient errors

    Defaults to 4 max_retries, base delay of 0.5s, and max_delay of 8s
    """
    for i in range(max_retries):
        try:
            return fn()
        except KafkaException as exc:
            if i == max_retries - 1:
                raise
            delay = min(base_delay * (2**i), max_delay)
            _log.warning(
                "kafka op failed (attempt %d/%d): %s; retrying in %.1fs",
                i + 1,
                max_retries,
                exc,
                delay,
            )
            time.sleep(delay)

    raise AssertionError("unreachable: retry loop exited without return or raise")


# queue for source to stash and target to read at completion
# using a queue instead of just str in case of same basename
# (a/report.pdf and b/report.pdf) edge case
_correlation_ids: dict[str, deque[str]] = defaultdict(deque)


def stash_correlation_id(doc_name: str, correlation_id: str) -> None:
    _correlation_ids[doc_name].append(correlation_id)


def pop_correlation_id(doc_name: str) -> str | None:
    queue = _correlation_ids.get(doc_name)
    if not queue:
        return None

    correlation_id = queue.popleft()
    if not queue:
        del _correlation_ids[doc_name]

    return correlation_id
