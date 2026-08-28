import hashlib
import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from docling_jobkit.connectors.database_target_processor import (
    BaseDatabaseTargetProcessor,
)
from docling_jobkit.connectors.kafka.models import KafkaChunkTarget
from docling_jobkit.public_errors import TargetWriteError

_log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from confluent_kafka import Producer

    from docling_jobkit.datamodel.result import ChunkedDocumentResultItem


class KafkaTargetProcessor(BaseDatabaseTargetProcessor[KafkaChunkTarget]):
    def __init__(self, target: KafkaChunkTarget) -> None:
        super().__init__(target)
        self._producer: "Optional[Producer]" = None
        self._current_document_hash: Optional[str] = None
        self._delivery_errors: list[Any] = []
        # Broker/client errors from the error_cb (e.g. _UNKNOWN_TOPIC).
        # These carry the real reason behind a _MSG_TIMED_OUT delivery failure.
        self._broker_errors: list[Any] = []

    @classmethod
    def check_dependencies(cls) -> None:
        import confluent_kafka  # noqa: F401

    @classmethod
    def get_config_types(cls):
        return (KafkaChunkTarget,)

    def _initialize(self) -> None:
        from confluent_kafka import Producer

        from docling_jobkit.connectors.kafka.helper import build_producer_config

        try:
            config = build_producer_config(self._target)
            config["error_cb"] = self._on_error
            self._producer = Producer(config)
        except Exception as exc:
            _log.error("kafka: connection failed", exc_info=True)
            raise TargetWriteError(
                f"Could not connect to Kafka brokers {self._target.bootstrap_servers}."
            ) from exc

        # Verify the topic exists before producing any messages.  librdkafka
        # surfaces a missing topic only as _MSG_TIMED_OUT on each message
        # (the real error goes to the internal log, not the delivery callback),
        # making the root cause invisible.  An explicit metadata fetch here
        # gives a clear error immediately at connect time.
        self._verify_topic()
        _log.info(
            "kafka: connected to %s, topic=%s",
            self._target.bootstrap_servers[0]
            if len(self._target.bootstrap_servers) == 1
            else f"{len(self._target.bootstrap_servers)} brokers",
            self._target.topic,
        )

    def _verify_topic(self) -> None:
        """Raise TargetWriteError if the configured topic does not exist."""
        from confluent_kafka.admin import AdminClient

        from docling_jobkit.connectors.kafka.helper import build_producer_config

        config = build_producer_config(self._target)
        # AdminClient shares the same config shape as Producer.
        admin = AdminClient(config)
        try:
            # list_topics() fetches cluster metadata; timeout is generous
            # but bounded so a network issue doesn't hang indefinitely.
            meta = admin.list_topics(
                topic=self._target.topic,
                timeout=10.0,
            )
        except Exception as exc:
            raise TargetWriteError(
                f"Could not fetch Kafka metadata for topic "
                f"'{self._target.topic}': {exc}"
            ) from exc

        topic_meta = meta.topics.get(self._target.topic)
        if topic_meta is None or topic_meta.error is not None:
            from confluent_kafka import KafkaError

            err = topic_meta.error if topic_meta is not None else None
            if err is not None and err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                raise TargetWriteError(
                    f"Topic '{self._target.topic}' does not exist on the broker. "
                    f"Create the topic or set 'allow.auto.create.topics=true' on "
                    f"the broker."
                )
            raise TargetWriteError(
                f"Kafka topic '{self._target.topic}' is not available: {err}"
            )
        _log.debug("kafka: topic '%s' exists and is accessible", self._target.topic)

    def _finalize(self) -> None:
        """Tear the producer down.

        ``end_chunks()`` already flushed and raised for the documents it saw;
        anything left here is a leftover from a path that skipped it. Report it
        loudly rather than dropping it silently — this runs from ``__exit__``,
        where raising would mask the exception that caused the teardown.
        """
        if self._producer is None:
            return

        producer, self._producer = self._producer, None
        remaining = producer.flush(10.0)
        if remaining:
            _log.error(
                "kafka: %d message(s) still queued for topic %s at teardown; "
                "they were dropped undelivered",
                remaining,
                self._target.topic,
            )
        if self._delivery_errors:
            _log.error(
                "kafka: %d unreported delivery error(s) for topic %s at "
                "teardown; first: %s",
                len(self._delivery_errors),
                self._target.topic,
                self._delivery_errors[0],
            )
        self._delivery_errors.clear()
        self._broker_errors.clear()

    # Streaming chunk protocol

    def instance_requires_chunks(self) -> bool:
        return True

    def begin_chunks(
        self,
        filename: str,
        temp_dir: Path,
        chunk_target_key: Optional[str] = None,
        document_hash: Optional[str] = None,
    ) -> None:
        """Capture document_hash for stable chunk IDs."""
        _log.info("kafka: begin_chunks for %s (hash=%s)", filename, document_hash)
        self._current_document_hash = document_hash
        self._delivery_errors.clear()
        self._broker_errors.clear()

    def consume_chunk(self, chunk: "ChunkedDocumentResultItem") -> None:
        """Produce one chunk message to Kafka."""
        from confluent_kafka import KafkaException

        from docling_jobkit.convert.chunking import _chunk_row_payload

        if self._producer is None:
            raise RuntimeError("KafkaTargetProcessor is not initialized")

        # Derive stable chunk ID
        chunk_id = self._stable_chunk_id(
            self._current_document_hash,
            chunk.filename,
            chunk.chunk_index,
        )

        # Build the message value from the chunk.  Kafka is append-only, so a
        # re-run after a mid-document failure appends a duplicate set of chunks
        # instead of overwriting (as OpenSearch does): the ID has to travel in
        # the payload so a consumer reading it alone can dedupe.
        row = _chunk_row_payload(chunk, self._target)
        row[self._target.chunk_id_field] = chunk_id
        value = json.dumps(row, ensure_ascii=False).encode("utf-8")

        # Determine message key based on key_mode
        key: Optional[str] = None
        if self._target.key_mode == "doc_id":
            key = chunk.filename
        elif self._target.key_mode == "chunk_id":
            key = chunk_id
        # else key_mode == "none", key stays None

        # Headers duplicate payload fields so consumers can route without
        # deserialising the value.  The values are always str, but produce()
        # takes an invariant list, so the element type has to stay as wide as
        # its signature.
        headers: list[tuple[str, str | bytes | None]] = [
            ("doc_id", chunk.filename),
            ("chunk_index", str(chunk.chunk_index)),
            ("chunk_id", chunk_id),
        ]

        _log.debug(
            "kafka: producing chunk %d for %s (key=%s, value_len=%d)",
            chunk.chunk_index,
            chunk.filename,
            key,
            len(value),
        )

        # Produce, retrying until the queue drains or the deadline passes.
        deadline = time.monotonic() + self._target.queue_full_timeout_seconds
        warned = False
        while True:
            try:
                self._producer.produce(
                    topic=self._target.topic,
                    key=key.encode("utf-8") if key else None,
                    value=value,
                    headers=headers,
                    on_delivery=self._on_delivery,
                )
                # Process delivery callbacks without blocking
                self._producer.poll(0)
                return
            except BufferError as exc:
                # Local queue full: bounded by queue_max_kbytes /
                # queue_max_messages, so this is backpressure, not a failure.
                time_left = deadline - time.monotonic()
                if time_left <= 0:
                    raise TargetWriteError(
                        f"Kafka producer queue still full after "
                        f"{self._target.queue_full_timeout_seconds:g}s "
                        f"for topic {self._target.topic}"
                    ) from exc
                if not warned:
                    _log.warning(
                        "kafka: producer queue full, waiting up to %.1fs...",
                        time_left,
                    )
                    warned = True
                # poll() serves delivery callbacks, which is what frees queue
                # slots.  The 50 ms floor keeps a near-expired deadline from
                # spinning.
                self._producer.poll(max(0.05, min(0.5, time_left)))
            except KafkaException as exc:
                # librdkafka rejects an oversized message synchronously from
                # produce() (MSG_SIZE_TOO_LARGE), and raw KafkaExceptions are
                # not recognised by classify_public_task_failure(); wrapping
                # them in TargetWriteError gives the same TARGET_UNAVAILABLE
                # classification the other targets produce.
                raise TargetWriteError(
                    f"Kafka rejected a {len(value)}-byte message for chunk "
                    f"{chunk.chunk_index} of {chunk.filename} on topic "
                    f"{self._target.topic}. If the chunk exceeds the message "
                    f"size limit, raise 'message.max.bytes' on the broker and "
                    f"'max.message.bytes' on the topic, or chunk smaller."
                ) from exc

    def end_chunks(self) -> None:
        """Flush producer and check for delivery errors."""
        if self._producer is None:
            return

        _log.debug("kafka: flushing producer...")
        remaining = self._producer.flush(10.0)

        if remaining > 0:
            # flush() timed out.  Do one extra poll to drain any callbacks
            # that fired just as flush() gave up, then check for delivery errors.
            self._producer.poll(0.5)

        if self._delivery_errors:
            from confluent_kafka import KafkaException

            first_error = self._delivery_errors[0]
            # If the error_cb captured broker-level detail (e.g. _SSL,
            # _ALL_BROKERS_DOWN), surface that; otherwise fall back to the
            # generic _MSG_TIMED_OUT from the delivery callback.
            if self._broker_errors:
                broker_err_str = " | ".join(str(e) for e in self._broker_errors)
                hint = (
                    f"Kafka delivery failed for topic '{self._target.topic}': "
                    f"{broker_err_str}"
                )
            else:
                hint = (
                    f"Kafka message delivery failed for topic "
                    f"'{self._target.topic}': {first_error}"
                )
            _log.error("kafka: %s", hint)
            # KafkaError is a C type with no __traceback__; wrap it in a
            # KafkaException before chaining so 'raise ... from' doesn't
            # crash the traceback formatter.
            raise TargetWriteError(hint) from KafkaException(first_error)

        if remaining > 0:
            _log.error("kafka: flush timeout, %d messages remain in queue", remaining)
            raise TargetWriteError(
                f"Kafka producer flush timeout: {remaining} messages still in queue "
                f"for topic '{self._target.topic}'"
            )

        _log.info("kafka: flush completed successfully")

        # Reset state
        self._current_document_hash = None
        self._delivery_errors.clear()
        self._broker_errors.clear()

    def abort_chunks(self) -> None:
        """Discard in-flight state. Already-sent messages stay sent."""
        self._current_document_hash = None
        self._delivery_errors.clear()
        self._broker_errors.clear()

    def _on_error(self, err) -> None:
        """Error callback for broker/client-level errors (error_cb).

        These fire with the real reason code (e.g. _UNKNOWN_TOPIC) that
        librdkafka wraps into the generic _MSG_TIMED_OUT delivery error.
        Captured here so end_chunks() can surface actionable diagnostics.
        """
        _log.warning("kafka: broker/client error: %s", err)
        self._broker_errors.append(err)

    def _on_delivery(self, err, msg) -> None:
        """Delivery callback for producer.produce().

        Captures errors in self._delivery_errors so end_chunks() can raise them.
        """
        if err is not None:
            _log.error("kafka: delivery error: %s", err)
            self._delivery_errors.append(err)
        else:
            _log.debug(
                "kafka: delivery success: topic=%s partition=%d offset=%d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    @staticmethod
    def _stable_chunk_id(
        binary_hash: Optional[str],
        filename: str,
        chunk_index: int,
    ) -> str:
        """Derive a deterministic, content-addressed ID for a single chunk."""
        key = f"{binary_hash or filename}:{filename}:{chunk_index}"
        return hashlib.sha256(key.encode(), usedforsecurity=False).hexdigest()

    # Doc protocol (unused)

    def upsert_row(self, row: dict[str, Any]) -> None:
        raise NotImplementedError(
            "kafka_chunks target does not support document-level upsert_row(). "
        )


__all__ = ["KafkaTargetProcessor"]
