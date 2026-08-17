import hashlib
import json
import logging
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
            _log.info(
                "Initializing Kafka producer: brokers=%s topic=%s",
                self._target.bootstrap_servers,
                self._target.topic,
            )
            self._producer = Producer(config)
            _log.info("Kafka producer initialized successfully")
        except Exception as exc:
            _log.error("Failed to initialize Kafka producer", exc_info=True)
            raise TargetWriteError(
                f"Could not connect to Kafka brokers {self._target.bootstrap_servers}."
            ) from exc

    def _finalize(self) -> None:
        if self._producer is not None:
            self._producer.flush(10.0)
            self._producer = None

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

    def consume_chunk(self, chunk: "ChunkedDocumentResultItem") -> None:
        """Produce one chunk message to Kafka."""
        from docling_jobkit.convert.chunking import _chunk_row_payload

        if self._producer is None:
            raise RuntimeError("KafkaTargetProcessor is not initialized")

        # Build the message value from the chunk
        row = _chunk_row_payload(chunk, self._target)
        value = json.dumps(row, ensure_ascii=False).encode("utf-8")

        # Derive stable chunk ID
        chunk_id = self._stable_chunk_id(
            self._current_document_hash,
            chunk.filename,
            chunk.chunk_index,
        )

        # Determine message key based on key_mode
        key: Optional[str] = None
        if self._target.key_mode == "doc_id":
            key = chunk.filename
        elif self._target.key_mode == "chunk_id":
            key = chunk_id
        # else key_mode == "none", key stays None

        # Build headers (list of tuples for confluent_kafka)
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

        # Produce the message with retry on BufferError
        for attempt in range(2):
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
                break
            except BufferError:
                if attempt == 0:
                    _log.warning("kafka: producer queue full, waiting...")
                    self._producer.poll(1.0)
                else:
                    raise TargetWriteError(
                        f"Kafka producer queue full after retry for topic {self._target.topic}"
                    )

    def end_chunks(self) -> None:
        """Flush producer and check for delivery errors."""
        if self._producer is None:
            return

        _log.debug("kafka: flushing producer...")
        remaining = self._producer.flush(10.0)
        if remaining > 0:
            _log.error("kafka: flush timeout, %d messages remain in queue", remaining)
            raise TargetWriteError(
                f"Kafka producer flush timeout: {remaining} messages still in queue"
            )

        # Poll to process any pending delivery callbacks (flush() only blocks
        # on transmission, not on broker ack — callbacks may still be pending)
        self._producer.poll(1.0)

        # Check for delivery errors
        if self._delivery_errors:
            first_error = self._delivery_errors[0]
            _log.error(
                "kafka: delivery failed for topic %s: %s",
                self._target.topic,
                first_error,
            )
            raise TargetWriteError(
                f"Kafka message delivery failed for topic {self._target.topic}"
            ) from first_error

        _log.info("kafka: flush completed successfully")

        # Reset state
        self._current_document_hash = None
        self._delivery_errors.clear()

    def abort_chunks(self) -> None:
        """Discard in-flight state. Already-sent messages stay sent."""
        self._current_document_hash = None
        self._delivery_errors.clear()

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
