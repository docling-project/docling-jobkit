import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Optional

from pydantic import BaseModel
from typing_extensions import override

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    ConverterSource,
    SourceDocumentRef,
)

if TYPE_CHECKING:
    from confluent_kafka import Consumer, Producer

from docling_jobkit.datamodel.kafka_coords import KafkaSourceCoordinates
from docling_jobkit.datamodel.kafka_events import JobTriggerEvent

_log = logging.getLogger(__name__)


# TODO: support Ray in the future
class KafkaSourceProcessor(
    BaseSourceProcessor[KafkaSourceCoordinates, JobTriggerEvent]
):
    def __init__(self, coords: KafkaSourceCoordinates):
        super().__init__(coords)
        self._coords = coords
        # built lazily/conditionally: consumer + dlq in the poller, inner in workers
        self._consumer: Optional["Consumer"] = None
        self._dlq_producer: Optional["Producer"] = None  # only if dlq_topic is set
        self._backing_processor: Optional[BaseSourceProcessor] = None
        self._seen_keys: set[str] = set()  # idempotency for this run

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (KafkaSourceCoordinates,)

    def _initialize(self):
        # consumer is only used in polling process and inner source is only used in fetching workers
        pass

    def _finalize(self) -> None:
        if self._backing_processor is not None:
            self._backing_processor.__exit__(None, None, None)
        if self._dlq_producer is not None:
            self._dlq_producer.flush(10)
        if self._consumer is not None:
            self._consumer.close()
        self._seen_keys.clear()

    @override
    def __exit__(self, exc_type, exc_val, exc_tb):
        # at least once: commit only on clean run (crash reprocesses batch)
        if exc_type is None and self._consumer is not None:
            from confluent_kafka import KafkaError, KafkaException

            try:
                self._consumer.commit(asynchronous=False)
            except KafkaException as exc:
                # _NO_OFFSET is not an error since nothing was consumed this run
                if exc.args[0].code() != KafkaError._NO_OFFSET:
                    _log.warning("offset commit failed", exc_info=True)

        return super().__exit__(exc_type, exc_val, exc_tb)

    def _list_document_ids(self) -> Iterator[JobTriggerEvent]:
        from confluent_kafka import KafkaError, KafkaException

        from docling_jobkit.connectors.kafka.helper import (
            build_consumer,
            build_producer,
            publish,
            subscribe_or_replay,
            test_connection,
        )

        # building our consumer here instead of init where its actually used
        self._consumer = build_consumer(self._coords.client_config)
        test_connection(self._consumer)
        subscribe_or_replay(
            self._consumer,
            self._coords.topic,
            start_timestamp=self._coords.start_timestamp,
        )

        _log.info(
            "kafka: consuming topic=%s max_messages=%s",
            self._coords.topic,
            self._coords.max_messages,
        )

        # Main polling loop
        count, dlq_count, last_activity = 0, 0, time.monotonic()
        reason = "max_messages"
        while self._coords.max_messages is None or count < self._coords.max_messages:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                if time.monotonic() - last_activity >= self._coords.idle_timeout_s:
                    reason = "idle_timeout"
                    break
                continue

            err = msg.error()
            if err is not None and (
                err.code() == KafkaError._PARTITION_EOF or err.retriable()
            ):
                continue
            if err is not None:
                raise KafkaException(err)

            last_activity = time.monotonic()
            raw = msg.value()
            if raw is None:  # empty Payload / tombstomb so nothing to parse
                continue
            try:
                event = JobTriggerEvent.model_validate_json(raw)
            except Exception:
                _log.warning("malformed trigger msg, sending to DLQ", exc_info=True)
                if self._coords.dlq_topic:
                    if self._dlq_producer is None:
                        self._dlq_producer = build_producer(self._coords.client_config)
                    publish(
                        self._dlq_producer, self._coords.dlq_topic, raw, key=msg.key()
                    )
                    dlq_count += 1
                continue

            key = event.idempotency_key or event.locator
            if key in self._seen_keys:
                _log.debug("kafka: skipping duplicate idempotency_key=%s", key)
                continue
            self._seen_keys.add(key)
            count += 1
            _log.debug(
                "kafka: consumed correlation_id=%s locator=%s",
                event.correlation_id,
                event.locator,
            )

            yield event

        _log.info(
            "kafka: poll finished consumed=%d dlq=%d reason=%s",
            count,
            dlq_count,
            reason,
        )

    @override
    def _make_document_ref(
        self, identifier: JobTriggerEvent, source_index: int
    ) -> SourceDocumentRef[JobTriggerEvent]:
        return SourceDocumentRef(
            id=identifier,
            source_index=source_index,
            source_uri=identifier.correlation_id,
            filename=identifier.filename or identifier.locator.rsplit("/", 1)[-1],
        )

    @override
    def fetch_converter_source_by_ref(
        self,
        ref: SourceDocumentRef[JobTriggerEvent],
        *,
        max_file_size: int | None = None,
    ) -> ConverterSource:
        from docling_jobkit.connectors.kafka.helper import stash_correlation_id

        if self._backing_processor is None:
            from docling_jobkit.connectors.source_processor_factory import (
                get_source_processor,
            )

            self._backing_processor = get_source_processor(
                self._coords.backing_processor
            )
            self._backing_processor.__enter__()

        _log.debug(
            "kafka: fetching locator=%s correlation_id=%s",
            ref.id.locator,
            ref.id.correlation_id,
        )

        source = self._backing_processor.fetch_by_locator(
            ref.id.locator, max_file_size=max_file_size
        )
        if isinstance(source, DocumentStream):
            source.name = Path(source.name).name
            stash_correlation_id(source.name, ref.id.correlation_id)

        return source

    def _fetch_documents(
        self, *, max_file_size: int | None = None
    ) -> Iterator[DocumentStream]:
        for source_index, identifier in enumerate(self._list_document_ids()):
            ref = self._make_document_ref(identifier, source_index)
            try:
                source = self.fetch_converter_source_by_ref(
                    ref, max_file_size=max_file_size
                )
            except Exception:
                _log.warning(
                    "Kafka: fetch failed correlation_id=%s locator=%s; skipping",
                    ref.id.correlation_id,
                    ref.id.locator,
                    exc_info=True,
                )
                continue

            # fetch returns str | DocumentStream; a URL string is valid on the chunk path but
            # the eager path can only yield bytes. Narrows the union and fails clearly otherwise.
            if not isinstance(source, DocumentStream):
                raise RuntimeError(
                    "Kafka -local path requires an inner source that returns document bytes; "
                    f"{type(source).__name__} is not supported."
                )

            yield source
