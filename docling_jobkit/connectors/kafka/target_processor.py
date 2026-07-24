import logging
from pathlib import Path
from typing import BinaryIO

from pydantic import BaseModel

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.document import ConversionResult

from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.kafka_coords import KafkaTargetCoordinates
from docling_jobkit.datamodel.kafka_events import JobErrorEvent, JobStatusEvent

_log = logging.getLogger(__name__)


class KafkaTargetProcessor(BaseTargetProcessor):
    def __init__(self, coords: KafkaTargetCoordinates):
        super().__init__()
        self._coords = coords
        self._written_keys: list[str] = []

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (KafkaTargetCoordinates,)

    def _initialize(self) -> None:
        from docling_jobkit.connectors.kafka.helper import build_producer
        from docling_jobkit.connectors.target_processor_factory import (
            get_target_processor,
        )

        self._backing_config = get_target_processor(self._coords.backing_processor)
        self._backing_config.__enter__()
        self._producer = build_producer(self._coords.client_config)

    def _finalize(self) -> None:
        self._producer.flush(10)
        self._backing_config.__exit__(None, None, None)

    def upload_file(
        self, filename: str | Path, target_filename: str, content_type: str
    ) -> None:
        self._backing_config.upload_file(filename, target_filename, content_type)
        self._written_keys.append(target_filename)

    def upload_object(
        self, obj: str | bytes | BinaryIO, target_filename: str, content_type: str
    ) -> None:
        self._backing_config.upload_object(obj, target_filename, content_type)
        self._written_keys.append(target_filename)

    def on_document_completed(self, conv_res: ConversionResult) -> None:
        from docling_jobkit.connectors.kafka.helper import (
            pop_correlation_id,
            publish,
            retry_with_backoff,
        )

        name = Path(conv_res.input.file).name
        correlation_id = pop_correlation_id(name)
        if correlation_id is None:
            _log.warning("kafka: no correlation_id for %s; using name stem", name)
            correlation_id = Path(conv_res.input.file).stem

        written, self._written_keys = self._written_keys, []

        event: JobStatusEvent | JobErrorEvent
        if conv_res.status == ConversionStatus.FAILURE:
            topic = self._coords.error_topic or self._coords.status_topic
            event = JobErrorEvent(
                correlation_id=correlation_id,
                error_type="conversion_failed",
                message="; ".join(e.error_message for e in conv_res.errors)
                or "document conversion failed",
                retryable=False,
            )
            _log.info(
                "kafka: publishing error event correlation_id=%s topic=%s",
                correlation_id,
                topic,
            )
        else:
            topic = self._coords.status_topic
            # claim-check: one URI per artifact wrote
            build_uri = getattr(self._backing_config, "build_artifact_uri", None)
            output_ref = (
                [build_uri(k) for k in written] if build_uri is not None else []
            )
            event = JobStatusEvent(
                correlation_id=correlation_id, status="succeeded", output_ref=output_ref
            )

            _log.debug(
                "kafka: publishing status event correlation_id=%s topic=%s output_ref=%s",
                correlation_id,
                topic,
                output_ref,
            )

        payload = event.model_dump_json().encode("utf-8")
        try:
            retry_with_backoff(
                lambda: publish(self._producer, topic, payload, key=correlation_id)
            )
        except Exception:
            _log.error(
                "kafka: failed to publish %s for correlation_id=%s to topic=%s "
                "after retries; event is lost",
                type(event).__name__,
                correlation_id,
                topic,
                exc_info=True,
            )
