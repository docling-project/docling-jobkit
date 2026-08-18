import base64
import logging
from typing import Any

from docling_jobkit.connectors.kafka.models import KafkaChunkTarget

_log = logging.getLogger(__name__)


def build_producer_config(target: KafkaChunkTarget) -> dict[str, Any]:
    """Build confluent_kafka.Producer config dict from target model."""
    protocol = target.effective_security_protocol

    config: dict[str, Any] = {
        "bootstrap.servers": ",".join(target.bootstrap_servers),
        "acks": target.acks,
        # Bound librdkafka's internal queues.  Its defaults (1 GiB / 100 000
        # messages) are native, per-worker memory on top of the models and the
        # DoclingDocument; see KafkaChunkTarget for the full rationale.
        "queue.buffering.max.kbytes": target.queue_max_kbytes,
        "queue.buffering.max.messages": target.queue_max_messages,
        "compression.type": target.compression_type,
        "client.id": "docling-jobkit",
        "security.protocol": protocol,
    }

    # The idempotent producer is what actually makes key_mode='doc_id' ordered:
    # without it, up to 1 000 000 requests may be in flight per connection and
    # a retry can reorder messages within a partition.  librdkafka rejects
    # enable.idempotence together with acks != 'all', so honour the user's
    # explicit choice of a weaker acks setting instead of failing at connect.
    if target.acks == "all":
        config["enable.idempotence"] = True
    elif target.key_mode == "doc_id":
        _log.warning(
            "kafka: acks=%s disables the idempotent producer, so chunks of a "
            "document may be reordered within their partition despite "
            "key_mode='doc_id'. Use acks='all' if ordering matters.",
            target.acks,
        )

    if protocol in ("SSL", "SASL_SSL"):
        config["enable.ssl.certificate.verification"] = target.verify_certs

    if target.auth is not None:
        auth = target.auth
        config["sasl.mechanism"] = auth.mechanism
        config["sasl.username"] = auth.username
        config["sasl.password"] = auth.password.get_secret_value()

        # Pass CA cert inline as PEM string (librdkafka ssl.ca.pem parameter)
        if auth.ca_cert is not None:
            ca_pem = base64.b64decode(auth.ca_cert.get_secret_value()).decode("utf-8")
            config["ssl.ca.pem"] = ca_pem

    return config


__all__ = ["build_producer_config"]
