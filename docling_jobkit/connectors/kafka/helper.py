from typing import Any

from docling_jobkit.connectors.kafka.models import KafkaChunkTarget


def build_producer_config(target: KafkaChunkTarget) -> dict[str, Any]:
    """Build confluent_kafka.Producer config dict from target model."""
    config: dict[str, Any] = {
        "bootstrap.servers": ",".join(target.bootstrap_servers),
        "acks": target.acks,
    }

    if target.auth is None:
        config["security.protocol"] = "PLAINTEXT"
    else:
        auth = target.auth
        config["security.protocol"] = "SASL_SSL"  # Always use SSL with SASL
        config["sasl.mechanism"] = auth.mechanism  # PLAIN for Confluent Cloud API auth
        config["sasl.username"] = auth.username
        config["sasl.password"] = auth.password.get_secret_value()
        if auth.ca_cert_path:
            config["ssl.ca.location"] = auth.ca_cert_path  # Optional: custom CA only

    return config


__all__ = ["build_producer_config"]
