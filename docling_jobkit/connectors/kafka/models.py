from typing import Literal, Optional

from pydantic import BaseModel, Field, SecretStr

from docling_jobkit.datamodel.target_field_slots import ChunkFieldSlots, FieldMappings


class KafkaSaslAuth(BaseModel):
    # Confluent Cloud uses PLAIN - SCRAMs here for on-prem instances
    mechanism: Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] = "PLAIN"
    username: str
    password: SecretStr
    ca_cert_path: Optional[str] = None  # Optional path to CA cert for TLS


class KafkaChunkTarget(FieldMappings, ChunkFieldSlots):
    """Kafka target for chunk-level streaming.

    Each chunk is published as a separate message with JSON value.
    """

    kind: Literal["kafka_chunks"] = "kafka_chunks"

    bootstrap_servers: list[str] = Field(
        description="Kafka broker addresses (host:port).",
        examples=[["localhost:9092"]],
    )

    topic: str = Field(
        description="Kafka topic to publish chunks to.",
        examples=["docling.chunks"],
    )

    auth: Optional[KafkaSaslAuth] = Field(
        default=None,
        description="SASL authentication. Omit for plaintext (no auth).",
    )

    key_mode: Literal["doc_id", "chunk_id", "none"] = Field(
        default="doc_id",
        description=(
            "- 'doc_id': all chunks of a document land in one partition, ordered.\n"
            "- 'chunk_id': stable content hash, spread across partitions.\n"
            "- 'none': null key, round-robin distribution."
        ),
    )

    acks: Literal["0", "1", "all"] = Field(
        default="all",
        description=(
            "- '0': no ack (fire-and-forget)\n"
            "- '1': leader ack only\n"
            "- 'all': all in-sync replicas (most durable)"
        ),
    )


__all__ = [
    "KafkaChunkTarget",
    "KafkaSaslAuth",
]
