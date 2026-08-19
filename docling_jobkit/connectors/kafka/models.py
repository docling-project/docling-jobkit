from typing import Annotated, Any, Literal, Optional

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    SecretStr,
    model_validator,
)

from docling_jobkit.datamodel.target_field_slots import ChunkFieldSlots, FieldMappings


def _acks_to_str(value: Any) -> Any:
    """YAML parses ``acks: 1`` as an int, and pydantic v2 does not coerce
    int -> str for a string ``Literal``.  Normalise so the unquoted form every
    Kafka doc uses validates."""
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return value


AcksLiteral = Annotated[Literal["0", "1", "all"], BeforeValidator(_acks_to_str)]

SecurityProtocol = Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]


class KafkaSaslAuth(BaseModel):
    """SASL credentials.

    ``kind`` is the discriminator: it is the natural place to hang a future
    mTLS or OAUTHBEARER variant without breaking existing configs.
    """

    # Unknown keys are rejected: a misspelled 'mechanism' or 'username' would
    # otherwise be silently dropped and only surface as a connection failure
    # minutes into a job.
    model_config = ConfigDict(extra="forbid")

    kind: Literal["sasl"] = "sasl"
    # Confluent Cloud uses PLAIN - SCRAMs here for on-prem instances
    mechanism: Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] = "PLAIN"
    username: str
    password: SecretStr
    ca_cert: Optional[str] = Field(
        default=None,
        description=(
            "Base64-encoded CA certificate for TLS verification. Use this for "
            "self-signed or internal CAs. Omit for publicly-trusted CAs."
        ),
    )


class KafkaChunkTarget(FieldMappings, ChunkFieldSlots):
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

    security_protocol: Optional[SecurityProtocol] = Field(
        default=None,
        description=(
            "Broker connection protocol. Defaults to 'SASL_SSL' when 'auth' is "
            "set and 'PLAINTEXT' otherwise. Set explicitly for "
            "'SASL_PLAINTEXT' (SCRAM inside a trusted network) or 'SSL' "
            "(TLS without SASL)."
        ),
    )

    verify_certs: bool = Field(
        default=True,
        description=(
            "Verify the broker's TLS certificate. Only applies to the "
            "SSL / SASL_SSL protocols. Disable for self-signed certs in "
            "development only."
        ),
    )

    key_mode: Literal["doc_id", "chunk_id", "none"] = Field(
        default="doc_id",
        description=(
            "- 'doc_id': all chunks of a document land in one partition, ordered.\n"
            "- 'chunk_id': stable content hash, spread across partitions.\n"
            "- 'none': null key, round-robin distribution.\n"
            "Ordering under 'doc_id' relies on the idempotent producer, which "
            "is only enabled when acks == 'all'.\n"
            "WARNING: do not enable log compaction on the topic with "
            "key_mode='doc_id' — the broker would keep only the last chunk of "
            "each document and silently discard the rest."
        ),
    )

    acks: AcksLiteral = Field(
        default="all",
        description=(
            "- '0': no ack (fire-and-forget)\n"
            "- '1': leader ack only\n"
            "- 'all': all in-sync replicas (most durable)\n"
            "The idempotent producer requires 'all'; with '0' or '1' it is "
            "switched off and a retry can reorder messages within a partition."
        ),
    )

    # Producer queue bounds.  produce() memcpy's into librdkafka's internal
    # queues inside the worker process, so these are *native* client-side
    # memory (invisible to tracemalloc) that stacks on top of the models and
    # the DoclingDocument.  librdkafka's own defaults are 1 GiB / 100 000
    # messages, which under a k8s memory limit surfaces as an OOMKill with no
    # Python traceback.
    queue_max_kbytes: int = Field(
        default=65536,
        gt=0,
        description=(
            "Producer queue size limit in KiB (librdkafka "
            "'queue.buffering.max.kbytes'). Bounds native client-side memory; "
            "librdkafka's own default is 1 GiB."
        ),
    )

    queue_max_messages: int = Field(
        default=10_000,
        gt=0,
        description=(
            "Producer queue size limit in messages (librdkafka "
            "'queue.buffering.max.messages'). librdkafka's own default is "
            "100 000."
        ),
    )

    compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"] = Field(
        default="lz4",
        description=(
            "Producer-side compression. Close to free on chunk text and cuts "
            "both queued bytes and broker traffic."
        ),
    )

    queue_full_timeout_seconds: float = Field(
        default=30.0,
        ge=0.0,
        description=(
            "How long to keep retrying a single chunk while the producer queue "
            "is full before failing the task with TargetWriteError."
        ),
    )

    delivery_timeout_seconds: float = Field(
        default=8.0,
        gt=0.0,
        description=(
            "Per-message delivery timeout in seconds (librdkafka "
            "'message.timeout.ms'). librdkafka will retry delivery until this "
            "deadline expires, then fire the delivery callback with an error. "
            "Must be shorter than the flush timeout (10 s) so that delivery "
            "errors surface before the flush call gives up."
        ),
    )

    chunk_id_field: str = Field(
        default="chunk_id",
        description=(
            "Field name carrying the deterministic chunk ID in the message "
            "value. Kafka is append-only, so a re-run after a mid-document "
            "failure appends a duplicate set of chunks; consumers dedupe on "
            "this ID. It is also emitted as a message header."
        ),
    )

    # Redeclared explicitly (both base classes declare it) so the default is
    # readable as deliberate rather than an MRO coincidence.  Kafka/JSON carry
    # uint64 fine; enable if your consumers deserialize into int64 (Kafka
    # Connect, Jackson, Go) or JavaScript, which lose or reject
    # DoclingDocument.origin.binary_hash (a uint64, always > 2^53 and > 2^63-1
    # about half the time).
    coerce_large_ints_to_str: bool = False

    @model_validator(mode="after")
    def _check_auth_matches_protocol(self) -> "KafkaChunkTarget":
        protocol = self.security_protocol
        if protocol is None:
            return self
        if protocol.startswith("SASL_") and self.auth is None:
            raise ValueError(
                f"security_protocol '{protocol}' requires an 'auth' block."
            )
        if not protocol.startswith("SASL_") and self.auth is not None:
            raise ValueError(
                f"security_protocol '{protocol}' does not use SASL, but an "
                "'auth' block was provided."
            )
        return self

    @property
    def effective_security_protocol(self) -> SecurityProtocol:
        """Resolved protocol: explicit when set, else derived from ``auth``."""
        if self.security_protocol is not None:
            return self.security_protocol
        return "SASL_SSL" if self.auth is not None else "PLAINTEXT"


__all__ = [
    "KafkaChunkTarget",
    "KafkaSaslAuth",
]
