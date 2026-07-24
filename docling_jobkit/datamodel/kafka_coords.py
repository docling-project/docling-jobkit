# NOTE: we may need to move these coordinates to the base docling repo in the future
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field, model_validator

from docling_jobkit.datamodel.task import TaskSource, TaskTarget


class KafkaSourceCoordinates(BaseModel):
    kind: Literal["kafka"] = "kafka"

    topic: Annotated[
        str, Field(description="kafka topic to consume from", examples=["docling.jobs"])
    ]

    client_config: Annotated[
        dict[str, str],
        Field(
            description=(
                "The kafka consumer config, passed straight to "
                "confluent_kafka.Consumer(). Required keys: bootstrap.servers, group.id"
            ),
            examples=["bootstrap.servers: broker:9092", "group.id: docling-jobkit"],
        ),
    ]

    max_messages: Annotated[
        Optional[int], Field(ge=1, description="Stop after N messages")
    ] = None

    idle_timeout_s: Annotated[
        float,
        Field(default=5.0, gt=0.0, description="Stop after this many idle seconds"),
    ]

    dlq_topic: Annotated[
        Optional[str],
        Field(
            description="Topic for unparsable input messages",
            examples=["docling.jobs.dlq"],
        ),
    ] = None

    start_timestamp: Annotated[
        Optional[float], Field(description="Replay: seek to an timestamp")
    ] = None

    # inner connector the locator is resolved against (holds its own creds)
    backing_processor: TaskSource

    @model_validator(mode="after")
    def _require_consumer_keys(self) -> "KafkaSourceCoordinates":
        missing = {"bootstrap.servers", "group.id"} - self.client_config.keys()
        if missing:
            raise ValueError(f"client_config missing required keys: {missing}")
        return self


class KafkaTargetCoordinates(BaseModel):
    kind: Literal["kafka"] = "kafka"

    status_topic: Annotated[
        str,
        Field(
            description="kafka topic to pub status/result events",
            examples=["docling.results"],
        ),
    ]

    client_config: Annotated[
        dict[str, str],
        Field(
            description=(
                "The kafka producer config, passed straight to "
                "confluent_kafka.Producer(). Required keys: bootstrap.servers"
            ),
            examples=["bootstrap.servers: broker:9092"],
        ),
    ]

    error_topic: Annotated[
        Optional[str],
        Field(description="Topic for error events; defaults to status_topic."),
    ] = None

    # the processor the locator is resolved against (holds its own creds)
    backing_processor: TaskTarget

    @model_validator(mode="after")
    def _require_producer_keys(self) -> "KafkaTargetCoordinates":
        if "bootstrap.servers" not in self.client_config:
            raise ValueError("client_config missing required keys: bootstrap.servers")
        return self
