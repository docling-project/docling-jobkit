# Helper file to publish kafka messages to broker to test the kafka connector
# Need kafka broker running and cli running:
#   - docker compose -f dev/docker-compose.kafka.yaml up -d
import uuid

from docling_jobkit.connectors.kafka.helper import build_producer, publish
from docling_jobkit.datamodel.kafka_events import JobTriggerEvent

BOOTSTRAP = "localhost:9092"
JOBS_TOPIC = "docling.jobs"

event = JobTriggerEvent(
    correlation_id=f"test-{uuid.uuid4().hex[:8]}", locator="<file name/file id>"
)

producer = build_producer({"bootstrap.servers": BOOTSTRAP})

publish(
    producer,
    JOBS_TOPIC,
    event.model_dump_json().encode("utf-8"),
    key=event.correlation_id,
)

print(f"published {event.correlation_id} -> {JOBS_TOPIC}")
