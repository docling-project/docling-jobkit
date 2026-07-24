from collections import defaultdict, deque
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaError, KafkaException
from pydantic import ValidationError

from docling.datamodel.base_models import DocumentStream

import docling_jobkit.connectors.kafka.helper as kafka_helper
from docling_jobkit.connectors.kafka.source_processor import KafkaSourceProcessor
from docling_jobkit.connectors.local_path.models import LocalPathSource
from docling_jobkit.connectors.source_processor import SourceDocumentRef
from docling_jobkit.datamodel.kafka_coords import KafkaSourceCoordinates
from docling_jobkit.datamodel.kafka_events import JobTriggerEvent


@pytest.fixture(autouse=True)
def _reset_correlation_ids() -> None:
    original = kafka_helper._correlation_ids
    kafka_helper._correlation_ids = defaultdict(deque)
    yield
    kafka_helper._correlation_ids = original


@pytest.fixture
def source_coords() -> KafkaSourceCoordinates:
    return KafkaSourceCoordinates(
        topic="test-topic",
        client_config={"bootstrap.servers": "broker:9092", "group.id": "test-group"},
        idle_timeout_s=5.0,
        backing_processor=LocalPathSource(path=Path(".")),
    )


@pytest.fixture
def patched_helpers(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    consumer = MagicMock()
    monkeypatch.setattr(kafka_helper, "build_consumer", lambda _: consumer)
    monkeypatch.setattr(kafka_helper, "test_connection", lambda *a, **kw: None)
    monkeypatch.setattr(kafka_helper, "subscribe_or_replay", lambda *a, **kw: None)
    return consumer


def _make_msg(
    event: JobTriggerEvent | None = None, value_bytes: bytes | None = None
) -> MagicMock:
    msg = MagicMock()
    msg.error.return_value = None
    msg.key.return_value = b"key"
    msg.value.return_value = (
        event.model_dump_json().encode() if event is not None else value_bytes
    )
    return msg


def test_source_coords_missing_bootstrap_servers_raises() -> None:
    with pytest.raises(ValidationError, match="bootstrap.servers"):
        KafkaSourceCoordinates(
            topic="t",
            client_config={"group.id": "g"},
            idle_timeout_s=5.0,
            backing_processor=LocalPathSource(path=Path(".")),
        )


def test_source_coords_missing_group_id_raises() -> None:
    with pytest.raises(ValidationError, match="group.id"):
        KafkaSourceCoordinates(
            topic="t",
            client_config={"bootstrap.servers": "broker:9092"},
            idle_timeout_s=5.0,
            backing_processor=LocalPathSource(path=Path(".")),
        )


def test_make_document_ref_preserves_fields(
    source_coords: KafkaSourceCoordinates,
) -> None:
    processor = KafkaSourceProcessor(source_coords)
    event = JobTriggerEvent(correlation_id="corr-abc", locator="inbox/report.pdf")

    ref = processor._make_document_ref(event, source_index=3)

    assert ref.source_index == 3
    assert ref.source_uri == "corr-abc"
    assert ref.filename == "report.pdf"


def test_make_document_ref_uses_explicit_filename(
    source_coords: KafkaSourceCoordinates,
) -> None:
    processor = KafkaSourceProcessor(source_coords)
    event = JobTriggerEvent(
        correlation_id="corr-abc", locator="inbox/report.pdf", filename="override.pdf"
    )
    assert (
        processor._make_document_ref(event, source_index=0).filename == "override.pdf"
    )


def test_list_document_ids_stops_at_max_messages(
    source_coords: KafkaSourceCoordinates, patched_helpers: MagicMock
) -> None:
    coords = source_coords.model_copy(update={"max_messages": 2})
    processor = KafkaSourceProcessor(coords)
    e1 = JobTriggerEvent(correlation_id="c1", locator="a.pdf")
    e2 = JobTriggerEvent(correlation_id="c2", locator="b.pdf")
    patched_helpers.poll.side_effect = [_make_msg(e1), _make_msg(e2)]

    assert [e.correlation_id for e in processor._list_document_ids()] == ["c1", "c2"]


def test_list_document_ids_stops_on_idle_timeout(
    source_coords: KafkaSourceCoordinates,
    patched_helpers: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processor = KafkaSourceProcessor(
        source_coords.model_copy(update={"max_messages": None})
    )
    patched_helpers.poll.return_value = None
    call_n = [0]

    def _monotonic() -> float:
        call_n[0] += 1
        return 0.0 if call_n[0] == 1 else 6.0

    monkeypatch.setattr("time.monotonic", _monotonic)
    assert list(processor._list_document_ids()) == []


def test_list_document_ids_skips_duplicates(
    source_coords: KafkaSourceCoordinates, patched_helpers: MagicMock
) -> None:
    coords = source_coords.model_copy(update={"max_messages": 2})
    processor = KafkaSourceProcessor(coords)
    e1 = JobTriggerEvent(
        correlation_id="c1", locator="a.pdf", idempotency_key="same-key"
    )
    e2 = JobTriggerEvent(
        correlation_id="c2", locator="a.pdf", idempotency_key="same-key"
    )
    e3 = JobTriggerEvent(correlation_id="c3", locator="b.pdf")
    patched_helpers.poll.side_effect = [_make_msg(e1), _make_msg(e2), _make_msg(e3)]

    assert [e.correlation_id for e in processor._list_document_ids()] == ["c1", "c3"]


def test_list_document_ids_skips_tombstones(
    source_coords: KafkaSourceCoordinates, patched_helpers: MagicMock
) -> None:
    coords = source_coords.model_copy(update={"max_messages": 1})
    processor = KafkaSourceProcessor(coords)
    good_event = JobTriggerEvent(correlation_id="c1", locator="a.pdf")
    patched_helpers.poll.side_effect = [
        _make_msg(value_bytes=None),
        _make_msg(good_event),
    ]

    events = list(processor._list_document_ids())
    assert len(events) == 1 and events[0].correlation_id == "c1"


def test_list_document_ids_malformed_msg_goes_to_dlq(
    source_coords: KafkaSourceCoordinates,
    patched_helpers: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coords = source_coords.model_copy(
        update={"max_messages": 1, "dlq_topic": "test.dlq"}
    )
    processor = KafkaSourceProcessor(coords)
    mock_publish = MagicMock()
    monkeypatch.setattr(kafka_helper, "build_producer", lambda _: MagicMock())
    monkeypatch.setattr(kafka_helper, "publish", mock_publish)
    good_event = JobTriggerEvent(correlation_id="c1", locator="a.pdf")
    patched_helpers.poll.side_effect = [
        _make_msg(value_bytes=b"bad-json"),
        _make_msg(good_event),
    ]

    events = list(processor._list_document_ids())

    assert len(events) == 1
    assert mock_publish.call_args[0][1] == "test.dlq"


def test_list_document_ids_malformed_msg_no_dlq_topic(
    source_coords: KafkaSourceCoordinates,
    patched_helpers: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coords = source_coords.model_copy(update={"max_messages": 1, "dlq_topic": None})
    processor = KafkaSourceProcessor(coords)
    mock_build_producer = MagicMock()
    monkeypatch.setattr(kafka_helper, "build_producer", mock_build_producer)
    good_event = JobTriggerEvent(correlation_id="c1", locator="a.pdf")
    patched_helpers.poll.side_effect = [
        _make_msg(value_bytes=b"bad-json"),
        _make_msg(good_event),
    ]

    list(processor._list_document_ids())

    mock_build_producer.assert_not_called()


def test_exit_commits_on_clean_run(source_coords: KafkaSourceCoordinates) -> None:
    processor = KafkaSourceProcessor(source_coords)
    processor._consumer = MagicMock()
    processor.__exit__(None, None, None)
    processor._consumer.commit.assert_called_once_with(asynchronous=False)


def test_exit_skips_commit_on_exception(source_coords: KafkaSourceCoordinates) -> None:
    processor = KafkaSourceProcessor(source_coords)
    processor._consumer = MagicMock()
    processor.__exit__(ValueError, ValueError("boom"), None)
    processor._consumer.commit.assert_not_called()


def test_exit_ignores_no_offset_error(source_coords: KafkaSourceCoordinates) -> None:
    processor = KafkaSourceProcessor(source_coords)
    processor._consumer = MagicMock()
    no_offset_err = MagicMock()
    no_offset_err.code.return_value = KafkaError._NO_OFFSET
    processor._consumer.commit.side_effect = KafkaException(no_offset_err)
    processor.__exit__(None, None, None)


def test_fetch_converter_source_stashes_correlation_id(
    source_coords: KafkaSourceCoordinates,
) -> None:
    processor = KafkaSourceProcessor(source_coords)
    event = JobTriggerEvent(correlation_id="corr-xyz", locator="inbox/doc.pdf")
    processor._backing_processor = MagicMock()
    processor._backing_processor.fetch_by_locator.return_value = DocumentStream(
        name="doc.pdf", stream=BytesIO(b"data")
    )
    ref = SourceDocumentRef(
        id=event, source_index=0, source_uri=event.correlation_id, filename="doc.pdf"
    )

    processor.fetch_converter_source_by_ref(ref)

    assert kafka_helper.pop_correlation_id("doc.pdf") == "corr-xyz"
