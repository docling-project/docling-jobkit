from collections import defaultdict, deque
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from docling.datamodel.base_models import ConversionStatus

import docling_jobkit.connectors.kafka.helper as kafka_helper
from docling_jobkit.connectors.kafka.target_processor import KafkaTargetProcessor
from docling_jobkit.connectors.local_path.models import LocalPathTarget
from docling_jobkit.datamodel.kafka_coords import KafkaTargetCoordinates
from docling_jobkit.datamodel.kafka_events import JobErrorEvent, JobStatusEvent


@pytest.fixture(autouse=True)
def _reset_correlation_ids() -> None:
    original = kafka_helper._correlation_ids
    kafka_helper._correlation_ids = defaultdict(deque)
    yield
    kafka_helper._correlation_ids = original


@pytest.fixture
def target_coords() -> KafkaTargetCoordinates:
    return KafkaTargetCoordinates(
        status_topic="docling.results",
        client_config={"bootstrap.servers": "broker:9092"},
        backing_processor=LocalPathTarget(path=Path("./out")),
    )


@pytest.fixture
def processor(target_coords: KafkaTargetCoordinates) -> KafkaTargetProcessor:
    p = KafkaTargetProcessor(target_coords)
    p._producer = MagicMock()
    p._producer.flush.return_value = 0
    p._backing_config = MagicMock()
    return p


def _make_conv_result(
    filename: str, status: ConversionStatus, error_messages: list[str] | None = None
) -> MagicMock:
    conv_res = MagicMock()
    conv_res.input.file = filename
    conv_res.status = status
    conv_res.errors = [MagicMock(error_message=m) for m in (error_messages or [])]
    return conv_res


def test_target_coords_missing_bootstrap_servers_raises() -> None:
    with pytest.raises(ValidationError, match="bootstrap.servers"):
        KafkaTargetCoordinates(
            status_topic="t",
            client_config={},
            backing_processor=LocalPathTarget(path=Path("./out")),
        )


def test_on_document_completed_success_publishes_status_event(
    processor: KafkaTargetProcessor,
) -> None:
    kafka_helper.stash_correlation_id("report.pdf", "corr-123")
    processor.on_document_completed(
        _make_conv_result("path/to/report.pdf", ConversionStatus.SUCCESS)
    )

    topic, payload, *_ = processor._producer.produce.call_args[0]
    event = JobStatusEvent.model_validate_json(payload)
    assert event.correlation_id == "corr-123"
    assert event.status == "succeeded"
    assert topic == "docling.results"


def test_on_document_completed_success_includes_output_ref(
    processor: KafkaTargetProcessor,
) -> None:
    kafka_helper.stash_correlation_id("report.pdf", "corr-456")
    processor._written_keys = ["out/report.md", "out/report.json"]
    processor._backing_config.build_artifact_uri.side_effect = (
        lambda k: f"s3://bucket/{k}"
    )

    processor.on_document_completed(
        _make_conv_result("report.pdf", ConversionStatus.SUCCESS)
    )

    _, payload, *_ = processor._producer.produce.call_args[0]
    event = JobStatusEvent.model_validate_json(payload)
    assert event.output_ref == [
        "s3://bucket/out/report.md",
        "s3://bucket/out/report.json",
    ]


def test_on_document_completed_failure_publishes_error_event(
    processor: KafkaTargetProcessor,
) -> None:
    kafka_helper.stash_correlation_id("report.pdf", "corr-789")
    processor.on_document_completed(
        _make_conv_result(
            "report.pdf", ConversionStatus.FAILURE, ["PDF parsing failed"]
        )
    )

    _, payload, *_ = processor._producer.produce.call_args[0]
    event = JobErrorEvent.model_validate_json(payload)
    assert event.correlation_id == "corr-789"
    assert event.error_type == "conversion_failed"
    assert "PDF parsing failed" in event.message


def test_on_document_completed_failure_uses_error_topic(
    target_coords: KafkaTargetCoordinates,
) -> None:
    coords = target_coords.model_copy(update={"error_topic": "docling.errors"})
    p = KafkaTargetProcessor(coords)
    p._producer = MagicMock()
    p._producer.flush.return_value = 0
    p._backing_config = MagicMock()

    kafka_helper.stash_correlation_id("report.pdf", "corr-err")
    p.on_document_completed(
        _make_conv_result("report.pdf", ConversionStatus.FAILURE, ["oops"])
    )

    assert p._producer.produce.call_args[0][0] == "docling.errors"


def test_on_document_completed_missing_correlation_id_falls_back(
    processor: KafkaTargetProcessor,
) -> None:
    processor.on_document_completed(
        _make_conv_result("path/to/report.pdf", ConversionStatus.SUCCESS)
    )

    _, payload, *_ = processor._producer.produce.call_args[0]
    assert JobStatusEvent.model_validate_json(payload).correlation_id == "report"


def test_upload_file_delegates_to_backing_and_tracks_key(
    processor: KafkaTargetProcessor, tmp_path: Path
) -> None:
    src = tmp_path / "doc.pdf"
    src.write_bytes(b"data")

    processor.upload_file(src, "out/doc.pdf", "application/pdf")

    processor._backing_config.upload_file.assert_called_once_with(
        src, "out/doc.pdf", "application/pdf"
    )
    assert "out/doc.pdf" in processor._written_keys


def test_upload_object_delegates_to_backing_and_tracks_key(
    processor: KafkaTargetProcessor,
) -> None:
    processor.upload_object(b"data", "out/doc.json", "application/json")

    processor._backing_config.upload_object.assert_called_once_with(
        b"data", "out/doc.json", "application/json"
    )
    assert "out/doc.json" in processor._written_keys
