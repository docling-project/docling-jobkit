import base64
from io import BytesIO

import pytest
from pydantic import SecretStr

from docling.datamodel.base_models import DocumentStream
from docling.datamodel.service.requests import (
    FileSourceRequest,
    HttpSourceRequest,
    S3SourceRequest,
)
from docling.datamodel.service.sources import FileSource, HttpSource, S3Coordinates
from docling.datamodel.service.tasks import TaskType

from docling_jobkit.cli.local import JobConfig as LocalJobConfig
from docling_jobkit.cli.multiproc import JobConfig as MultiprocJobConfig
from docling_jobkit.connectors.s3_source_processor import S3SourceProcessor
from docling_jobkit.connectors.source_processor_factory import get_source_processor
from docling_jobkit.convert.source_expansion import expand_task_sources
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_targets import InBodyTarget
from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestrator,
    RQOrchestratorConfig,
)


def _make_s3_source() -> S3Coordinates:
    return S3Coordinates(
        endpoint="127.0.0.1:9000",
        verify_ssl=False,
        access_key=SecretStr("minioadmin"),
        secret_key=SecretStr("minioadmin"),
        bucket="test-bucket",
        key_prefix="incoming/",
    )


def test_expand_task_sources_materializes_s3(monkeypatch: pytest.MonkeyPatch):
    s3_source = _make_s3_source()
    task = Task(
        task_id="task-1",
        task_type=TaskType.CONVERT,
        sources=[s3_source],
        target=InBodyTarget(),
    )
    expected_docs = [
        DocumentStream(name="a.pdf", stream=BytesIO(b"a")),
        DocumentStream(name="b.pdf", stream=BytesIO(b"b")),
    ]

    class FakeSourceProcessor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def iterate_documents(self):
            return iter(expected_docs)

    monkeypatch.setattr(
        "docling_jobkit.convert.source_expansion.get_source_processor",
        lambda source: FakeSourceProcessor(),
    )

    convert_sources, headers = expand_task_sources(task)

    assert headers is None
    assert convert_sources == expected_docs


def test_get_source_processor_accepts_s3coordinates():
    processor = get_source_processor(_make_s3_source())
    assert isinstance(processor, S3SourceProcessor)


def test_task_roundtrip_accepts_request_subclasses_without_normalization():
    task = Task(
        task_id="task-1",
        task_type=TaskType.CONVERT,
        sources=[
            FileSourceRequest(
                filename="doc.pdf",
                base64_string=base64.b64encode(b"pdf-bytes").decode(),
            ),
            HttpSourceRequest(url="https://example.com/doc.pdf"),
            S3SourceRequest(
                endpoint="127.0.0.1:9000",
                verify_ssl=False,
                access_key=SecretStr("minioadmin"),
                secret_key=SecretStr("minioadmin"),
                bucket="test-bucket",
                key_prefix="incoming/",
            ),
        ],
        target=InBodyTarget(),
    )

    task_data = task.model_dump(mode="json", serialize_as_any=True)
    rebuilt_task = Task.model_validate(task_data)

    assert isinstance(rebuilt_task.sources[0], FileSource)
    assert isinstance(rebuilt_task.sources[1], HttpSource)
    assert isinstance(rebuilt_task.sources[2], S3Coordinates)


@pytest.mark.asyncio
async def test_rq_enqueue_preserves_s3coordinates(monkeypatch: pytest.MonkeyPatch):
    orchestrator = RQOrchestrator(config=RQOrchestratorConfig())
    captured: dict[str, object] = {}

    class FakeQueue:
        def enqueue(self, *args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs

    async def _noop(*args, **kwargs):
        return None

    orchestrator._rq_queue = FakeQueue()
    monkeypatch.setattr(orchestrator, "init_task_tracking", _noop)
    monkeypatch.setattr(orchestrator, "_store_task_in_redis", _noop)

    s3_source = _make_s3_source()
    task = await orchestrator.enqueue(
        sources=[s3_source],
        convert_options=None,
        target=InBodyTarget(),
    )

    assert len(task.sources) == 1
    assert isinstance(task.sources[0], S3Coordinates)

    task_data = captured["kwargs"]["kwargs"]["task_data"]
    assert len(task_data["sources"]) == 1
    assert task_data["sources"][0]["bucket"] == s3_source.bucket
    assert task_data["sources"][0]["key_prefix"] == s3_source.key_prefix


@pytest.mark.parametrize("job_config_cls", [LocalJobConfig, MultiprocJobConfig])
@pytest.mark.parametrize(
    ("source_payload", "expected_type"),
    [
        (
            {
                "kind": "file",
                "filename": "doc.pdf",
                "base64_string": base64.b64encode(b"pdf-bytes").decode(),
            },
            FileSourceRequest,
        ),
        (
            {
                "kind": "http",
                "url": "https://example.com/doc.pdf",
            },
            HttpSourceRequest,
        ),
        (
            {
                "kind": "s3",
                "endpoint": "127.0.0.1:9000",
                "verify_ssl": False,
                "access_key": "minioadmin",
                "secret_key": "minioadmin",
                "bucket": "test-bucket",
                "key_prefix": "incoming/",
            },
            S3SourceRequest,
        ),
    ],
)
def test_cli_config_parsing_keeps_kind_compatibility(
    job_config_cls, source_payload, expected_type
):
    config = job_config_cls.model_validate(
        {
            "sources": [source_payload],
            "target": {"kind": "local_path", "path": "."},
        }
    )

    assert isinstance(config.sources[0], expected_type)


def test_expand_task_sources_preserves_file_and_headers():
    encoded = base64.b64encode(b"pdf-bytes").decode()
    file_source = FileSource(filename="doc.pdf", base64_string=encoded)
    http_source = HttpSource(
        url="https://example.com/doc.pdf",
        headers={"Authorization": "Bearer test-token"},
    )
    task = Task(
        task_id="task-1",
        task_type=TaskType.CONVERT,
        sources=[file_source, http_source],
        target=InBodyTarget(),
    )

    convert_sources, headers = expand_task_sources(task)

    assert headers == {"Authorization": "Bearer test-token"}
    assert len(convert_sources) == 2
    assert isinstance(convert_sources[0], DocumentStream)
    assert convert_sources[0].name == "doc.pdf"
    assert convert_sources[1] == "https://example.com/doc.pdf"
