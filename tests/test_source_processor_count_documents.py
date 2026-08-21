"""Tests for count_documents() wiring across source processors and the orchestrator helper.

Covers:
- BaseSourceProcessor.count_documents() public method (initialization guard)
- _count_documents_for_task() in serve_deployment correctly sums across sources
- PassthroughTaskRequest carries expected_doc_count
- FileNet _count_documents() document_ids multi-ID fix
"""

import os
from io import BytesIO
from typing import Iterator

import pytest

pytest.importorskip("ray")
if os.getenv("CI"):
    pytest.skip("Skipping Ray tests in CI", allow_module_level=True)

from docling.datamodel.base_models import DocumentStream
from docling.datamodel.service.tasks import TaskType

from docling_jobkit.connectors.source_processor import BaseSourceProcessor
from docling_jobkit.datamodel.task import Task
from docling_jobkit.orchestrators.ray.models import PassthroughTaskRequest
from docling_jobkit.orchestrators.ray.serve_deployment import _count_documents_for_task

# ---------------------------------------------------------------------------
# Minimal concrete processor for unit tests
# ---------------------------------------------------------------------------


class _FixedCountProcessor(BaseSourceProcessor):
    """Processor that reports a fixed pre-known document count."""

    def __init__(self, count: int):
        super().__init__(count)
        self._count = count

    def _initialize(self): ...

    def _finalize(self): ...

    def _fetch_documents(self, *, max_file_size=None) -> Iterator[DocumentStream]:
        for i in range(self._count):
            yield DocumentStream(name=f"doc_{i}.pdf", stream=BytesIO(b"pdf"))

    def _count_documents(self) -> int:
        return self._count


class _NoneCountProcessor(_FixedCountProcessor):
    """Processor that cannot pre-count."""

    def _count_documents(self) -> int | None:
        return None


# ---------------------------------------------------------------------------
# BaseSourceProcessor.count_documents() public method
# ---------------------------------------------------------------------------


def test_count_documents_requires_initialization():
    """count_documents() must raise before the context manager is entered."""
    proc = _FixedCountProcessor(5)
    with pytest.raises(RuntimeError, match="not initialized"):
        proc.count_documents()


def test_count_documents_returns_value_when_initialized():
    """count_documents() delegates to _count_documents() when open."""
    with _FixedCountProcessor(7) as proc:
        assert proc.count_documents() == 7


def test_count_documents_returns_none_when_not_implemented():
    """count_documents() returns None when _count_documents() returns None."""
    with _NoneCountProcessor(3) as proc:
        assert proc.count_documents() is None


# ---------------------------------------------------------------------------
# _count_documents_for_task()
# ---------------------------------------------------------------------------


def _make_task_with_s3_sources(n: int = 1) -> Task:
    """Build a Task with *n* S3 sources (real registered BaseModel types)."""
    from docling.datamodel.service.requests import S3SourceRequest
    from docling.datamodel.service.targets import InBodyTarget

    sources = [
        S3SourceRequest(
            endpoint="127.0.0.1:9000",
            verify_ssl=False,
            access_key="key",
            secret_key="secret",
            bucket="bucket",
            key_prefix=f"prefix-{i}/",
        )
        for i in range(n)
    ]
    return Task(
        task_id="test-task",
        task_type=TaskType.CONVERT,
        sources=sources,
        target=InBodyTarget(),
    )


def test_count_documents_for_task_single_source(monkeypatch):
    proc = _FixedCountProcessor(4)
    monkeypatch.setattr(
        "docling_jobkit.orchestrators.ray.serve_deployment.get_source_processor",
        lambda source, **kwargs: proc,
    )
    task = _make_task_with_s3_sources(1)
    assert _count_documents_for_task(task) == 4


def test_count_documents_for_task_returns_none_when_source_cannot_count(monkeypatch):
    proc = _NoneCountProcessor(3)
    monkeypatch.setattr(
        "docling_jobkit.orchestrators.ray.serve_deployment.get_source_processor",
        lambda source, **kwargs: proc,
    )
    task = _make_task_with_s3_sources(1)
    assert _count_documents_for_task(task) is None


def test_count_documents_for_task_sums_multiple_sources(monkeypatch):
    """When task has multiple sources, counts are summed."""
    calls = iter([_FixedCountProcessor(3), _FixedCountProcessor(5)])
    monkeypatch.setattr(
        "docling_jobkit.orchestrators.ray.serve_deployment.get_source_processor",
        lambda source, **kwargs: next(calls),
    )
    task = _make_task_with_s3_sources(2)
    assert _count_documents_for_task(task) == 8


def test_count_documents_for_task_document_stream_counts_as_one(monkeypatch):
    """Raw DocumentStream sources count as 1 each without opening a processor."""
    opened = []

    class _TrackedProcessor(_FixedCountProcessor):
        def _initialize(self):
            opened.append(True)

    monkeypatch.setattr(
        "docling_jobkit.orchestrators.ray.serve_deployment.get_source_processor",
        lambda source, **kwargs: _TrackedProcessor(99),
    )
    from docling.datamodel.service.targets import InBodyTarget

    stream = DocumentStream(name="pre-materialized.pdf", stream=BytesIO(b"pdf"))
    task = Task(
        task_id="test-task",
        task_type=TaskType.CONVERT,
        sources=[stream],
        target=InBodyTarget(),
    )
    result = _count_documents_for_task(task)
    assert result == 1
    assert opened == [], "Processor must not be opened for DocumentStream sources"


def test_count_documents_for_task_empty_task():
    """Empty source list returns None."""
    from docling.datamodel.service.targets import InBodyTarget

    task = Task(
        task_id="t",
        task_type=TaskType.CONVERT,
        sources=[],
        target=InBodyTarget(),
    )
    assert _count_documents_for_task(task) is None


# ---------------------------------------------------------------------------
# PassthroughTaskRequest carries expected_doc_count
# ---------------------------------------------------------------------------


def test_passthrough_request_carries_none_by_default():
    from docling.datamodel.service.targets import InBodyTarget

    task = Task(
        task_id="t",
        task_type=TaskType.CONVERT,
        sources=[],
        target=InBodyTarget(),
    )
    req = PassthroughTaskRequest(task=task)
    assert req.expected_doc_count is None


def test_passthrough_request_carries_explicit_count():
    from docling.datamodel.service.targets import InBodyTarget

    task = Task(
        task_id="t",
        task_type=TaskType.CONVERT,
        sources=[],
        target=InBodyTarget(),
    )
    req = PassthroughTaskRequest(task=task, expected_doc_count=42)
    assert req.expected_doc_count == 42


# ---------------------------------------------------------------------------
# FileNet _count_documents() multi-ID fix
# ---------------------------------------------------------------------------


def test_filenet_count_documents_multi_id():
    """_count_documents() must return the number of IDs, not always 1."""
    from pydantic import SecretStr

    from docling_jobkit.connectors.filenet.models import FileNetCoordinates
    from docling_jobkit.connectors.filenet.source_processor import (
        FileNetSourceProcessor,
    )

    coords = FileNetCoordinates(
        base_url="https://filenet.example.com",
        username="user",
        api_key=SecretStr("key"),
        repository_id="repo",
        document_ids=["{id-1}", "{id-2}", "{id-3}"],
    )
    processor = FileNetSourceProcessor(coords)
    # Bypass _initialize (network call) — we only test the counting logic
    processor._auth_header = "Basic dGVzdA=="
    processor._graphql_url = "https://filenet.example.com/graphql"
    processor._initialized = True
    assert processor.count_documents() == 3


def test_filenet_count_documents_multi_id_respects_max():
    from pydantic import SecretStr

    from docling_jobkit.connectors.filenet.models import FileNetCoordinates
    from docling_jobkit.connectors.filenet.source_processor import (
        FileNetSourceProcessor,
    )

    coords = FileNetCoordinates(
        base_url="https://filenet.example.com",
        username="user",
        api_key=SecretStr("key"),
        repository_id="repo",
        document_ids=["{id-1}", "{id-2}", "{id-3}"],
        max_num_elements=2,
    )
    processor = FileNetSourceProcessor(coords)
    processor._auth_header = "Basic dGVzdA=="
    processor._graphql_url = "https://filenet.example.com/graphql"
    processor._initialized = True
    assert processor.count_documents() == 2
