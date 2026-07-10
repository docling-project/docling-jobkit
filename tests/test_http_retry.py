"""Tests for retrying HTTP source materialization (docling_jobkit.convert.http_retry)."""

import httpx
import pytest

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.service.sources import HttpSource
from docling.datamodel.service.tasks import TaskType
from docling_core.types.io import DocumentStream

from docling_jobkit.convert import http_retry
from docling_jobkit.convert.materialization import (
    SourceFetchError,
    SourceLimitExceededError,
)
from docling_jobkit.convert.source_expansion import expand_task_sources
from docling_jobkit.datamodel.task import Task
from docling_jobkit.orchestrators.rq import worker

SOURCE = HttpSource(url="http://example.com/doc.pdf")


def _fetch_error(status: int) -> SourceFetchError:
    """A SourceFetchError chained from an httpx HTTP status error (as the real fetch raises)."""
    request = httpx.Request("GET", "http://example.com/doc.pdf")
    response = httpx.Response(status, request=request)
    cause = httpx.HTTPStatusError(f"{status}", request=request, response=response)
    err = SourceFetchError(f"Source 'doc.pdf' could not be downloaded: {cause}")
    err.__cause__ = cause
    return err


def _connect_error() -> SourceFetchError:
    request = httpx.Request("GET", "http://example.com/doc.pdf")
    cause = httpx.ConnectError("connection refused", request=request)
    err = SourceFetchError(f"Source 'doc.pdf' could not be downloaded: {cause}")
    err.__cause__ = cause
    return err


# --------------------------------------------------------------------------- #
# fetch_http_source_with_retry                                                 #
# --------------------------------------------------------------------------- #
@pytest.fixture
def spy(monkeypatch):
    """Patch the async fetch with a controllable stub and capture attempts + sleeps."""
    state = {"attempts": 0, "sleeps": []}

    def install(*, fail_with=None, fail_times=0, payload=b"PDFBYTES"):
        async def fake_fetch(source, *, max_file_size):
            state["attempts"] += 1
            if state["attempts"] <= fail_times:
                raise fail_with
            return payload

        monkeypatch.setattr(http_retry, "fetch_http_source_bytes_async", fake_fetch)

    state["install"] = install
    state["sleep"] = lambda delay: state["sleeps"].append(delay)
    return state


def _run(spy, **kwargs):
    return http_retry.fetch_http_source_with_retry(
        SOURCE,
        max_file_size=None,
        task_id="t1",
        sleep=spy["sleep"],
        **kwargs,
    )


def test_success_first_attempt(spy):
    spy["install"](fail_times=0, payload=b"PDFBYTES")
    result = _run(spy, max_retries=3, retry_delay=5.0)
    assert isinstance(result, DocumentStream)
    assert result.name == "doc.pdf"
    assert result.stream.read() == b"PDFBYTES"
    assert spy["attempts"] == 1
    assert spy["sleeps"] == []


def test_retryable_503_retries_then_succeeds(spy):
    spy["install"](fail_with=_fetch_error(503), fail_times=2, payload=b"OK")
    result = _run(spy, max_retries=3, retry_delay=2.0)
    assert isinstance(result, DocumentStream)
    assert spy["attempts"] == 3  # 1 initial + 2 retries
    assert spy["sleeps"] == [2.0, 2.0]


def test_retryable_503_exhausts_and_raises(spy):
    spy["install"](fail_with=_fetch_error(503), fail_times=99)
    with pytest.raises(SourceFetchError):
        _run(spy, max_retries=3, retry_delay=1.0)
    assert spy["attempts"] == 4  # 1 initial + 3 retries
    assert spy["sleeps"] == [1.0, 1.0, 1.0]


def test_connection_error_is_retried(spy):
    spy["install"](fail_with=_connect_error(), fail_times=99)
    with pytest.raises(SourceFetchError):
        _run(spy, max_retries=2, retry_delay=0.5)
    assert spy["attempts"] == 3
    assert spy["sleeps"] == [0.5, 0.5]


def test_non_retryable_404_fails_immediately(spy):
    spy["install"](fail_with=_fetch_error(404), fail_times=99)
    with pytest.raises(SourceFetchError):
        _run(spy, max_retries=5, retry_delay=5.0)
    assert spy["attempts"] == 1  # no retries for a permanent status
    assert spy["sleeps"] == []


def test_oversize_is_not_retried(spy):
    spy["install"](fail_with=SourceLimitExceededError("too big"), fail_times=99)
    with pytest.raises(SourceLimitExceededError):
        _run(spy, max_retries=5, retry_delay=5.0)
    assert spy["attempts"] == 1
    assert spy["sleeps"] == []


def test_zero_retries_disables_retrying(spy):
    spy["install"](fail_with=_fetch_error(503), fail_times=99)
    with pytest.raises(SourceFetchError):
        _run(spy, max_retries=0, retry_delay=5.0)
    assert spy["attempts"] == 1
    assert spy["sleeps"] == []


# --------------------------------------------------------------------------- #
# build_http_failure_document                                                  #
# --------------------------------------------------------------------------- #
def test_build_http_failure_document_records_cause():
    doc = http_retry.build_http_failure_document(
        SOURCE, _fetch_error(503), source_index=2
    )
    assert doc.status == ConversionStatus.FAILURE
    assert doc.source_index == 2
    assert doc.source_uri == "http://example.com/doc.pdf"
    # The cause is recorded (unlike the silent-drop path, which left errors empty).
    assert doc.errors
    assert "503" in doc.errors[0].error_message


# --------------------------------------------------------------------------- #
# expand_task_sources                                                          #
# --------------------------------------------------------------------------- #
def test_expand_task_sources_uses_materializer():
    """HttpSource is materialized via the callback; origins are tracked."""
    task = Task(task_id="t", task_type=TaskType.CONVERT, sources=[SOURCE])
    sentinel = DocumentStream(name="doc.pdf", stream=__import__("io").BytesIO(b"X"))
    seen = []

    def materializer(src):
        seen.append(src)
        return sentinel

    convert_sources, _headers, source_indices = expand_task_sources(
        task, http_materializer=materializer
    )
    assert convert_sources == [sentinel]
    assert source_indices == [0]
    assert seen == [SOURCE]


def test_expand_task_sources_skips_failed_materialization():
    """A None from the materializer skips the source (no converter input)."""
    task = Task(task_id="t", task_type=TaskType.CONVERT, sources=[SOURCE])
    convert_sources, _headers, source_indices = expand_task_sources(
        task, http_materializer=lambda _src: None
    )
    assert convert_sources == []
    assert source_indices == []


def test_expand_task_sources_default_passes_url_string():
    """Without a materializer, behavior is unchanged (URL string + headers)."""
    src = HttpSource(
        url="http://example.com/doc.pdf", headers={"authorization": "Bearer x"}
    )
    task = Task(task_id="t", task_type=TaskType.CONVERT, sources=[src])
    convert_sources, headers, source_indices = expand_task_sources(task)
    assert convert_sources == ["http://example.com/doc.pdf"]
    assert headers == {"authorization": "Bearer x"}
    assert source_indices == [0]


# --------------------------------------------------------------------------- #
# RQ worker _prepare_convert_sources (end-to-end wiring)                        #
# --------------------------------------------------------------------------- #
def test_worker_prepare_convert_sources_materializes_http(monkeypatch):
    """The RQ worker fetches HTTP sources with the retry config."""
    attempts = {"n": 0}

    async def fake_fetch(source, *, max_file_size):
        attempts["n"] += 1
        if attempts["n"] <= 1:  # one transient failure, then success
            raise _fetch_error(503)
        return b"PDFBYTES"

    monkeypatch.setattr(http_retry, "fetch_http_source_bytes_async", fake_fetch)
    monkeypatch.setattr(http_retry.time, "sleep", lambda _d: None)

    task = Task(task_id="t", task_type=TaskType.CONVERT, sources=[SOURCE])
    prepared = worker._prepare_convert_sources(
        task, max_file_size=None, max_task_retries=3, retry_delay=0.0
    )

    assert attempts["n"] == 2  # retried once
    assert len(prepared.convert_sources) == 1
    assert isinstance(prepared.convert_sources[0], DocumentStream)
    assert prepared.source_indices == [0]
    assert prepared.materialization_failures == []
    assert prepared.source_info[0]["type"] == "HttpSource"


def test_worker_terminal_failure_becomes_document_failure(monkeypatch):
    """A non-retryable fetch failure is recorded as a document FAILURE (not a raise)."""

    async def fake_fetch(source, *, max_file_size):
        raise _fetch_error(404)

    monkeypatch.setattr(http_retry, "fetch_http_source_bytes_async", fake_fetch)

    task = Task(task_id="t", task_type=TaskType.CONVERT, sources=[SOURCE])
    prepared = worker._prepare_convert_sources(
        task, max_file_size=None, max_task_retries=3, retry_delay=0.0
    )

    assert prepared.convert_sources == []  # nothing handed to the converter
    assert len(prepared.materialization_failures) == 1
    failed = prepared.materialization_failures[0]
    assert failed.status == ConversionStatus.FAILURE
    assert failed.source_uri == "http://example.com/doc.pdf"
    assert failed.errors  # cause recorded


def test_worker_batch_records_per_document_failure(monkeypatch):
    """In a batch, one unfetchable source fails per-document; others still convert."""
    good = HttpSource(url="http://example.com/good.pdf")
    bad = HttpSource(url="http://example.com/bad.pdf")

    async def fake_fetch(source, *, max_file_size):
        if "bad" in str(source.url):
            raise _fetch_error(404)
        return b"GOODBYTES"

    monkeypatch.setattr(http_retry, "fetch_http_source_bytes_async", fake_fetch)

    task = Task(task_id="t", task_type=TaskType.CONVERT, sources=[good, bad])
    prepared = worker._prepare_convert_sources(
        task, max_file_size=None, max_task_retries=1, retry_delay=0.0
    )

    # Good source is materialized for conversion, mapped back to origin index 0.
    assert len(prepared.convert_sources) == 1
    assert isinstance(prepared.convert_sources[0], DocumentStream)
    assert prepared.source_indices == [0]
    # Bad source is recorded as a document-level failure at origin index 1.
    assert len(prepared.materialization_failures) == 1
    failed = prepared.materialization_failures[0]
    assert failed.status == ConversionStatus.FAILURE
    assert failed.source_index == 1
    assert failed.source_uri == "http://example.com/bad.pdf"
    assert failed.errors
