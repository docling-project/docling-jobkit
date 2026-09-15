from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
import requests

from docling_jobkit.connectors.databricks_volumes import helper as dbvol_helper
from docling_jobkit.connectors.databricks_volumes.helper import (
    _with_source_retry,
    download_document,
    iter_directory,
    list_directory_page,
)
from docling_jobkit.connectors.errors import (
    SourceConnectorAuthenticationError,
    SourceConnectorPolicyError,
    SourceConnectorUnavailableError,
)
from docling_jobkit.convert.materialization import SourceLimitExceededError


def _make_http_exc(status_code: int) -> requests.HTTPError:
    response = MagicMock()
    response.status_code = status_code
    return requests.HTTPError(response=response)


@pytest.mark.parametrize(
    "transient_exc",
    [
        requests.Timeout("timed out"),
        requests.ConnectionError("connection refused"),
        _make_http_exc(503),
        _make_http_exc(429),
    ],
)
def test_exp_backoff_retries_on_transient_error_then_succeeds(
    transient_exc: Any,
) -> None:
    fn = MagicMock(side_effect=[transient_exc, "ok"])
    with patch("docling_jobkit.connectors.databricks_volumes.helper.time.sleep"):
        assert _with_source_retry(fn, "op") == "ok"

    assert fn.call_count == 2


def test_exp_backoff_raises_policy_error_immediately_on_4xx() -> None:
    fn = MagicMock(side_effect=_make_http_exc(404))
    with patch("docling_jobkit.connectors.databricks_volumes.helper.time.sleep"):
        with pytest.raises(SourceConnectorPolicyError):
            _with_source_retry(fn, "op")

    fn.assert_called_once()


def test_exp_backoff_does_not_retry_auth_failures() -> None:
    fn = MagicMock(side_effect=_make_http_exc(403))
    with patch("docling_jobkit.connectors.databricks_volumes.helper.time.sleep"):
        with pytest.raises(SourceConnectorAuthenticationError):
            _with_source_retry(fn, "op")

    fn.assert_called_once()


def test_exp_backoff_exhausts_retries_then_raises_unavailable() -> None:
    fn = MagicMock(side_effect=requests.Timeout("timed out"))
    with patch(
        "docling_jobkit.connectors.databricks_volumes.helper.time.sleep"
    ) as mock_sleep:
        with pytest.raises(SourceConnectorUnavailableError):
            _with_source_retry(fn, "op")

    assert mock_sleep.call_args_list == [call(0.5), call(1.0), call(2.0)]


def test_errors_are_attributed_to_databricks_volumes() -> None:
    fn = MagicMock(side_effect=_make_http_exc(404))
    with patch("docling_jobkit.connectors.databricks_volumes.helper.time.sleep"):
        with pytest.raises(SourceConnectorPolicyError) as exc_info:
            _with_source_retry(fn, "op")

    assert exc_info.value.source_kind == "databricks_volumes"


def _page(entries: list[dict], next_token: str | None) -> dict:
    return {"contents": entries, "next_page_token": next_token}


def test_list_directory_page_sends_page_token_when_present(monkeypatch) -> None:
    captured_kwargs = {}

    def _get(*args, **kwargs):
        captured_kwargs.update(kwargs)
        response = MagicMock()
        response.json.return_value = _page([], None)
        return response

    monkeypatch.setattr(
        "docling_jobkit.connectors.databricks_volumes.helper.requests.get", _get
    )

    list_directory_page("host", "tok", "/Volumes/main/default/docs", page_token="tok-1")

    assert captured_kwargs["params"]["page_token"] == "tok-1"
    assert captured_kwargs["headers"]["Authorization"] == "Bearer tok"


def test_iter_directory_paginates_across_pages() -> None:
    with patch.object(
        dbvol_helper,
        "list_directory_page",
        side_effect=[
            _page([{"name": "a.pdf", "path": "/Volumes/x/a.pdf"}], "tok-1"),
            _page([{"name": "b.pdf", "path": "/Volumes/x/b.pdf"}], "tok-2"),
            _page([{"name": "c.pdf", "path": "/Volumes/x/c.pdf"}], None),
        ],
    ) as mock_page:
        entries = list(iter_directory("host", "tok", "/Volumes/x"))

    assert [e["name"] for e in entries] == ["a.pdf", "b.pdf", "c.pdf"]
    assert mock_page.call_count == 3
    assert mock_page.call_args_list[1].kwargs["page_token"] == "tok-1"
    assert mock_page.call_args_list[2].kwargs["page_token"] == "tok-2"


def test_iter_directory_stops_without_next_page_token() -> None:
    with patch.object(
        dbvol_helper,
        "list_directory_page",
        side_effect=[_page([{"name": "a.pdf", "path": "/Volumes/x/a.pdf"}], None)],
    ) as mock_page:
        entries = list(iter_directory("host", "tok", "/Volumes/x"))

    assert [e["name"] for e in entries] == ["a.pdf"]
    assert mock_page.call_count == 1


def _fake_streamed_response(
    chunks: list[bytes], *, status_code: int = 200
) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.iter_content.return_value = iter(chunks)
    return response


def test_download_document_builds_files_api_url_and_streams_chunks(monkeypatch):
    response = _fake_streamed_response([b"PDF-", b"bytes"])
    captured_kwargs: dict = {}
    captured_args: list = []

    def _get(*args, **kwargs):
        captured_args.extend(args)
        captured_kwargs.update(kwargs)
        return response

    monkeypatch.setattr(
        "docling_jobkit.connectors.databricks_volumes.helper.requests.get", _get
    )

    buffer = download_document("files.databricks.com", "tok", "/Volumes/x/doc.pdf")

    assert buffer.read() == b"PDF-bytes"
    assert (
        captured_args[0]
        == "https://files.databricks.com/api/2.0/fs/files/Volumes/x/doc.pdf"
    )
    assert captured_kwargs["headers"]["Authorization"] == "Bearer tok"


def test_download_document_requests_streaming_response_no_redirects(monkeypatch):
    """stream=True is required so the response body isn't fully materialized
    by requests itself before our bounded loop ever sees it; allow_redirects
    must be False so a same-host response can't silently redirect off-host
    and carry the Authorization header with it."""
    response = _fake_streamed_response([b"PDF"])
    captured_kwargs = {}

    def _get(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return response

    monkeypatch.setattr(
        "docling_jobkit.connectors.databricks_volumes.helper.requests.get", _get
    )

    download_document("files.databricks.com", "tok", "/Volumes/x/doc.pdf")

    assert captured_kwargs["stream"] is True
    assert captured_kwargs["allow_redirects"] is False


def test_download_document_enforces_max_file_size(monkeypatch):
    response = _fake_streamed_response([b"0" * 5, b"0" * 5, b"0" * 5])
    monkeypatch.setattr(
        "docling_jobkit.connectors.databricks_volumes.helper.requests.get",
        lambda *a, **k: response,
    )

    with pytest.raises(SourceLimitExceededError):
        download_document(
            "files.databricks.com", "tok", "/Volumes/x/doc.pdf", max_file_size=8
        )


def test_download_document_no_limit_allows_large_body(monkeypatch):
    response = _fake_streamed_response([b"0" * 1000])
    monkeypatch.setattr(
        "docling_jobkit.connectors.databricks_volumes.helper.requests.get",
        lambda *a, **k: response,
    )

    buffer = download_document("files.databricks.com", "tok", "/Volumes/x/doc.pdf")
    assert len(buffer.read()) == 1000


def test_download_document_rejects_redirect(monkeypatch):
    response = _fake_streamed_response([], status_code=302)
    monkeypatch.setattr(
        "docling_jobkit.connectors.databricks_volumes.helper.requests.get",
        lambda *a, **k: response,
    )

    with pytest.raises(SourceConnectorPolicyError, match="redirect"):
        download_document("files.databricks.com", "tok", "/Volumes/x/doc.pdf")


# --- sanitized error messages -------------------------------------------------


@pytest.mark.parametrize("status", [401, 403])
def test_source_auth_status_raises_authentication_error(status: int) -> None:
    """401/403 must classify as an authentication failure, like every other
    source connector, instead of a generic policy error."""
    fn = MagicMock(side_effect=_make_http_exc(status))
    with patch("docling_jobkit.connectors.databricks_volumes.helper.time.sleep"):
        with pytest.raises(SourceConnectorAuthenticationError) as exc_info:
            _with_source_retry(fn, "op")

    assert exc_info.value.source_kind == "databricks_volumes"
    assert not exc_info.value.retryable


@pytest.mark.parametrize("status", [401, 403, 404, 422, 400])
def test_source_errors_never_echo_the_request_url(status: int) -> None:
    """str(requests.HTTPError) embeds the full request URL, and
    build_public_task_error surfaces SourceConnectorError text verbatim to API
    clients — so the workspace host must not appear in the message."""
    response = MagicMock()
    response.status_code = status
    exc = requests.HTTPError(
        f"{status} Client Error: for url: "
        "https://dbc-secret.cloud.databricks.com/api/2.0/fs/files/Volumes/x",
        response=response,
    )
    fn = MagicMock(side_effect=exc)

    with patch("docling_jobkit.connectors.databricks_volumes.helper.time.sleep"):
        with pytest.raises(Exception) as exc_info:
            _with_source_retry(fn, "op")

    message = str(exc_info.value)
    assert "dbc-secret" not in message
    assert "https://" not in message


def test_exhausted_http_error_does_not_echo_the_request_url() -> None:
    response = MagicMock()
    response.status_code = 503
    exc = requests.HTTPError(
        "503 Server Error for url: https://dbc-secret.cloud.databricks.com/x",
        response=response,
    )
    fn = MagicMock(side_effect=exc)

    with patch("docling_jobkit.connectors.databricks_volumes.helper.time.sleep"):
        with pytest.raises(SourceConnectorUnavailableError) as exc_info:
            _with_source_retry(fn, "op")

    assert "dbc-secret" not in str(exc_info.value)


# --- error translation boundary -----------------------------------------------


@pytest.mark.parametrize(
    "transient_exc",
    [
        requests.exceptions.ChunkedEncodingError("peer closed"),
        requests.exceptions.ContentDecodingError("bad gzip"),
    ],
)
def test_streaming_transport_errors_are_retried(transient_exc: Any) -> None:
    """A connection dropped while reading the body used to escape as a raw
    requests exception (reported to the client as an internal error) because the
    iter_content loop ran after the retry helper had returned."""
    fn = MagicMock(side_effect=[transient_exc, "ok"])
    with patch("docling_jobkit.connectors.databricks_volumes.helper.time.sleep"):
        assert _with_source_retry(fn, "op") == "ok"

    assert fn.call_count == 2


def test_download_retries_when_the_body_stream_breaks(monkeypatch):
    broken = MagicMock()
    broken.status_code = 200
    broken.iter_content.side_effect = requests.exceptions.ChunkedEncodingError("drop")
    good = _fake_streamed_response([b"PDF-bytes"])
    responses = iter([broken, good])

    monkeypatch.setattr(
        "docling_jobkit.connectors.databricks_volumes.helper.requests.get",
        lambda *a, **k: next(responses),
    )
    monkeypatch.setattr(
        "docling_jobkit.connectors.databricks_volumes.helper.time.sleep",
        lambda _: None,
    )

    buffer = download_document("files.databricks.com", "tok", "/Volumes/x/doc.pdf")

    assert buffer.read() == b"PDF-bytes"
    broken.close.assert_called_once()


def test_download_closes_response_when_size_limit_aborts(monkeypatch):
    """Aborting mid-stream must not strand the socket."""
    response = _fake_streamed_response([b"0" * 5, b"0" * 5, b"0" * 5])
    monkeypatch.setattr(
        "docling_jobkit.connectors.databricks_volumes.helper.requests.get",
        lambda *a, **k: response,
    )

    with pytest.raises(SourceLimitExceededError):
        download_document(
            "files.databricks.com", "tok", "/Volumes/x/doc.pdf", max_file_size=8
        )

    response.close.assert_called_once()


def test_download_closes_response_on_success(monkeypatch):
    response = _fake_streamed_response([b"PDF"])
    monkeypatch.setattr(
        "docling_jobkit.connectors.databricks_volumes.helper.requests.get",
        lambda *a, **k: response,
    )

    download_document("files.databricks.com", "tok", "/Volumes/x/doc.pdf")

    response.close.assert_called_once()


def test_download_redirect_message_omits_the_workspace_host(monkeypatch):
    response = _fake_streamed_response([], status_code=302)
    monkeypatch.setattr(
        "docling_jobkit.connectors.databricks_volumes.helper.requests.get",
        lambda *a, **k: response,
    )

    with pytest.raises(SourceConnectorPolicyError) as exc_info:
        download_document("dbc-secret.cloud.databricks.com", "tok", "/Volumes/x/d.pdf")

    assert "dbc-secret" not in str(exc_info.value)


def test_list_directory_page_translates_malformed_json(monkeypatch):
    response = MagicMock()
    response.json.side_effect = ValueError("not json")
    monkeypatch.setattr(
        "docling_jobkit.connectors.databricks_volumes.helper.requests.get",
        lambda *a, **k: response,
    )

    with pytest.raises(SourceConnectorUnavailableError, match="malformed"):
        list_directory_page("host", "tok", "/Volumes/main/default/docs")
