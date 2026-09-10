from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
import requests

from docling_jobkit.connectors.databricks_volumes import helper as dbvol_helper
from docling_jobkit.connectors.databricks_volumes.helper import (
    _with_exponential_retry,
    iter_directory,
    list_directory_page,
)
from docling_jobkit.connectors.errors import (
    SourceConnectorPolicyError,
    SourceConnectorUnavailableError,
)


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
        assert _with_exponential_retry(fn, "op") == "ok"

    assert fn.call_count == 2


def test_exp_backoff_raises_policy_error_immediately_on_4xx() -> None:
    fn = MagicMock(side_effect=_make_http_exc(403))
    with patch("docling_jobkit.connectors.databricks_volumes.helper.time.sleep"):
        with pytest.raises(SourceConnectorPolicyError):
            _with_exponential_retry(fn, "op")

    fn.assert_called_once()


def test_exp_backoff_exhausts_retries_then_raises_unavailable() -> None:
    fn = MagicMock(side_effect=requests.Timeout("timed out"))
    with patch(
        "docling_jobkit.connectors.databricks_volumes.helper.time.sleep"
    ) as mock_sleep:
        with pytest.raises(SourceConnectorUnavailableError):
            _with_exponential_retry(fn, "op")

    assert mock_sleep.call_args_list == [call(0.5), call(1.0), call(2.0)]


def test_errors_are_attributed_to_databricks_volumes() -> None:
    fn = MagicMock(side_effect=_make_http_exc(404))
    with patch("docling_jobkit.connectors.databricks_volumes.helper.time.sleep"):
        with pytest.raises(SourceConnectorPolicyError) as exc_info:
            _with_exponential_retry(fn, "op")

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
