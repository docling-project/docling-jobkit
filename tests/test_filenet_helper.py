from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
import requests

from docling_jobkit.connectors import filenet_helper
from docling_jobkit.connectors.filenet_helper import (
    _execute_graphql_query,
    _with_exponential_retry,
    list_folder_documents,
    list_repository_documents,
)

# _with_exponential_retry tests


def _make_http_exc(status_code: int) -> requests.HTTPError:
    """helper to create the http exception with the specified status code"""
    response = MagicMock()
    response.status_code = status_code

    return requests.HTTPError(response=response)


@pytest.mark.parametrize(
    "transient_exc",
    [
        requests.Timeout("timed out"),
        requests.ConnectionError("connection refused"),
        _make_http_exc(503),  # 5xx errors are transient
        _make_http_exc(429),  # 429 is transient: rate-limit
    ],
)
def test_exp_backoff_retries_on_transient_error_then_succeeds(
    transient_exc: Any,
) -> None:
    """It should not fail immediately and retry if transient"""
    fn = MagicMock(side_effect=[transient_exc, "ok"])
    with patch("docling_jobkit.connectors.filenet_helper.time.sleep"):
        assert _with_exponential_retry(fn, "op") == "ok"

    assert fn.call_count == 2


def test_exp_backoff_raises_immediately_on_4xx() -> None:
    """Any 4xx error (except 429) is not considered a transient error so it should raise immediately"""
    fn = MagicMock(side_effect=_make_http_exc(403))
    with patch("docling_jobkit.connectors.filenet_helper.time.sleep"):
        with pytest.raises(requests.HTTPError):
            _with_exponential_retry(fn, "op")

    fn.assert_called_once()


def test_exp_backoff_sequence() -> None:
    """It should do proper exp backoff wait properly (increases each time)"""
    fn = MagicMock(side_effect=requests.Timeout("timed out"))
    with patch("docling_jobkit.connectors.filenet_helper.time.sleep") as mock_sleep:
        with pytest.raises(requests.Timeout):
            _with_exponential_retry(fn, "op")

    assert mock_sleep.call_args_list == [call(0.5), call(1.0), call(2.0)]


# _execute_graphql_query tests


def test_graphql_errors_payload_raises_immediately_without_retry() -> None:
    """GraphQL 200 response with errors key is a perm error and shouldn't retry"""
    ok_res = MagicMock()
    ok_res.json.return_value = {"errors": [{"message": "Invalid identifier"}]}

    with patch("docling_jobkit.connectors.filenet_helper.time.sleep") as mock_sleep:
        with patch(
            "docling_jobkit.connectors.filenet_helper.requests.post",
            return_value=ok_res,
        ):
            with pytest.raises(RuntimeError, match="GraphQL query failed"):
                _execute_graphql_query(
                    "https://{host}/content-services-graphql", "auth", "query { }"
                )

    mock_sleep.assert_not_called()


# pagination tests


def _document_set(ids: list[str], token: str | None) -> dict:
    return {
        "documents": [{"id": i, "name": f"{i}.pdf"} for i in ids],
        "pageInfo": {"token": token},
    }


def test_list_repository_documents_paginates_across_pages() -> None:
    with patch.object(
        filenet_helper,
        "_execute_graphql_query",
        side_effect=[
            {"documents": _document_set(["1"], "tok-1")},
            {"moreDocuments": _document_set(["2"], "tok-2")},
            {"moreDocuments": _document_set(["3"], None)},
        ],
    ) as mock_query:
        docs = list(list_repository_documents(MagicMock(), "auth", page_size=1))

    assert [d["id"] for d in docs] == ["1", "2", "3"]  # ordering
    assert mock_query.call_count == 3

    assert mock_query.call_args_list[1].args[3] == {"token": "tok-1"}
    assert mock_query.call_args_list[2].args[3] == {"token": "tok-2"}


def test_list_folder_documents_paginates_across_pages() -> None:
    with patch.object(
        filenet_helper,
        "_execute_graphql_query",
        side_effect=[
            {"folder": {"containedDocuments": _document_set(["1"], "tok-1")}},
            {"moreDocuments": _document_set(["2"], None)},
        ],
    ) as mock_query:
        docs = list(list_folder_documents(MagicMock(), "auth", "/folder"))

    assert [d["id"] for d in docs] == ["1", "2"]
    assert mock_query.call_count == 2

    assert mock_query.call_args_list[1].args[3] == {"token": "tok-1"}


def test_pagination_stops_on_empty_token_without_extra_calls() -> None:
    with patch.object(
        filenet_helper,
        "_execute_graphql_query",
        side_effect=[{"documents": _document_set(["1", "2"], None)}],
    ) as mock_query:
        docs = list(list_repository_documents(MagicMock(), "auth"))

    assert [d["id"] for d in docs] == ["1", "2"]
    assert mock_query.call_count == 1
