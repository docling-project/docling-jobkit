from unittest.mock import MagicMock, patch

from docling_jobkit.connectors import filenet_helper
from docling_jobkit.connectors.filenet_helper import (
    list_folder_documents,
    list_repository_documents,
)


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
