from typing import Any, Callable
from unittest.mock import MagicMock, call, patch

import httpx
import pytest
from astrapy.exceptions import (
    CollectionInsertManyException,
    DataAPIHttpException,
    DataAPITimeoutException,
)

from docling_jobkit.connectors.astradb_helper import (
    _with_exponential_retry,
    build_records_from_chunks,
    get_collection,
    insert_records,
)
from docling_jobkit.convert.embedding import EmbeddingError
from docling_jobkit.datamodel.result import ChunkedDocumentResultItem

# build_records_from_chunks test stuff


@pytest.fixture
def mock_chunks() -> list[ChunkedDocumentResultItem]:
    """mocks the chunks from docling"""
    return [
        ChunkedDocumentResultItem(
            filename="doc.pdf", chunk_index=0, text="hello world", doc_items=[]
        )
    ]


def test_build_records_include_vector(
    mock_chunks: list[ChunkedDocumentResultItem],
) -> None:
    """The final record constructed shoulkd include a vector field"""
    mock_emb_model = MagicMock()
    mock_emb_model.encode.return_value.tolist.return_value = [[0.1, 0.2, 0.3]]

    records = build_records_from_chunks(
        mock_chunks, "doc1", "source.pdf", mock_emb_model
    )

    assert "$vector" in records[0]


def test_build_records_propagates_embedding_error(
    mock_chunks: list[ChunkedDocumentResultItem],
) -> None:
    """embedding failure should raise EmbeddingError"""
    with patch(
        "docling_jobkit.connectors.astradb_helper.generate_text_embedding",
        side_effect=EmbeddingError("fail"),
    ):
        with pytest.raises(EmbeddingError):
            build_records_from_chunks(mock_chunks, "doc1", "source.pdf", MagicMock())


# exponential backoff test stuff


def _make_http_exc(status_code: int) -> DataAPIHttpException:
    """helper to create the http exception with the specified status code"""
    httpx_error = MagicMock()
    httpx_error.response.status_code = status_code

    return DataAPIHttpException(
        text="error", httpx_error=httpx_error, error_descriptors=[]
    )


def _make_timeout_exc() -> DataAPITimeoutException:
    return DataAPITimeoutException(
        text="timeout", timeout_type="connect", endpoint=None, raw_payload=None
    )


@pytest.mark.parametrize(
    "transient_exc",
    [
        _make_timeout_exc(),
        _make_http_exc(503),  # 5xx errors are transient
        _make_http_exc(429),  # 429 is transient: rate-limit
        _make_http_exc(408),
        httpx.ConnectError("Connection refused"),
    ],
)
def test_exp_backoff_retries_on_transient_error_then_succeeds(
    transient_exc: Callable[[], Any],
) -> None:
    """It should not fail immediately and retry if transient"""
    fn = MagicMock(side_effect=[transient_exc, "ok"])
    with patch("docling_jobkit.connectors.astradb_helper.time.sleep"):
        assert _with_exponential_retry(fn, "op") == "ok"

    assert fn.call_count == 2


def test_exp_backoff_raises_immediately_on_4xx() -> None:
    """Any 4xx error is not considered a transient error so it should raise immediately"""
    fn = MagicMock(side_effect=_make_http_exc(403))
    with patch("docling_jobkit.connectors.astradb_helper.time.sleep"):
        with pytest.raises(DataAPIHttpException):
            _with_exponential_retry(fn, "op")

    fn.assert_called_once()


def test_exp_backoff_sequence() -> None:
    """It should do proper exp backoff wait properly (increases each time)"""
    fn = MagicMock(side_effect=_make_timeout_exc())
    with patch("docling_jobkit.connectors.astradb_helper.time.sleep") as mock_sleep:
        with pytest.raises(DataAPITimeoutException):
            _with_exponential_retry(fn, "op")

    assert mock_sleep.call_args_list == [call(1.0), call(2.0), call(4.0)]


# insert_records


def test_insert_records_empty_list_is_noop() -> None:
    """Empty record list should return without touching the collection."""
    collection = MagicMock()
    insert_records(collection, [], source_name="doc.pdf")
    collection.insert_many.assert_not_called()
    collection.update_one.assert_not_called()


def test_insert_records_new_doc_calls_insert_many() -> None:
    """All-new records: insert_many is called once per batch, update_one never."""
    collection = MagicMock()
    records = [{"_id": f"doc:chunk:{i}", "text": f"chunk {i}"} for i in range(3)]
    insert_records(collection, records, source_name="doc.pdf")
    collection.insert_many.assert_called_once_with(records, ordered=False)
    collection.update_one.assert_not_called()


def test_insert_records_conflict_falls_back_to_update_one() -> None:
    """On a duplicate-id conflict, update_one is called only for the failing record."""
    collection = MagicMock()
    records = [
        {"_id": "doc:chunk:0", "text": "a"},
        {"_id": "doc:chunk:1", "text": "b"},
    ]
    # Simulate doc:chunk:0 inserted successfully; doc:chunk:1 conflicted
    exc = CollectionInsertManyException(
        inserted_ids=["doc:chunk:0"], exceptions=[MagicMock()]
    )
    collection.insert_many.side_effect = exc

    insert_records(collection, records, source_name="doc.pdf")

    collection.update_one.assert_called_once_with(
        {"_id": "doc:chunk:1"},
        {"$set": {"text": "b"}},
        upsert=True,
    )


# get_collection


def test_get_collection_uses_token_and_returns_collection() -> None:
    """get_collection should authenticate with the token and return the collection."""
    from docling_jobkit.datamodel.astradb_coords import AstraDBCoordinates

    coords = AstraDBCoordinates.model_validate(
        {
            "api_endpoint": "https://abc123.apps.astra.datastax.com",
            "token": "AstraCS:test_token",
            "keyspace": "test_keyspace",
            "collection_name": "test_collection",
        }
    )

    mock_collection = MagicMock()
    mock_db = MagicMock()
    mock_db.create_collection.return_value = mock_collection
    mock_client = MagicMock()
    mock_client.get_database.return_value = mock_db

    with patch(
        "astrapy.DataAPIClient",
        return_value=mock_client,
    ):
        result = get_collection(coords, emb_dim=768)

    mock_client.get_database.assert_called_once_with(
        str(coords.api_endpoint), keyspace="test_keyspace"
    )
    mock_db.create_collection.assert_called_once()
    assert result is mock_collection
