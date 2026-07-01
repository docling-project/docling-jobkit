from typing import Any, Callable
from unittest.mock import MagicMock, call, patch

import httpx
import pytest
from astrapy.exceptions import DataAPIHttpException, DataAPITimeoutException

from docling_jobkit.connectors.astradb_helper import (
    _with_exponential_retry,
    build_records_from_chunks,
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
