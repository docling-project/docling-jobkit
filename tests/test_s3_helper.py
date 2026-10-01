from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest

from docling.datamodel.service.sources import S3Coordinates

from docling_jobkit.connectors.s3 import helper
from docling_jobkit.connectors.s3.helper import get_s3_connection, strip_prefix_postfix


@dataclass
class _S3CoordinatesDouble:
    endpoint: str = "s3.example.com"
    verify_ssl: bool = True
    access_key: str | None = None
    secret_key: str | None = None
    region: str | None = None


def _recording_session(monkeypatch: pytest.MonkeyPatch) -> Mock:
    session = Mock()
    monkeypatch.setattr(helper, "Session", lambda: session)
    return session


def _connection_kwargs(call: Any) -> dict[str, Any]:
    return call.kwargs


def test_get_s3_connection_omits_credentials_for_default_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _recording_session(monkeypatch)

    get_s3_connection(_S3CoordinatesDouble())  # type: ignore[arg-type]

    for call in (session.client.call_args, session.resource.call_args):
        kwargs = _connection_kwargs(call)
        assert "aws_access_key_id" not in kwargs
        assert "aws_secret_access_key" not in kwargs


def test_get_s3_connection_passes_complete_explicit_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _recording_session(monkeypatch)
    coords = _S3CoordinatesDouble(
        access_key="explicit-access-key", secret_key="explicit-secret-key"
    )

    get_s3_connection(coords)  # type: ignore[arg-type]

    for call in (session.client.call_args, session.resource.call_args):
        kwargs = _connection_kwargs(call)
        assert kwargs["aws_access_key_id"] == "explicit-access-key"
        assert kwargs["aws_secret_access_key"] == "explicit-secret-key"


@pytest.mark.parametrize(
    ("access_key", "secret_key"),
    [("access-key", None), (None, "secret-key")],
)
def test_get_s3_connection_rejects_partial_credentials(
    monkeypatch: pytest.MonkeyPatch,
    access_key: str | None,
    secret_key: str | None,
) -> None:
    session = _recording_session(monkeypatch)
    coords = _S3CoordinatesDouble(
        access_key=access_key,
        secret_key=secret_key,
    )

    with pytest.raises(ValueError, match="access_key and secret_key"):
        get_s3_connection(coords)  # type: ignore[arg-type]

    session.client.assert_not_called()
    session.resource.assert_not_called()


def test_strip_prefix_postfix():
    in_set = {"mypath/json/file_1.json", "mypath/json/file_2.json"}
    out_set = strip_prefix_postfix(in_set, prefix="mypath/json/", extension=".json")

    assert len(in_set) == len(out_set)
    assert out_set == {"file_1", "file_2"}


def test_get_s3_connection_forwards_region_name():
    coords = S3Coordinates(
        endpoint="s3.us-east-2.amazonaws.com",
        region="us-east-2",
        verify_ssl=True,
        access_key="key",
        secret_key="secret",
        bucket="bucket-a",
        key_prefix="",
    )

    with patch("docling_jobkit.connectors.s3.helper.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        get_s3_connection(coords)

        assert mock_session.client.call_args.kwargs["region_name"] == "us-east-2"
        assert mock_session.resource.call_args.kwargs["region_name"] == "us-east-2"


def test_get_s3_connection_region_defaults_to_none():
    coords = S3Coordinates(
        endpoint="127.0.0.1:9000",
        verify_ssl=False,
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="test",
        key_prefix="",
    )

    with patch("docling_jobkit.connectors.s3.helper.Session") as mock_session_cls:
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        get_s3_connection(coords)

        assert mock_session.client.call_args.kwargs["region_name"] is None
        assert mock_session.resource.call_args.kwargs["region_name"] is None
