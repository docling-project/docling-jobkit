from dataclasses import dataclass
from typing import Any
from unittest.mock import Mock

import pytest

from docling_jobkit.connectors.s3 import helper
from docling_jobkit.connectors.s3.helper import get_s3_connection, strip_prefix_postfix


@dataclass
class _S3CoordinatesDouble:
    endpoint: str = "s3.example.com"
    verify_ssl: bool = True
    access_key: str | None = None
    secret_key: str | None = None


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
