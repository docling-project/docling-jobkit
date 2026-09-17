from unittest.mock import MagicMock, patch

from docling.datamodel.service.sources import S3Coordinates

from docling_jobkit.connectors.s3.helper import get_s3_connection, strip_prefix_postfix


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
