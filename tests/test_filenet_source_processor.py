import logging
from unittest.mock import MagicMock, patch

import pytest
import requests

from docling_jobkit.connectors.filenet_source_processor import FileNetSourceProcessor


def test_initialize_does_not_log_connected_on_probe_failure(caplog) -> None:
    processor = FileNetSourceProcessor(MagicMock())
    with patch("docling_jobkit.connectors.filenet_helper.time.sleep"):
        with patch(
            "requests.post", side_effect=requests.ConnectionError("DNS failure")
        ):
            with pytest.raises(requests.ConnectionError):
                processor._initialize()

    assert "Connected to FileNet" not in caplog.text


def test_initialize_logs_connected_after_successful_probe(caplog) -> None:
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "data": {"documents": {"pageInfo": {"totalCount": 0}}}
    }
    processor = FileNetSourceProcessor(MagicMock())

    with caplog.at_level(logging.INFO):
        with patch("requests.post", return_value=mock_response):
            processor._initialize()

    assert "Connected to FileNet" in caplog.text
