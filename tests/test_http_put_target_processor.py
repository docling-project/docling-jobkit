import httpx
import pytest

from docling.datamodel.service.responses import FailureCategory, FailurePhase
from docling.datamodel.service.targets import PutTarget

from docling_jobkit.connectors.http.target_processor import HttpPutTargetProcessor
from docling_jobkit.public_errors import (
    TargetWriteError,
    classify_public_task_failure,
)


def test_http_put_failure_is_reported_as_target_unavailable(monkeypatch, tmp_path):
    archive = tmp_path / "result.zip"
    archive.write_bytes(b"result")
    request = httpx.Request("PUT", "https://example.com/result")

    def fail_put(*args, **kwargs):
        del args, kwargs
        raise httpx.ConnectError("connection failed", request=request)

    monkeypatch.setattr(httpx, "put", fail_put)
    processor = HttpPutTargetProcessor(PutTarget(url=str(request.url)), max_retries=1)

    with pytest.raises(TargetWriteError) as exc_info:
        processor.upload_file(archive, archive.name, "application/zip")

    failure = classify_public_task_failure(exc_info.value, task_id="task-1")
    assert failure.category == FailureCategory.TARGET_UNAVAILABLE
    assert failure.phase == FailurePhase.ORCHESTRATION
