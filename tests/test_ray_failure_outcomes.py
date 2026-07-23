import os

import msgpack
import pytest

# redis_helper (imported below) pulls in ray transitively; skip cleanly when ray
# is absent, and never run Ray tests in CI (CI does not provision Ray).
pytest.importorskip("ray")
if os.getenv("CI"):
    pytest.skip("Skipping Ray tests in CI", allow_module_level=True)

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.service.responses import (
    FailureCategory,
    FailurePhase,
    PublicFailureInfo,
)

from docling_jobkit.datamodel.result import (
    DoclingTaskResult,
    DocumentResultItem,
    ExportDocumentResponse,
)
from docling_jobkit.datamodel.stored_outcome import (
    StoredFailureOutcome,
    StoredSuccessOutcome,
)
from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager
from docling_jobkit.orchestrators.serialization import make_msgpack_safe


def _success_result() -> DoclingTaskResult:
    return DoclingTaskResult(
        result=DocumentResultItem(
            document=ExportDocumentResponse(filename="doc.md", md_content="hello"),
            status=ConversionStatus.SUCCESS,
        ),
        processing_time=0.1,
        num_converted=1,
        num_succeeded=1,
        num_partially_succeeded=0,
        num_failed=0,
    )


def test_decode_stored_outcome_accepts_success_envelope() -> None:
    payload = RedisStateManager._serialize_stored_outcome(  # type: ignore[attr-defined]
        StoredSuccessOutcome(result=_success_result())
    )

    decoded = RedisStateManager.decode_stored_outcome(payload)

    assert isinstance(decoded, StoredSuccessOutcome)
    assert decoded.result.result.kind == "ExportResult"


def test_decode_stored_outcome_accepts_failure_envelope() -> None:
    payload = RedisStateManager._serialize_stored_outcome(  # type: ignore[attr-defined]
        StoredFailureOutcome(
            failure=PublicFailureInfo(
                category=FailureCategory.INTERNAL,
                message="Internal processing error.",
                retryable=False,
                phase=FailurePhase.ORCHESTRATION,
            )
        )
    )

    decoded = RedisStateManager.decode_stored_outcome(payload)

    assert isinstance(decoded, StoredFailureOutcome)
    assert decoded.failure.category == FailureCategory.INTERNAL


def test_decode_stored_outcome_accepts_legacy_task_result_blob() -> None:
    legacy_blob = msgpack.packb(
        make_msgpack_safe(_success_result().model_dump()),
        use_bin_type=True,
    )

    decoded = RedisStateManager.decode_stored_outcome(legacy_blob)

    assert isinstance(decoded, DoclingTaskResult)
    assert decoded.result.kind == "ExportResult"
