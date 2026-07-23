from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from docling.datamodel.base_models import (
    ConversionStatus,
    DoclingComponentType,
    ErrorItem,
)
from docling.datamodel.service.callbacks import CallbackSpec
from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.responses import FailureCategory, FailurePhase
from docling.datamodel.service.targets import InBodyTarget

from docling_jobkit.connectors.errors import (
    ConnectorAuthenticationError,
    SourceConnectorAuthenticationError,
)
from docling_jobkit.connectors.filenet.errors import FileNetGraphQLError
from docling_jobkit.convert.materialization import SourceLimitExceededError
from docling_jobkit.convert.results import process_exportable_results
from docling_jobkit.datamodel.exportable_document import ExportableDocument
from docling_jobkit.datamodel.task import Task
from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestratorConfig,
    _TaskUpdate,
)
from docling_jobkit.orchestrators.rq.worker import _run_docling_task
from docling_jobkit.public_errors import (
    INTERNAL_TASK_ERROR_MESSAGE,
    TargetWriteError,
    build_public_error_item,
    build_public_task_error,
    classify_public_task_failure,
    render_public_error_list,
)


def test_build_public_error_item_surfaces_exception_detail():
    error = build_public_error_item(RuntimeError("ray actor died"))

    assert error.component_type == DoclingComponentType.PIPELINE
    assert error.module_name == "RuntimeError"
    assert error.error_message == "ray actor died"


def test_build_public_task_error_sanitizes_by_default():
    assert (
        build_public_task_error(RuntimeError("actor oom"))
        == INTERNAL_TASK_ERROR_MESSAGE
    )


def test_build_public_task_error_preserves_source_limit_failure_message():
    exc = SourceLimitExceededError(
        "Source 'incoming/doc.pdf' exceeds max_file_size=8 bytes"
    )

    assert build_public_task_error(exc) == str(exc)


def test_connector_authentication_failure_is_client_actionable():
    try:
        raise ValueError("invalid_grant")
    except ValueError as cause:
        exc = SourceConnectorAuthenticationError(
            "Google Drive authentication failed; re-authorize and supply valid "
            "credentials."
        )
        exc.__cause__ = cause

    failure = classify_public_task_failure(
        exc,
        task_id="task-1",
        phase=FailurePhase.EXECUTION,
    )

    assert failure.category == FailureCategory.POLICY
    assert failure.phase == FailurePhase.SOURCE_ENUMERATION
    assert failure.retryable is False
    assert failure.message == str(exc)
    assert build_public_task_error(exc) == str(exc)


def test_target_connector_authentication_failure_preserves_phase():
    exc = ConnectorAuthenticationError("Google Drive authentication failed.")

    failure = classify_public_task_failure(
        exc,
        task_id="task-1",
        phase=FailurePhase.EXECUTION,
    )

    assert failure.category == FailureCategory.POLICY
    assert failure.phase == FailurePhase.EXECUTION


def test_classify_public_task_failure_sanitizes_in_production():
    failure = classify_public_task_failure(
        RuntimeError("actor oom"),
        task_id="task-1",
    )

    assert failure.category == FailureCategory.CAPACITY
    assert failure.phase == FailurePhase.ORCHESTRATION
    assert (
        failure.message == "Service capacity was exhausted while processing the task."
    )


def test_classify_public_task_failure_preserves_http_details():
    request = httpx.Request("GET", "https://example.com/missing.pdf")
    response = httpx.Response(404, request=request)
    exc = httpx.HTTPStatusError("404 not found", request=request, response=response)

    failure = classify_public_task_failure(exc, task_id="task-1")

    assert failure.category == FailureCategory.POLICY
    assert failure.phase == FailurePhase.SOURCE_ENUMERATION
    assert failure.details["source_kind"] == "http"


def test_classify_http_status_error_preserves_upstream_message():
    request = httpx.Request("GET", "https://example.com/missing.pdf")
    response = httpx.Response(404, request=request)
    exc = httpx.HTTPStatusError("404 not found", request=request, response=response)

    failure = classify_public_task_failure(exc, task_id="task-1")

    assert failure.category == FailureCategory.POLICY
    assert failure.phase == FailurePhase.SOURCE_ENUMERATION
    assert failure.message == "404 not found"
    assert failure.details == {"source_kind": "http"}


def test_classify_internal_category_sanitizes_message():
    failure = classify_public_task_failure(
        RuntimeError("redis connection refused: redis://internal-host:6379"),
        task_id="task-1",
    )

    assert failure.category == FailureCategory.INTERNAL
    assert failure.message == INTERNAL_TASK_ERROR_MESSAGE


def test_classify_http_transport_error_uses_curated_message():
    request = httpx.Request("GET", "https://example.com/missing.pdf")
    exc = httpx.ConnectError(
        "connection refused for https://example.com/missing.pdf", request=request
    )

    failure = classify_public_task_failure(exc, task_id="task-1")

    assert failure.category == FailureCategory.SOURCE_UNAVAILABLE
    assert failure.phase == FailurePhase.SOURCE_ENUMERATION
    assert failure.message == "Source document could not be reached."
    assert failure.details == {}


def test_classify_source_limit_failure_as_policy():
    exc = SourceLimitExceededError(
        "Source 'incoming/doc.pdf' exceeds max_file_size=8 bytes"
    )

    failure = classify_public_task_failure(exc, task_id="task-1")

    assert failure.category == FailureCategory.POLICY
    assert failure.phase == FailurePhase.SOURCE_ENUMERATION
    assert failure.retryable is False
    assert failure.message == str(exc)


def test_classify_filenet_graphql_error_as_policy():
    exc = FileNetGraphQLError(
        'GraphQL query failed: [{"message": "Invalid identifier"}]'
    )

    failure = classify_public_task_failure(exc, task_id="task-1")

    assert failure.category == FailureCategory.POLICY
    assert failure.phase == FailurePhase.SOURCE_ENUMERATION
    assert failure.retryable is False
    assert failure.message == str(exc)
    assert build_public_task_error(exc) == str(exc)


def test_classify_ray_failure_preserves_connector_authentication_error():
    ray = pytest.importorskip("ray")
    from docling_jobkit.orchestrators.ray.failure_classification import (
        classify_ray_public_task_failure,
    )

    auth_error = SourceConnectorAuthenticationError(
        "Google Drive authentication failed; re-authorize and supply valid credentials."
    )
    auth_error.__cause__ = ValueError("invalid_grant")
    exc = ray.exceptions.RayTaskError(
        "converter.handle_request",
        "traceback",
        auth_error,
    )

    failure = classify_ray_public_task_failure(
        exc,
        task_id="task-1",
        phase=FailurePhase.EXECUTION,
    )

    assert failure.category == FailureCategory.POLICY
    assert failure.phase == FailurePhase.SOURCE_ENUMERATION
    assert failure.message == str(auth_error)


def test_render_public_error_list_avoids_python_repr():
    errors = [
        ErrorItem(
            component_type=DoclingComponentType.PIPELINE,
            module_name="RuntimeError",
            error_message="alpha",
        ),
        ErrorItem(
            component_type=DoclingComponentType.PIPELINE,
            module_name="ValueError",
            error_message="beta",
        ),
    ]

    assert render_public_error_list(errors, debug_enabled=False) == "alpha; beta"
    assert (
        render_public_error_list(errors, debug_enabled=True)
        == "RuntimeError: alpha; ValueError: beta"
    )


def test_build_failed_slice_result_surfaces_exception_detail():
    pytest.importorskip("ray")
    from docling_jobkit.orchestrators.ray.serve_deployment import (
        _build_failed_slice_result,
    )

    result = _build_failed_slice_result(
        filename="slice.pdf",
        page_range=(1, 2),
        slice_index=0,
        exc=RuntimeError("ray actor died"),
        debug_error_details=False,
    )

    assert result.errors[0].module_name == "RuntimeError"
    assert result.errors[0].error_message == "ray actor died"


def test_process_exportable_results_renders_callback_errors_without_repr(
    tmp_path: Path,
):
    callback_invoker = MagicMock()
    exportable_document = ExportableDocument(
        file=Path("failed.pdf"),
        status=ConversionStatus.FAILURE,
        errors=[
            ErrorItem(
                component_type=DoclingComponentType.PIPELINE,
                module_name="RuntimeError",
                error_message="alpha",
            ),
            ErrorItem(
                component_type=DoclingComponentType.PIPELINE,
                module_name="ValueError",
                error_message="beta",
            ),
        ],
    )
    task = Task(
        task_id="task-1",
        target=InBodyTarget(),
        convert_options=ConvertDocumentsOptions(),
        callbacks=[CallbackSpec(url="http://callback.example")],
    )

    process_exportable_results(
        task=task,
        exportable_documents=[exportable_document],
        work_dir=tmp_path,
        callback_invoker=callback_invoker,
        debug_error_details=False,
    )

    callback_errors = [
        call.kwargs["progress"].document.error
        for call in callback_invoker.invoke_callbacks_async.call_args_list
        if hasattr(call.kwargs["progress"], "document")
    ]
    assert callback_errors == ["alpha; beta"]


def test_rq_worker_publishes_sanitized_failure_message(tmp_path: Path):
    published_updates: list[str] = []

    class _Connection:
        def publish(self, _channel: str, payload: str) -> None:
            published_updates.append(payload)

    class _Job:
        connection = _Connection()

    task = Task(
        task_id="task-1",
        target=InBodyTarget(),
        convert_options=ConvertDocumentsOptions(),
    )
    config = RQOrchestratorConfig(
        scratch_dir=tmp_path,
        debug_error_details=False,
    )

    with patch(
        "docling_jobkit.orchestrators.rq.worker.get_current_job",
        return_value=_Job(),
    ):
        with pytest.raises(RuntimeError, match="No converter"):
            _run_docling_task(
                task=task,
                conversion_manager=None,
                orchestrator_config=config,
                scratch_dir=tmp_path,
            )

    failure_update = next(
        _TaskUpdate.model_validate_json(update)
        for update in published_updates
        if '"task_status":"failure"' in update
    )
    assert failure_update.error_message == INTERNAL_TASK_ERROR_MESSAGE


def test_rq_worker_keeps_internal_failure_sanitized_even_in_debug_mode(tmp_path: Path):
    published_updates: list[str] = []

    class _Connection:
        def publish(self, _channel: str, payload: str) -> None:
            published_updates.append(payload)

    class _Job:
        connection = _Connection()

    task = Task(
        task_id="task-1",
        target=InBodyTarget(),
        convert_options=ConvertDocumentsOptions(),
    )
    config = RQOrchestratorConfig(
        scratch_dir=tmp_path,
        debug_error_details=True,
    )

    with patch(
        "docling_jobkit.orchestrators.rq.worker.get_current_job",
        return_value=_Job(),
    ):
        with pytest.raises(RuntimeError, match="No converter"):
            _run_docling_task(
                task=task,
                conversion_manager=None,
                orchestrator_config=config,
                scratch_dir=tmp_path,
            )

    failure_update = next(
        _TaskUpdate.model_validate_json(update)
        for update in published_updates
        if '"task_status":"failure"' in update
    )
    assert failure_update.error_message == INTERNAL_TASK_ERROR_MESSAGE


def test_classify_target_write_error():
    exc = TargetWriteError("Failed to upload to target URL after 3 attempts.")

    failure = classify_public_task_failure(
        exc,
        task_id="task-1",
        phase=FailurePhase.EXECUTION,
    )

    assert failure.category == FailureCategory.TARGET_UNAVAILABLE
    assert failure.phase == FailurePhase.EXECUTION
    assert failure.retryable is False
    assert failure.message == "Result could not be written to the requested target."


def test_classify_target_write_error_preserves_phase():
    exc = TargetWriteError("upload failed")

    failure = classify_public_task_failure(
        exc,
        task_id="task-2",
        phase=FailurePhase.ORCHESTRATION,
    )

    assert failure.category == FailureCategory.TARGET_UNAVAILABLE
    assert failure.phase == FailurePhase.ORCHESTRATION
    assert failure.message == "Result could not be written to the requested target."
