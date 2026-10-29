"""Tests for the optional conversion-completion hook in the RQ task runner."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from docling.datamodel.service.options import ConvertDocumentsOptions
from docling.datamodel.service.targets import InBodyTarget

from docling_jobkit.datamodel.task import Task
from docling_jobkit.orchestrators.rq.orchestrator import RQOrchestratorConfig
from docling_jobkit.orchestrators.rq.worker import _run_docling_task


class _Connection:
    def __init__(self) -> None:
        self.published: list[str] = []

    def publish(self, _channel: str, payload: str) -> None:
        self.published.append(payload)


class _Job:
    def __init__(self) -> None:
        self.connection = _Connection()


class _FakeConversionManager:
    config = SimpleNamespace(max_file_size=None)

    def __init__(self, conv_results=None, error: Exception | None = None) -> None:
        self._conv_results = conv_results or []
        self._error = error

    def convert_documents(self, **_kwargs):
        if self._error is not None:
            raise self._error
        return self._conv_results

    def parse_chunking_options(self, _options):
        return None


def _conv_result(page_count: int | None) -> SimpleNamespace:
    document = (
        None
        if page_count is None
        else SimpleNamespace(
            pages={page: object() for page in range(1, page_count + 1)}
        )
    )
    return SimpleNamespace(document=document)


def _run_with_hook(tmp_path, conversion_manager, on_conversion_complete):
    task = Task(
        task_id="task-1",
        sources=[],
        target=InBodyTarget(),
        convert_options=ConvertDocumentsOptions(),
    )
    config = RQOrchestratorConfig(scratch_dir=tmp_path)
    with (
        patch(
            "docling_jobkit.orchestrators.rq.worker.get_current_job",
            return_value=_Job(),
        ),
        patch(
            "docling_jobkit.orchestrators.rq.worker.process_exportable_results",
            side_effect=RuntimeError("stop after conversion"),
        ),
        # The conversion-failure case re-raises its own error instead.
        pytest.raises((RuntimeError, ValueError)),
    ):
        _run_docling_task(
            task=task,
            conversion_manager=conversion_manager,
            orchestrator_config=config,
            scratch_dir=tmp_path,
            on_conversion_complete=on_conversion_complete,
        )


def test_conversion_complete_hook_receives_total_page_count(tmp_path):
    captured: list[int] = []
    manager = _FakeConversionManager(
        conv_results=[_conv_result(2), _conv_result(3), _conv_result(0)]
    )

    _run_with_hook(tmp_path, manager, captured.append)

    assert captured == [5]


def test_conversion_complete_hook_skips_missing_documents(tmp_path):
    captured: list[int] = []
    manager = _FakeConversionManager(conv_results=[_conv_result(4), _conv_result(None)])

    _run_with_hook(tmp_path, manager, captured.append)

    assert captured == [4]


def test_conversion_complete_hook_not_called_when_conversion_fails(tmp_path):
    captured: list[int] = []
    manager = _FakeConversionManager(error=ValueError("conversion failed"))

    _run_with_hook(tmp_path, manager, captured.append)

    assert captured == []
