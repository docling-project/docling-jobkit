import pytest

from docling_jobkit.datamodel.result import DoclingTaskResult, RemoteTargetResult
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.result_status import task_status_from_result


@pytest.mark.parametrize(
    "converted,succeeded,partial,expected",
    [
        (0, 0, 0, TaskStatus.FAILURE),
        (0, 1, 0, TaskStatus.FAILURE),
        (0, 0, 1, TaskStatus.FAILURE),
        (0, 1, 1, TaskStatus.FAILURE),
        (3, 0, 0, TaskStatus.FAILURE),
        (3, 1, 0, TaskStatus.SUCCESS),
        (3, 0, 1, TaskStatus.SUCCESS),
        (3, 1, 1, TaskStatus.SUCCESS),
        (3, 3, 0, TaskStatus.SUCCESS),
    ],
)
def test_task_status_from_result(converted, succeeded, partial, expected):
    result = DoclingTaskResult(
        result=RemoteTargetResult(),
        processing_time=0.1,
        num_converted=converted,
        num_succeeded=succeeded,
        num_partially_succeeded=partial,
        num_failed=max(0, converted - succeeded - partial),
    )
    assert task_status_from_result(result) == expected
