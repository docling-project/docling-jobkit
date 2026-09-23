from docling_jobkit.datamodel.result import DoclingTaskResult
from docling_jobkit.datamodel.task_meta import TaskStatus


def task_status_from_result(result: DoclingTaskResult) -> TaskStatus:
    if result.num_converted == 0 or (
        result.num_succeeded == 0 and result.num_partially_succeeded == 0
    ):
        return TaskStatus.FAILURE
    return TaskStatus.SUCCESS
