"""Terminal task callbacks shared by Ray lifecycle owners."""

import logging
from typing import Literal

from docling.datamodel.service.callbacks import ProgressTaskCompleted
from docling.datamodel.service.responses import PublicFailureInfo

from docling_jobkit.datamodel.task import Task
from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker

_log = logging.getLogger(__name__)


def emit_task_completed_callback(
    task: Task,
    task_status: Literal["success", "failure"],
    failure: PublicFailureInfo | None = None,
) -> None:
    """Schedule the terminal task event after durable terminalization."""
    if not task.callbacks:
        return

    try:
        CallbackInvoker().invoke_callbacks_async(
            callbacks=task.callbacks,
            task_id=task.task_id,
            progress=ProgressTaskCompleted(
                task_status=task_status,
                failure=failure,
            ),
        )
    except Exception as exc:  # pragma: no cover - observability only
        _log.warning(
            "Failed to emit task completion callback for %s: %s",
            task.task_id,
            exc,
        )
