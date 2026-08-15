"""Terminal-failure progress callbacks shared by the orchestrators."""

import logging
from typing import Optional

from docling.datamodel.base_models import ConversionStatus
from docling.datamodel.service.callbacks import (
    ProcessedDocsItem,
    ProgressUpdateProcessed,
)
from docling.datamodel.service.responses import PublicFailureInfo

from docling_jobkit.datamodel.exportable_document import source_to_public_uri
from docling_jobkit.datamodel.task import Task
from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker

_log = logging.getLogger(__name__)


def emit_task_failure_callbacks(
    task: Task,
    failure: PublicFailureInfo,
    *,
    task_size: Optional[int] = None,
) -> None:
    """Notify callbacks that a task ended in a terminal, task-level FAILURE.

    A task that dies before or inside conversion never reaches the result
    processing path, so it emits no progress callbacks at all and a client that
    only listens on its callback URL never learns the task ended. This emits the
    terminal UPDATE_PROCESSED that the success and per-document failure paths
    would otherwise have emitted.

    Counts mirror the durable failure accounting (``delta_failed_documents=
    task_size``) so metering stays consistent even when a fan-out task had
    already reported partial progress. SET_NUM_DOCS and DOCUMENT_COMPLETED are
    deliberately not re-emitted here: a task can fail after partial progress, and
    a second SET_NUM_DOCS would confuse a receiver that already sized the task.

    Callers MUST gate this on the atomic terminalization result so that only the
    caller that actually flipped the durable status emits.
    """
    if not task.callbacks:
        return

    docs = [
        ProcessedDocsItem(
            source=source_to_public_uri(source) or "unknown",
            status=ConversionStatus.FAILURE,
            error=failure.message,
        )
        for source in task.sources
    ]
    if not docs:
        docs = [
            ProcessedDocsItem(
                source="unknown",
                status=ConversionStatus.FAILURE,
                error=failure.message,
            )
        ]

    num_docs = max(task_size if task_size is not None else len(task.sources), 1)

    try:
        CallbackInvoker().invoke_callbacks_async(
            callbacks=task.callbacks,
            task_id=task.task_id,
            progress=ProgressUpdateProcessed(
                num_processed=num_docs,
                num_succeeded=0,
                num_partially_succeeded=0,
                num_failed=num_docs,
                docs=docs,
            ),
        )
    except Exception as exc:  # pragma: no cover - observability only
        _log.warning(
            "Failed to emit terminal failure callbacks for %s: %s",
            task.task_id,
            exc,
        )
