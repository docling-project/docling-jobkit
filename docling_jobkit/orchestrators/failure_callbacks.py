"""Terminal-failure progress callbacks shared by the orchestrators."""

import logging

from docling.datamodel.base_models import ConversionStatus, DocumentStream
from docling.datamodel.service.callbacks import (
    ProcessedDocsItem,
    ProgressUpdateProcessed,
)
from docling.datamodel.service.responses import PublicFailureInfo
from docling.datamodel.service.sources import FileSource, HttpSource

from docling_jobkit.datamodel.exportable_document import source_to_public_uri
from docling_jobkit.datamodel.task import Task
from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker

_log = logging.getLogger(__name__)

# Source kinds that are exactly one document. Every other source kind is a
# container coordinate (bucket, prefix, drive folder) that expands to an unknown
# number of documents, so a per-source entry there would not be a document
# result at all.
_SINGLE_DOCUMENT_SOURCES = (DocumentStream, FileSource, HttpSource)


def _failed_document_items(task: Task, error: str) -> list[ProcessedDocsItem]:
    """Per-document failure entries, or [] when sources are not 1:1 documents."""
    if not task.sources:
        return []
    if not all(isinstance(source, _SINGLE_DOCUMENT_SOURCES) for source in task.sources):
        return []
    return [
        ProcessedDocsItem(
            source=source_to_public_uri(source) or "unknown",
            status=ConversionStatus.FAILURE,
            error=error,
        )
        for source in task.sources
    ]


def emit_task_failure_callbacks(
    task: Task,
    failure: PublicFailureInfo,
) -> None:
    """Notify callbacks that a task ended in a terminal, task-level FAILURE.

    A task that dies before or inside conversion never reaches the result
    processing path, so it emits no progress callbacks at all and a client that
    only listens on its callback URL never learns the task ended. This emits a
    terminal UPDATE_PROCESSED in its place.

    The callback protocol has no task-level failure event, so UPDATE_PROCESSED is
    the only terminal shape available. To avoid reporting document outcomes that
    are not document outcomes, the payload only carries per-document entries when
    every source of the task is itself exactly one document; container sources
    (an S3 prefix, a Drive folder) expand to an unknown document set, so those
    tasks get a terminal event with an empty ``docs`` list rather than a
    fabricated tally. Counts always match ``docs``.

    This remains a task-level signal, not a document tally: a task can die after
    it already reported successful documents, and this event does not retract or
    restate them. Receivers must treat a terminal UPDATE_PROCESSED as "the task
    is over" and read the task status endpoint for the authoritative outcome.
    SET_NUM_DOCS and DOCUMENT_COMPLETED are deliberately not re-emitted: a second
    SET_NUM_DOCS would confuse a receiver that already sized the task.

    Delivery is at most once, not exactly once. Callers MUST gate this on the
    atomic terminalization result so that only the caller that actually flipped
    the durable status emits — but that same gate means a lost notification is
    never retried: the durable transition has already committed when this is
    called, and ``CallbackInvoker`` posts from a fire-and-forget daemon thread,
    so an actor that exits before the POST completes drops the notification and
    every other component will see an already-terminal task. Guaranteed delivery
    would need a durable outbox with its own delivery state.
    """
    if not task.callbacks:
        return

    docs = _failed_document_items(task, failure.message)

    try:
        CallbackInvoker().invoke_callbacks_async(
            callbacks=task.callbacks,
            task_id=task.task_id,
            progress=ProgressUpdateProcessed(
                num_processed=len(docs),
                num_succeeded=0,
                num_partially_succeeded=0,
                num_failed=len(docs),
                docs=docs,
            ),
        )
    except Exception as exc:  # pragma: no cover - observability only
        _log.warning(
            "Failed to emit terminal failure callbacks for %s: %s",
            task.task_id,
            exc,
        )
