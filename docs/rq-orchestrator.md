# RQ orchestrator

The RQ orchestrator runs document conversions on a Redis queue. This page
describes how long the orchestrator keeps task metadata in Redis.

## Task metadata lifecycle

Every task has one metadata record under `docling:tasks:<task_id>:metadata`. The
record holds the serialized task, including its status.

| Task status | Metadata retention |
| --- | --- |
| `pending`, `started` | No expiry. The record is kept until the task reaches a terminal status. |
| `success`, `failure` | Expiry after `results_ttl`, 4 hours by default. A terminal write restarts that window. |

Active records are written without an expiry, so a task stays visible while it
waits in the queue or runs, no matter how long that takes. Rewriting an active
record also removes an expiry that was set earlier.

A terminal record is never replaced by an active one. A late update from
another replica cannot bring a finished task back to `pending` and cannot
extend the retention of its terminal record.

Reads do not extend the retention window. Metadata that already expired is not
restored, and metadata written by an earlier version keeps its previous expiry
until the task is written again.

## Memory cost

The metadata record holds the full serialized task: the sources, the conversion
options, the targets, the callbacks and the metadata. A source can carry the
file content itself, for example the base64 content of an uploaded document. An
active task therefore keeps its complete payload in Redis for as long as the
task is active.

The RQ job keeps a second copy of the task data. That copy has its own
retention and is not described here.

## Tasks that never finish

A task that never reaches a terminal status, for example because its queue
entry was lost, keeps its metadata record in Redis without a time limit. There
is no automatic cleanup for such records.

Operators who observe stuck active tasks have to reconcile or delete those
records themselves. Deleting a metadata record does not stop the RQ job, it
only removes the status information of the task.
