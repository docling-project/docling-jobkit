# `TASK_COMPLETED` callback behavior

This compares Ray HTTP callback behavior on `main` before PR #221 with the
behavior introduced by the PR. Redis task-status updates are not HTTP callbacks
and are therefore not listed as callbacks below.

| Scenario | Before PR #221 | After PR #221 |
| --- | --- | --- |
| Complete task; every document succeeds | `SET_NUM_DOCS`, per-document `DOCUMENT_COMPLETED`, and final `UPDATE_PROCESSED`; no task-terminal callback. | The same document callbacks, followed by `TASK_COMPLETED` with `task_status="success"`. |
| Complete task; some or every known document fails | Document callbacks and `UPDATE_PROCESSED` report the failed documents; no task-terminal callback. | The same document callbacks, followed by task `success` because the result set is complete and authoritative. |
| File or HTTP source fails materialization or preflight | The known source is reported as a document failure through the normal document callbacks; no task-terminal callback. | The same document failure callbacks, followed by task `success`. |
| S3 child has a source-specific or otherwise isolated conversion failure | The child becomes a document failure, siblings continue, and `UPDATE_PROCESSED` summarizes every known outcome. | Unchanged document behavior, including `UPDATE_PROCESSED`, followed by task `success`. |
| S3 child cannot initialize a configured model because it is absent from `artifacts_path` | The exception is incorrectly converted into one document failure, siblings continue, and the task appears successful. | The request-wide exception aborts fan-out. `UPDATE_PROCESSED` reports only outcomes already completed, then the lifecycle owner emits task `failure`; no document failure is invented for the missing model. |
| Converter or coordinator aborts before producing a complete result set | Any already-emitted document callbacks remain, but no HTTP callback reports the task failure. | Already-emitted document callbacks remain, then `TASK_COMPLETED` reports task `failure` with `PublicFailureInfo`. |
| Dispatcher detects timeout, actor death, or deployment failure | Redis marks the task failed, but no HTTP task-terminal callback is sent. | The dispatcher emits task `failure` only if it wins durable terminalization. |
| Reconciliation detects a stale execution lease and the dispatch hash survives | Redis marks the task failed, but callback destinations were not retained. | Callback specs are recovered from the TTL-bounded dispatch hash and reconciliation emits task `failure`. |
| Reconciliation has no usable dispatch hash or its callback data is legacy or corrupt | No HTTP task-terminal callback. | Still no HTTP callback because no trustworthy destination survives; durable failure and Redis status publication continue. |
| A competing lifecycle owner already terminalized the task | No task-terminal callback exists. | No duplicate terminal callback is scheduled because only the owner that changes durable status emits it. |
| Task has no callback specifications | No HTTP callbacks. | No HTTP callbacks. |
| Local or RQ orchestration | Existing callback behavior. | Unchanged; this PR adds terminal emission only to Ray. |

`TASK_COMPLETED` uses the existing `CallbackInvoker` transport. This PR does not
change ordering, retries, durability, or HTTP delivery guarantees.
