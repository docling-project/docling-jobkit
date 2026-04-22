# Ray PDF Fan-Out / Collect With Shared Artifacts

## Context
The current Ray path is `API request -> one Redis task -> one dispatcher admission -> one Serve replica call -> one terminal result`. `page_range` already works end-to-end and the Docling PDF pipelines only instantiate/process pages inside the selected range, so the page-slicing primitive exists today. The missing pieces are orchestration and transport: one logical request needs to become many page-slice executions without re-downloading or re-serializing the same large PDF.

Page numbering is confirmed absolute: when `page_range=(5,10)` is requested, the output `DoclingDocument` contains pages numbered 5–10 (not 1–6). `DoclingDocument.concatenate` therefore works correctly across chunks with no renumbering needed.

The locked v1 scope and non-goals are summarized in the final section of this document to avoid repeating the same invariants throughout.

## Summary
Implement PDF page-chunk fan-out as a **logical parent task with ephemeral internal child executions**, not as Redis-visible child tasks.

Split the current monolithic `DocumentProcessorDeployment` into two cooperating Ray Serve deployments with different resource profiles:

- **`FanoutCoordinatorDeployment`** (new, cheap): owns the parent task lifecycle — execution lease heartbeat, Redis finalization, source materialization, split planning, result assembly. Runs with relaxed concurrency (`max_ongoing_requests > 1`) because its work is mostly I/O and waiting. Holds no model weights, needs no GPU.
- **`PageWorkerDeployment`** (renamed from current `DocumentProcessorDeployment`): owns warm `DoclingConverterManager` instances and executes conversion work. Keeps `max_ongoing_requests=1` to preserve thread-safety. Receives pre-materialized PDF bytes as a `ray.ObjectRef` plus a page range; returns a conversion result. Has no Redis lifecycle responsibility.

The coordinator receives every parent task from the dispatcher. For single-source PDF tasks above the chunk threshold, it fans out N child requests to the worker deployment, awaits them asynchronously, and assembles the result. For all other tasks, it sends a single request to the worker and passes the result through. The coordinator is never resource-idle on a heavy replica — the worst case is a cheap coordinator slot waiting, while GPU-capable worker slots remain available for real conversion work.

This avoids:
- N repeated HTTP downloads for one remote PDF.
- N base64 copies of one uploaded PDF in Redis.
- Reworking the Redis scheduler to understand durable parent/child task graphs in v1.
- Deadlocking the Serve replica pool: coordinator and worker are separate deployments; parents never compete with children for the same replica pool.
- Re-initializing `DoclingConverterManager` per child: worker replicas stay warm across all child requests, exactly as today.

## Implementation Changes

### 1. Two-deployment architecture

Replace the single `DocumentProcessorDeployment` with two cooperating deployments deployed together:

**`FanoutCoordinatorDeployment`**
- Resource budget: `num_cpus=0.5`, moderate memory (enough to hold a materialized PDF and an assembled DoclingDocument), `num_gpus=0`.
- `max_ongoing_requests`: relaxed (e.g. 4–8) — coordinator work is async waiting, not CPU-bound.
- Holds: `RedisStateManager`, execution lease heartbeat logic, finalization calls, handle to `PageWorkerDeployment`.
- Does not hold a `DoclingConverterManager`.

**`PageWorkerDeployment`** (current `DocumentProcessorDeployment` stripped of lifecycle ownership)
- Resource budget: unchanged (full GPU + memory for warm models).
- `max_ongoing_requests=1`: required by thread-unsafe `DocumentConverter` and preserved for every replica that holds a warmed `DoclingConverterManager`.
- Holds: persistent `DoclingConverterManager` (`self.cm`) exactly as today.
- Public Serve entry point: `process_worker_request(request: WorkerRequest)` where `WorkerRequest` is a discriminated union with variants for `chunk_convert`, `materialized_convert`, and `passthrough_task`.
- Internal helpers may still be named `process_chunk` / `_process_materialized_convert` / `_process_passthrough_task`, but they are worker-internal branches behind one Serve boundary.
- Does **not** call `finalize_task_*_atomic`, does **not** maintain an execution lease. It is a pure conversion service.

The dispatcher routes all tasks to the coordinator handle (one handle, same as today, just pointing to the new coordinator deployment). The dispatcher is otherwise unchanged.

### 2. Where orchestration lives
- Redis dispatcher: unchanged.
- `orchestrators/ray/orchestrator.py`: responsible only for enqueueing the parent, status/result lookup, and parent lifecycle — unchanged.
- `orchestrators/ray/serve_deployment.py`: split into coordinator class and worker class. Coordinator owns the task processing entry point and lifecycle; worker owns conversion only.
- No Redis-visible child `Task`s in v1.

### 3. Shared artifact: direct ObjectRef ownership, no registry actor

The coordinator materializes the source once, places it in the Ray plasma store with `ray.put()`, and holds the returned `ray.ObjectRef` as a local variable for the duration of the fan-out. The ObjectRef is passed directly to each worker child request. Ray's reference counting keeps the object alive as long as the coordinator holds the reference; when the coordinator's scope exits (success, failure, or cancellation), the reference is released and Ray GC reclaims the plasma store memory.

No `RayArtifactRegistry` actor is needed. The coordinator *is* the registry: it owns the reference and is the only entity that created it. A registry actor would add a new component, a new cleanup path, and a new failure mode without improving durability, because if the coordinator dies the task fails regardless.

Ray's plasma store lives in shared memory and automatically spills to disk under memory pressure (default enabled). The existing `max_file_size` admission gate remains the only size-based guard in v1.

### 4. Shared source materialization and preflight helper

Extract a module-level helper `materialize_and_preflight(source, limits) -> MaterializedSource` in `docling-jobkit` and make it the canonical admission/preflight path for single-source PDF inputs. The coordinator uses it for fan-out planning; the worker uses the same helper for its direct materialized path. This avoids two diverging implementations of the same logic without trying to "extract" behavior from `DoclingConverterManager.convert_documents()`, which is currently only a thin wrapper around the lower-level converter call.

`MaterializedSource` contains:
- `bytes`: the raw PDF bytes
- `page_count: int`: from `PyPdfiumDocumentBackend.page_count()` (i.e. `len(self._pdoc)`) — no full parse
- `filename`: original filename for export and hashing

The helper applies `max_file_size` and `max_num_pages` checks immediately after materializing, before any further work. If either limit is exceeded, it raises a structured exception that the coordinator catches and converts to a task failure.

At parent execution start, the coordinator calls this helper for single-source paginated tasks. It uses the result to decide fan-out vs. pass-through. The materialized bytes are passed to the worker as an ObjectRef regardless of which path is taken — avoiding re-download even for non-fan-out PDF tasks.

For coordinator-admitted fan-out children, the worker does **not** treat `max_num_pages` or `max_file_size` as a fresh per-child admission gate. Those full-document gates have already been applied once, on the original request document, by the shared preflight helper. Child execution only applies the child `page_range` to an already-admitted document.

For multi-source tasks, the coordinator passes sources through to the worker as-is (current behavior, no coordinator-level materialization).

Source materialization:
- `HttpSource`: download once using the existing request headers.
- `FileSource` / `DocumentStream`: decode once from the task payload.

Page counting uses `pypdfium2` via `PyPdfiumDocumentBackend` — already a transitive dependency via docling. No additional library needed.

`max_num_pages` is validated against the **total** PDF page count, not the per-chunk size. This is the correct gate: fan-out does not bypass the document size limit, it only provides a more efficient execution strategy for documents that are within limits but large.

### 5. Child execution contract

Children are Ray Serve requests to `PageWorkerDeployment`, not `ray.remote` functions. This preserves warm `DoclingConverterManager` reuse: worker replicas keep their model state between child requests exactly as they do between sequential task calls today. The warm converter is the reason the current Serve design is performant for OCR/VLM-heavy workloads; child requests must share it.

Child input to the worker's `chunk_convert` request variant:
- `artifact_ref: ray.ObjectRef` — reference to materialized PDF bytes in the plasma store
- `page_range: tuple[int, int]` — sub-slice of the effective page range (see below)
- `options: ConvertDocumentsOptions` — copied from parent, with `page_range` overridden to the child's sub-range
- `metadata`: original filename and source metadata needed for export and hashing

Caller-supplied `page_range` interaction: if the original request includes `page_range=[10, 50]`, child ranges are sub-slices of that range — `[10, 19]`, `[20, 29]`, …, `[50, 50]`. The child's page range always overrides the parent's convert_options.page_range, not replaces the field wholesale.

Child behavior in the worker's `chunk_convert` branch:
- Dereference `artifact_ref` via `ray.get()` to obtain PDF bytes (shared memory, zero-copy on the same node).
- Build a `DocumentStream` from the bytes.
- Call `self.cm.convert_documents(...)` with the child page range — exactly the existing warm converter path.
- Do not reinterpret `max_num_pages` as a per-child limit. The full-document admission gate already ran once during coordinator preflight.
- Return `ChunkResult(status, document, errors, timings, page_range)`.

Do not split into physical sub-PDFs in v1.

### 6. Child parallelism and coordinator occupancy

Parent tasks still consume tenant slot counters as one logical task and one logical document. Child requests to the worker deployment are internal and must not increment Redis tenant counters.

The coordinator awaits child futures asynchronously over Serve handle futures. The coordinator replica is not CPU-bound while waiting, and because coordinator replicas carry no GPU or heavy model resources, the idle cost during the wait is low — a cheap coordinator slot, not a GPU.

`max_page_chunk_parallelism` is optional. When set, it caps the number of in-flight child requests: submit up to `min(number_of_chunks, max_page_chunk_parallelism)` initially and refill as children complete, to prevent a single large PDF from saturating the worker pool. When unset, the coordinator may submit all planned child requests at once. Use a bounded work-queue pattern (`asyncio.wait`, `as_completed`, semaphore, or equivalent) only when the cap is enabled; otherwise a plain `gather()` across the full child set is acceptable.

### 7. Collect and final result assembly

Await all child futures. Collect all results regardless of individual child outcome.

Task-level status remains binary — **`SUCCESS` or `FAILURE`** — consistent with the existing task lifecycle stack (`TaskStatus` in `task.py`, Redis terminalization in `redis_helper.py`, reconciliation in `dispatcher.py`). Document-level partial semantics are already represented in `ConversionStatus.PARTIAL_SUCCESS` within the result payload and are already handled in `results_processor.py`. Introducing a third task-level terminal state would ripple across all of those layers for no additional expressiveness over what the result payload already provides.

Result assembly rules:
- At least one child succeeds: coordinator assembles a result document from the successful chunks. The assembled document carries `ConversionStatus.PARTIAL_SUCCESS` if any children failed, `ConversionStatus.SUCCESS` if all succeeded. The task is marked **`SUCCESS`** and the result is stored.
- All children fail: task is marked **`FAILURE`**, no result document is stored.
- Fan-out setup fails before any child launches: task is marked **`FAILURE`** immediately.

Concatenate successful child documents with `DoclingDocument.concatenate` in ascending page-range order. The assembled document is exported through the same export helper as the current single-document path; no new export code path is needed.

Public response contract is unchanged: sync endpoints wait on the parent; async polling and result retrieval expose the parent only; no public child task IDs.

### 8. Failure and cleanup behavior

- Fan-out setup fails before any child launches: coordinator catches exception, calls `finalize_task_failure_atomic`, releases ObjectRef.
- All children fail: coordinator calls `finalize_task_failure_atomic`, releases ObjectRef.
- Some children fail: coordinator assembles partial result, calls `finalize_task_success_atomic` with `ConversionStatus.PARTIAL_SUCCESS` document, releases ObjectRef.
- Coordinator replica dies mid-flight: execution lease (maintained by coordinator) goes stale. Reconciler marks the parent failed after `heartbeat_interval × 4`. Child requests to the worker are orphaned and resolved by Serve's own request timeout. ObjectRef is released when coordinator's actor scope exits.
- ObjectRef release is in a `finally` block: mandatory on all coordinator exit paths.

## Public / Internal Interfaces
No request API changes in v1.

New internal deployment:
- `FanoutCoordinatorDeployment` (replaces `DocumentProcessorDeployment` as dispatcher entry point)
- `PageWorkerDeployment` (replaces `DocumentProcessorDeployment` as conversion backend)

New internal request model:
- `WorkerRequest` as a discriminated union with `chunk_convert`, `materialized_convert`, and `passthrough_task` variants

New Ray config flags:
- `enable_pdf_page_chunk_fanout: bool`
- `max_page_chunk_size: int`
- `max_page_chunk_parallelism: int | None`

Serve deployment settings should split by deployment instead of staying global:
- Coordinator-specific: `coordinator_min_replicas`, `coordinator_max_replicas`, `coordinator_target_requests_per_replica`, `coordinator_max_ongoing_requests_per_replica`, `coordinator_num_cpus`, `coordinator_memory_limit`, `coordinator_upscale_delay_s`, `coordinator_downscale_delay_s`, `coordinator_graceful_shutdown_wait_loop_s`, `coordinator_graceful_shutdown_timeout_s`
- Worker-specific: `worker_min_replicas`, `worker_max_replicas`, `worker_target_requests_per_replica`, `worker_max_ongoing_requests_per_replica`, `worker_num_cpus`, `worker_memory_limit`, `worker_upscale_delay_s`, `worker_downscale_delay_s`, `worker_graceful_shutdown_wait_loop_s`, `worker_graceful_shutdown_timeout_s`
- Shared/global can remain shared in v1: Redis config, dispatcher settings, tenant fairness limits, `task_timeout`, `heartbeat_interval`, `results_ttl`, object store memory, and logging

New internal models:
- `MaterializedSource(bytes, page_count, filename)`
- `ChunkSpec(page_range, chunk_index)`
- `ChunkResult(status, document, errors, timings, page_range)`
- `SplitPlan(total_pages, chunks, effective_page_range)`

No new public `TaskType`.
No new public request option for chunk size.
No chunk-level progress exposed in v1.
No new task-level terminal status: `TaskStatus` remains `PENDING / STARTED / SUCCESS / FAILURE`.

## Opening Questions For Refinement After Handoff
- Should shared artifacts be restart-safe by spilling to a configurable durable store (S3/GCS) rather than relying on Ray's default local disk spilling?
- Should chunk-level progress be exposed on the parent task, or should status remain coarse?
- Should later versions support multi-source requests by planning one sub-plan per eligible PDF source?
- After profiling, is backend-open overhead large enough that sub-PDF creation becomes worth the added complexity?
- **Coordinator idle cost at scale**: the coordinator replica holds its slot for the full parent task lifetime. With cheap resource budget and relaxed `max_ongoing_requests`, this is acceptable in v1. If coordinator slots become a bottleneck at scale, the Ray Core task DAG approach (`finalize.remote(*child_refs)`) releases the coordinator immediately after submitting children, but requires moving heartbeat and finalization into a plain Ray worker with the supervision trade-offs documented during plan review.

## Test Plan
- PDF below threshold:
  - processed via coordinator → single worker request (no fan-out)
  - no ObjectRef created beyond what the single request needs
  - no extra latency beyond the coordinator hop
- Non-PDF format:
  - coordinator passes through to worker as-is
  - no materialization in coordinator
- PDF exceeds `max_num_pages` or `max_file_size`:
  - coordinator fails parent before creating ObjectRef or sending any worker request
- PDF above threshold:
  - correct chunk plan for exact multiples and remainder chunks
  - children receive correct non-overlapping sub-ranges of the effective page range
  - caller-supplied `page_range` is respected: child ranges are sub-slices, not replacements
  - final concatenated page ordering is correct with no renumbering
- Remote PDF source:
  - source is downloaded once in the coordinator, not once per child
- Uploaded PDF source:
  - source bytes are decoded once in the coordinator
  - worker receives ObjectRef, not a copy of the bytes per child
- Child parallelism:
  - when `max_page_chunk_parallelism` is set, coordinator never exceeds that many in-flight worker requests
  - when `max_page_chunk_parallelism` is unset, coordinator may submit the full child set
- Warm converter reuse:
  - worker replicas do not re-initialize `DoclingConverterManager` between child requests from the same or different parents
- Tenant fairness:
  - two tenants each submitting one large PDF still schedule at parent fairness boundaries
  - child worker requests do not change Redis `active_tasks` / `active_documents`
- Status/result:
  - parent-only polling and result retrieval still work
  - result shape is identical to current single-document behavior for all-success case
- Partial-success path:
  - one child fails, remaining children complete
  - parent task status is `SUCCESS`
  - result document carries `ConversionStatus.PARTIAL_SUCCESS` with a gap in the assembled document covering the failed chunk's page range
- All-failure path:
  - all children fail, parent task status is `FAILURE`, ObjectRef is released
- Cleanup:
  - ObjectRef is released on success, partial-success, failure, and coordinator crash recovery

## Locked V1 Scope / Non-goals
- First cut is Ray-only in `docling-jobkit`.
- Fan-out is eligible only for single-source PDF convert requests. All other formats and all multi-source requests stay on the existing single-task path.
- Chunking is internal server/orchestrator policy, not a request option.
- `page_range` is the only child slicing mechanism in v1; there is no physical sub-PDF splitting.
- Parent/child means one public parent task plus internal ephemeral Serve child requests, not Redis-visible child tasks.
- Dispatcher and Redis admission remain parent-granularity; tenant fairness and billing remain parent-logical, and internal child work may be capped by `max_page_chunk_parallelism` when configured.
- Page numbering is absolute in docling output: `page_range=(5,10)` produces pages 5–10, enabling correct concatenation without renumbering.
- The shared PDF artifact lives in Ray object storage (plasma store) only in v1, with Ray's built-in disk spilling as the OOM safety valve.
- `max_num_pages` and `max_file_size` are hard gates applied before fan-out; fan-out does not bypass them.
- Task-level status stays binary (`SUCCESS` / `FAILURE`); partial page coverage is expressed in the result document's `ConversionStatus`, not in `TaskStatus`.
- There is no automatic parent retry in the current system; coordinator death terminally fails the parent task.

## Appendix: Implemented

Implemented in `docling-jobkit`:
- Added `convert/materialization.py` with canonical single-source PDF materialization + preflight, including `MaterializationLimits`, `MaterializedSource`, and limit-specific failures.
- Split the Ray Serve processing plane into `FanoutCoordinatorDeployment` and `PageWorkerDeployment` in `orchestrators/ray/serve_deployment.py`.
- Added internal Ray models for `SplitPlan`, `ChunkSpec`, `ChunkResult`, and the worker request/result payloads in `orchestrators/ray/models.py`.
- Added the internal fan-out config in `orchestrators/ray/config.py`:
  `enable_pdf_page_chunk_fanout`, `max_page_chunk_size`, `max_page_chunk_parallelism`, plus only the coordinator-specific overrides that remained necessary: `coordinator_target_requests_per_replica`, `coordinator_max_ongoing_requests_per_replica`, `coordinator_num_cpus`, and `coordinator_memory_limit`.
- Kept the dispatcher contract stable: the dispatcher still submits a single parent task through `process_task(task)` on the coordinator handle.
- Implemented single-source PDF convert orchestration:
  coordinator preflight/materialization, `ray.put()` shared artifact, optional page-range split plan, bounded or unbounded child fan-out, chunk collection, and final assembly with `DoclingDocument.concatenate`.
- Preserved parent-owned Redis lifecycle in the coordinator: STARTED update, execution lease heartbeat, durable success/failure terminalization, pub/sub updates, and tenant stats.
- Updated `convert/results.py` so `ConversionStatus.PARTIAL_SUCCESS` is exportable and counted as a successful logical document for result packaging and stats.

Implemented in `docling-serve`:
- Added Ray settings for the fan-out feature flags and the minimal coordinator override set in `docling_serve/settings.py`.
- Wired those settings into `RayOrchestratorConfig` construction in `docling_serve/orchestrator_factory.py`.

Validation run:
- `uv run pytest -q tests/test_ray_fanout.py`
- `uv run pytest -q tests/test_ray_orchestrator.py -k create_deployment`
- `uv run pytest -q tests/test_ray_dispatcher_hardening.py -k 'document_processor_deployment_stringifies_replica_id or serve_replica_does_not_delete_dispatch_key'`

Notes:
- The worker request models pass filename metadata plus a Ray `ObjectRef`; the full materialized bytes remain owned by the coordinator-local `MaterializedSource`.
- Fan-out eligibility is locked to single-source PDF `CONVERT` tasks, matching the v1 scope.
