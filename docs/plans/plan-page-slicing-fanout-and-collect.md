# Ray PDF Fan-Out / Collect With Shared Artifacts

## Context
The current Ray path is `API request -> one Redis task -> one dispatcher admission -> one Serve replica call -> one terminal result`. `page_range` already works end-to-end and the Docling PDF pipelines only instantiate/process pages inside the selected range, so the page-slicing primitive exists today. The missing pieces are orchestration and transport: one logical request needs to become many page-slice executions without re-downloading or re-serializing the same large PDF.

Page numbering is confirmed absolute: when `page_range=(5,10)` is requested, the output `DoclingDocument` contains pages numbered 5–10 (not 1–6). `DoclingDocument.concatenate` therefore works correctly across slices with no renumbering needed.

The locked v1 scope and non-goals are summarized in the final section of this document to avoid repeating the same invariants throughout.

## Summary
Implement PDF page-slice fan-out as a **logical parent task with ephemeral internal child executions**, not as Redis-visible child tasks.

Split the current monolithic Ray Serve document processor deployment into two cooperating deployments with different resource profiles:

- **`DoclingProcessorCoordinatorDeployment`** (new, cheap): owns the parent task lifecycle — execution lease heartbeat, Redis finalization, source materialization, split planning, result assembly. Runs with relaxed concurrency (`max_ongoing_requests > 1`) because its work is mostly I/O and waiting. Holds no model weights, needs no GPU.
- **`DoclingProcessorConverterDeployment`**: owns warm `DoclingConverterManager` instances and executes conversion work. Keeps `max_ongoing_requests=1` to preserve thread-safety. Receives pre-materialized PDF bytes as a `ray.ObjectRef` plus a page range; returns a conversion result. Has no Redis lifecycle responsibility.

The coordinator receives every parent task from the dispatcher. For single-source PDF tasks whose effective page range exceeds the slice threshold, it fans out N child requests to the converter deployment, awaits them asynchronously, and assembles the result. For all other tasks, it sends a single request to the converter and passes the result through. The coordinator is never resource-idle on a heavy replica — the worst case is a cheap coordinator slot waiting, while GPU-capable converter slots remain available for real conversion work.

This avoids:
- N repeated HTTP downloads for one remote PDF.
- N base64 copies of one uploaded PDF in Redis.
- Reworking the Redis scheduler to understand durable parent/child task graphs in v1.
- Deadlocking the Serve replica pool: coordinator and worker are separate deployments; parents never compete with children for the same replica pool.
- Re-initializing `DoclingConverterManager` per child: converter replicas stay warm across all child requests, exactly as today.

## Implementation Changes

### 1. Two-deployment architecture

Replace the single-deployment layout with two cooperating deployments deployed together:

**`DoclingProcessorCoordinatorDeployment`**
- Resource budget: `num_cpus=0.5`, moderate memory (enough to hold a materialized PDF and an assembled DoclingDocument), `num_gpus=0`.
- `max_ongoing_requests`: relaxed (e.g. 4–8) — coordinator work is async waiting, not CPU-bound.
- Holds: `RedisStateManager`, execution lease heartbeat logic, finalization calls, handle to `DoclingProcessorConverterDeployment`.
- Does not hold a `DoclingConverterManager`.

**`DoclingProcessorConverterDeployment`**
- Resource budget: unchanged (full GPU + memory for warm models).
- `max_ongoing_requests=1`: required by thread-unsafe `DocumentConverter` and preserved for every replica that holds a warmed `DoclingConverterManager`.
- Holds: persistent `DoclingConverterManager` (`self.cm`) exactly as today.
- Public Serve entry point: `process_converter_request(request: ConverterRequest)` where `ConverterRequest` is a discriminated union with variants for `slice_convert`, `materialized_convert`, and `passthrough_task`.
- Internal helpers may still be named `process_slice` / `_process_materialized_convert` / `_process_passthrough_task`, but they are converter-internal branches behind one Serve boundary.
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

Extract a module-level helper `materialize_and_preflight(source, limits) -> MaterializedSource` in `docling-jobkit` and make it the canonical admission/preflight path for single-source PDF inputs. The coordinator uses it before deciding between sliced fan-out and the single-request materialized path. This avoids two diverging implementations of the same logic without trying to "extract" behavior from `DoclingConverterManager.convert_documents()`, which is currently only a thin wrapper around the lower-level converter call.

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

`max_num_pages` is validated against the **total** PDF page count, not the per-slice size. This is the correct gate: fan-out does not bypass the document size limit, it only provides a more efficient execution strategy for documents that are within limits but large.

### 5. Child execution contract

Children are Ray Serve requests to `DoclingProcessorConverterDeployment`, not `ray.remote` functions. This preserves warm `DoclingConverterManager` reuse: converter replicas keep their model state between child requests exactly as they do between sequential task calls today. The warm converter is the reason the current Serve design is performant for OCR/VLM-heavy workloads; child requests must share it.

Child input to the worker's `slice_convert` request variant:
- `artifact_ref: ray.ObjectRef` — reference to materialized PDF bytes in the plasma store
- `page_range: tuple[int, int]` — sub-slice of the effective page range (see below)
- `options: ConvertDocumentsOptions` — copied from parent, with `page_range` overridden to the child's sub-range
- `filename`: original filename needed for export and hashing
- `slice_index: int` — ascending position in the slice plan for deterministic reassembly

Caller-supplied `page_range` interaction: if the original request includes `page_range=[10, 50]`, child ranges are sub-slices of that range — `[10, 19]`, `[20, 29]`, …, `[50, 50]`. The child's page range always overrides the parent's convert_options.page_range, not replaces the field wholesale.

Child behavior in the worker's `slice_convert` branch:
- Dereference `artifact_ref` via `ray.get()` to obtain PDF bytes (shared memory, zero-copy on the same node).
- Build a `DocumentStream` from the bytes.
- Call `self.cm.convert_documents(...)` with the child page range — exactly the existing warm converter path.
- Do not reinterpret `max_num_pages` as a per-child limit. The full-document admission gate already ran once during coordinator preflight.
- Return an `ExportableDocument` carrying conversion status, document payload when exportable, errors, timings, `page_range`, and `slice_index`.

Do not split into physical sub-PDFs in v1.

### 6. Child parallelism and coordinator occupancy

Parent tasks still consume tenant slot counters as one logical task and one logical document. Child requests to the converter deployment are internal and must not increment Redis tenant counters.

The coordinator awaits child futures asynchronously over Serve handle futures. The coordinator replica is not CPU-bound while waiting, and because coordinator replicas carry no GPU or heavy model resources, the idle cost during the wait is low — a cheap coordinator slot, not a GPU.

`max_page_slice_parallelism` bounds the number of in-flight child requests. When explicitly set, it caps slice fan-out directly. When unset, it defaults to `max_concurrent_tasks`, so the default behavior is still bounded and one large PDF cannot flood the worker pool by accident. Use a bounded work-queue pattern (`asyncio.wait`, `as_completed`, semaphore, or equivalent) to submit up to `min(number_of_slices, max_page_slice_parallelism)` initially and refill as children complete.

### 7. Collect and final result assembly

Await all child futures. Collect all results regardless of individual child outcome.

Task-level status remains binary — **`SUCCESS` or `FAILURE`** — consistent with the existing task lifecycle stack (`TaskStatus` in `task.py`, Redis terminalization in `redis_helper.py`, reconciliation in `dispatcher.py`). Document-level partial semantics are already represented in `ConversionStatus.PARTIAL_SUCCESS` within the result payload and are already handled in `results_processor.py`. Introducing a third task-level terminal state would ripple across all of those layers for no additional expressiveness over what the result payload already provides.

Result assembly rules:
- At least one child succeeds: coordinator assembles a result document from the successful slices. The assembled document carries `ConversionStatus.PARTIAL_SUCCESS` if any children failed, `ConversionStatus.SUCCESS` if all succeeded. The task is marked **`SUCCESS`** and the result is stored.
- All children fail: task is marked **`FAILURE`**, no result document is stored.
- Fan-out setup fails before any child launches: task is marked **`FAILURE`** immediately.

Concatenate successful child documents with `DoclingDocument.concatenate` in ascending page-range order. The assembled document is exported through the same export helper as the current single-document path; no new export code path is needed.

Internally, export/result processing should operate on an `ExportableDocument` model rather than directly on `ConversionResult`. This keeps slice metadata (`page_range`, `slice_index`) attached through collection and assembly while preserving the existing export targets and response formats.

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
- `DoclingProcessorCoordinatorDeployment` as dispatcher entry point
- `DoclingProcessorConverterDeployment` as conversion backend

New internal request model:
- `ConverterRequest` as a discriminated union with `slice_convert`, `materialized_convert`, and `passthrough_task` variants

New Ray config flags:
- `enable_pdf_page_slice_fanout: bool`
- `max_page_slice_size: int`
- `max_page_slice_parallelism: int | None`

Serve deployment settings should stay mostly shared in v1, with only the coordinator overrides split where they materially matter:
- Coordinator-specific: `coordinator_min_actors`, `coordinator_max_actors`, `coordinator_target_requests_per_replica`, `coordinator_max_ongoing_requests_per_replica`, `coordinator_actor_num_cpus`, `coordinator_actor_memory_request`
- Worker-specific: `min_actors`, `max_actors`, `target_requests_per_replica`, `max_ongoing_requests_per_replica`, `converter_actor_num_cpus`, `converter_actor_memory_request`
- Shared/global: `upscale_delay_s`, `downscale_delay_s`, `graceful_shutdown_*`, Redis config, dispatcher settings, tenant fairness limits, `task_timeout`, `heartbeat_interval`, `results_ttl`, object store memory, and logging

New internal models:
- `MaterializedSource(bytes, page_count, filename)`
- `SliceSpec(page_range, slice_index)`
- `SlicePlan(total_pages, slices, effective_page_range)`
- `ExportableDocument(file, document_hash, status, errors, timings, document, page_range, slice_index)`

No new public `TaskType`.
No new public request option for slice size.
No slice-level progress exposed in v1.
No new task-level terminal status: `TaskStatus` remains `PENDING / STARTED / SUCCESS / FAILURE`.

## Open Questions
- Should shared artifacts be restart-safe by spilling to a configurable durable store (S3/GCS) rather than relying on Ray's default local disk spilling?
- Should slice-level progress be exposed on the parent task, or should status remain coarse?
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
  - correct slice plan for exact multiples and remainder slices
  - children receive correct non-overlapping sub-ranges of the effective page range
  - caller-supplied `page_range` is respected: child ranges are sub-slices, not replacements
  - final concatenated page ordering is correct with no renumbering
- Remote PDF source:
  - source is downloaded once in the coordinator, not once per child
- Uploaded PDF source:
  - source bytes are decoded once in the coordinator
  - worker receives ObjectRef, not a copy of the bytes per child
- Child parallelism:
  - when `max_page_slice_parallelism` is set, coordinator never exceeds that many in-flight worker requests
  - when `max_page_slice_parallelism` is unset, coordinator defaults to `max_concurrent_tasks`
- Warm converter reuse:
  - converter replicas do not re-initialize `DoclingConverterManager` between child requests from the same or different parents
- Tenant fairness:
  - two tenants each submitting one large PDF still schedule at parent fairness boundaries
  - child worker requests do not change Redis `active_tasks` / `active_documents`
- Status/result:
  - parent-only polling and result retrieval still work
  - result shape is identical to current single-document behavior for all-success case
- Partial-success path:
  - one child fails, remaining children complete
  - parent task status is `SUCCESS`
  - result document carries `ConversionStatus.PARTIAL_SUCCESS` with a gap in the assembled document covering the failed slice's page range
- All-failure path:
  - all children fail, parent task status is `FAILURE`, ObjectRef is released
- Cleanup:
  - ObjectRef is released on success, partial-success, failure, and coordinator crash recovery

## Locked V1 Scope / Non-goals
- First cut is Ray-only in `docling-jobkit`.
- Fan-out is eligible only for single-source PDF convert requests. All other formats and all multi-source requests stay on the existing single-task path.
- Slicing is internal server/orchestrator policy, not a request option.
- `page_range` is the only child slicing mechanism in v1; there is no physical sub-PDF splitting.
- Parent/child means one public parent task plus internal ephemeral Serve child requests, not Redis-visible child tasks.
- Dispatcher and Redis admission remain parent-granularity; tenant fairness and billing remain parent-logical, and internal child work may be capped by `max_page_slice_parallelism` when configured.
- Page numbering is absolute in docling output: `page_range=(5,10)` produces pages 5–10, enabling correct concatenation without renumbering.
- The shared PDF artifact lives in Ray object storage (plasma store) only in v1, with Ray's built-in disk spilling as the OOM safety valve.
- `max_num_pages` and `max_file_size` are hard gates applied before fan-out; fan-out does not bypass them.
- Task-level status stays binary (`SUCCESS` / `FAILURE`); partial page coverage is expressed in the result document's `ConversionStatus`, not in `TaskStatus`.
- There is no automatic parent retry in the current system; coordinator death terminally fails the parent task.
