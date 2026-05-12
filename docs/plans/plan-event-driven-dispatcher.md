# Plan: Event-Driven Dispatcher (Steps 1–3)

## Goal

Remove the fixed latency floor and periodic Redis hammering from the task dispatcher
without changing the data model or payload handling.

After this plan:
- A newly enqueued task is admitted to Ray Serve in milliseconds, not up to
  `dispatcher_interval` seconds.
- The dispatcher does zero Redis `SCAN` operations while idle.
- Reconciliation (stale-task detection) runs at a 30 s cadence instead of every round.
- `dispatcher_interval` config knob still exists but becomes the slow-path resync
  cadence (fallback heartbeat), not the hot-path latency.

## Background / Why this matters

The current dispatcher in
`docling_jobkit/orchestrators/ray/dispatcher.py` runs
`_run_dispatch_loop()` (line 158) as:

```
while active:
    await _dispatch_round()          # does: reconcile + SCAN + N×Redis reads
    await asyncio.sleep(interval)    # DEFAULT 2.0 s → up to 2 s added latency
```

`_dispatch_round()` (line 235) calls `_reconcile_active_tasks()` and then
`get_all_tenants_with_tasks()` (redis_helper.py:233), which does a full `SCAN`
over all `tenant:*:tasks` keys plus an `LLEN` per key — every single round,
even when the system is idle.

The dispatcher is a **single Ray actor** (`@ray.remote class RayTaskDispatcher`)
and the orchestrator already holds a handle to it. This means there is a fast,
zero-Redis signalling path available: a fire-and-forget Ray remote call.

## Architecture

### Three changes, in isolation

#### 1. asyncio.Event wakeup (replaces the sleep)

Add a `_wake_event: asyncio.Event` to `RayTaskDispatcher.__init__`.

Replace:
```python
await asyncio.sleep(self.config.dispatcher_interval)
```
with:
```python
try:
    await asyncio.wait_for(
        self._wake_event.wait(),
        timeout=self.config.dispatcher_interval,  # now the slow-path cadence
    )
except asyncio.TimeoutError:
    pass
self._wake_event.clear()
```

The loop then runs the fast dispatch pass immediately on any wake, and falls
back to running at `dispatcher_interval` (default 2 s → should be raised to
~30 s once this is working) as a safety heartbeat.

Add a public Ray method:

```python
async def wake(self, tenant_id: str | None = None) -> None:
    """Signal the dispatcher to run a dispatch pass immediately."""
    if tenant_id is not None and tenant_id not in self._active_tenant_ids:
        self._active_tenant_ids.add(tenant_id)
        self._active_tenant_order.append(tenant_id)
    self._wake_event.set()
```

#### 2. Wakeup callers

Two places must call `dispatcher.wake.remote()` (fire-and-forget, no await):

**On enqueue** — `orchestrator.py`, at the end of `enqueue()`, after
`await self.redis_manager.enqueue_task(tenant_id, task)`:

```python
# After redis_manager.enqueue_task, before returning task
if self._dispatcher_handle is not None:
    self._dispatcher_handle.wake.remote(tenant_id=tenant_id)
```

The orchestrator already stores the dispatcher as `self._dispatcher` or
accesses it via `ensure_dispatcher_ready()`. Check the exact attribute name
and use the existing handle — no new state needed.

**On task completion** — `dispatcher.py`, in `_process_task_async()`, after
`response.result()` returns (line ~370). The success-path Redis writes
(`finalize_task_success_atomic`) happen **inside the Serve deployment**, not
in the dispatcher. By the time `response.result()` returns the dispatcher-side
work is done, and capacity has been freed — this is the right place to wake:

```python
# After response.result() returns (both success and error paths)
# Capacity was freed; wake the dispatch loop so queued work can advance.
self._wake_event.set()
```

This is an in-process set (not a Ray call) because `_process_task_async` runs
inside the same dispatcher actor. No extra overhead.

Note: `_process_task_async` runs as a fire-and-forget coroutine from the
dispatch loop; the `_wake_event.set()` call happens when the Ray future
resolves, which is entirely decoupled from the original dispatch iteration.

#### 3. In-memory active tenant ring + move reconciliation to elapsed-time cadence

**Ordered structure for fair round-robin**

Use a `deque` for iteration order and a `set` for O(1) membership, so
tenants are served in arrival order and no tenant monopolises the fast path:

```python
self._active_tenant_ids: set[str] = set()          # membership
self._active_tenant_order: deque[str] = deque()    # round-robin order
self._last_reconcile: float = 0.0
self._reconcile_interval: float = 30.0  # seconds
```

**Populate on startup**: change `_run_dispatch_loop` to call a new
`_resync_active_tenants()` once before the main loop:

```python
async def _resync_active_tenants(self) -> None:
    """Rebuild in-memory active tenant set from Redis. Called on startup and slow-path."""
    tenants = await self.redis_manager.get_all_tenants_with_tasks()
    for t in tenants:
        if t not in self._active_tenant_ids:
            self._active_tenant_ids.add(t)
            self._active_tenant_order.append(t)
```

**Maintain during dispatch**: in `_dispatch_tenant_task`, when the tenant
queue is empty (on peek or after draining):

```python
# On empty peek:
if task is None:
    self._active_tenant_ids.discard(tenant_id)
    return False
```

```python
# After successful dispatch, if queue now empty:
remaining = await self.redis_manager.get_tenant_queue_size(tenant_id)
if remaining == 0:
    self._active_tenant_ids.discard(tenant_id)
```

(The deque may still contain the tenant ID as a stale entry; check set
membership when popping from the deque and skip entries not in the set.)

**Change `_dispatch_round` to use in-memory ring**: replace
`await self.redis_manager.get_all_tenants_with_tasks()` with iteration
over `_active_tenant_order`. If `_active_tenant_ids` is empty, return
immediately. Pattern:

```python
async def _dispatch_round(self) -> None:
    if not self._active_tenant_ids:
        return
    # rotate through the deque; re-append still-active tenants at the end
    active_snapshot = len(self._active_tenant_order)
    for _ in range(active_snapshot):
        if not self._active_tenant_order:
            break
        tenant_id = self._active_tenant_order.popleft()
        if tenant_id not in self._active_tenant_ids:
            continue  # already removed, skip stale deque entry
        had_work = await self._dispatch_tenant_task(tenant_id)
        if tenant_id in self._active_tenant_ids:
            self._active_tenant_order.append(tenant_id)  # keep in ring
```

**Reconciliation runs on elapsed time, every loop iteration** — NOT only on
the slow-path timeout. Under constant enqueue/completion wakeups the timeout
may never fire, so stale `STARTED` tasks would never be reconciled. The check
must run on every loop iteration:

```python
now = asyncio.get_event_loop().time()
if now - self._last_reconcile >= self._reconcile_interval:
    await self._reconcile_active_tasks()
    await self._resync_active_tenants()  # heals any missed wakeups
    self._last_reconcile = now
```

This block goes at the bottom of every loop iteration, regardless of whether
the wake came from an event or a timeout.

## Dispatch loop after changes

```
startup:
  _resync_active_tenants()         # one SCAN, populates ordered ring

loop:
  wait(event OR timeout=30s)       # blocks; wakes on enqueue / completion / slow tick
  clear event

  # fast dispatch pass (uses in-memory ring, no Redis SCAN)
  while _active_tenant_ids not empty:
      rotate through _active_tenant_order
          dispatch until tenant capacity exhausted or queue empty
          if queue empty: remove from _active_tenant_ids

  # time-based reconciliation (every ~30s, regardless of wake source)
  now = monotonic()
  if now - _last_reconcile >= _reconcile_interval:
      _reconcile_active_tasks()    # SCAN, heals orphaned tasks
      _resync_active_tenants()     # SCAN, heals missed wakeups
      update_heartbeat()
      _last_reconcile = now
```

## Timing knobs — kept separate

The plan introduces or preserves **three independent knobs** to avoid coupling:

| Knob | Owner | Purpose | Default |
|------|-------|---------|---------|
| `dispatcher_interval` | `RayDispatcherConfig` | Dispatcher slow-path timeout (fallback wakeup cadence); also drives heartbeat TTL | 30.0 s |
| `_reconcile_interval` | in-process constant | Elapsed-time guard for reconciliation; separate so reconciliation cadence doesn't drift with `dispatcher_interval` tuning | 30.0 s |
| `supervisor_poll_interval` | `_supervise_dispatcher()` in `orchestrator.py` | Health-check cadence for the supervisor loop — **must not reuse `dispatcher_interval`** | 5.0 s (hardcoded or new config field) |

The supervisor at `orchestrator.py:405` currently derives its poll interval as
`max(1.0, self.config.dispatcher_interval)`. Changing `dispatcher_interval` to
30 s would slow supervisor health detection to 30 s. Fix this by replacing:

```python
poll_interval = max(1.0, self.config.dispatcher_interval)
```

with a fixed reasonable default (e.g. `5.0`) or a new dedicated config field
(`supervisor_poll_interval: float = 5.0`). The supervisor's health-check
cadence and the dispatcher's slow-path timeout serve different purposes and
should not share a knob.

## Failure modes and recoverability

### Dispatcher actor death

The dispatcher is a **detached Ray actor** (created with `lifetime="detached"` in
`orchestrator.py`). Ray will automatically restart a crashed detached actor.

On restart, `__init__` runs fresh: `_active_tenant_ids`/`_active_tenant_order`
are empty, `_wake_event` is unset. The first thing `_run_dispatch_loop` does is
`_resync_active_tenants()`, which does a Redis SCAN to rebuild the ordered ring.

**Startup recovery gap**: `_resync_active_tenants()` only re-enqueues tenants
that have tasks in PENDING state. Tenants whose tasks are in STARTED state (in
the middle of execution) are **not** re-added to the ring; those task slots stay
occupied until the reconciliation slow path runs for the first time (~30 s after
startup). During that window a tenant with available capacity and queued work
may not be dispatched. This is acceptable — 30 s is the ceiling — but must be
tested (see Testing section, item 6).

In-flight tasks are unaffected: the Serve worker that received
`deployment_handle.process_task.remote(task)` holds the Ray future independently
of the dispatcher actor. The worker writes results to Redis via
`finalize_task_success_atomic` regardless of whether the dispatcher is alive.
When the restarted dispatcher reconciles (first slow-path tick), it sees those
tasks as STARTED with an active execution-lease heartbeat and leaves them alone.

### Ray head node death

A Ray head restart destroys all actors (detached or not) and invalidates all
object refs. The orchestrator reconnects on the next request, re-creates the
detached dispatcher actor, and the same restart recovery described above applies.

Redis state survives independently (Redis is external to the cluster). All
PENDING tasks in Redis queues will be discovered via `_resync_active_tenants()`
on the new dispatcher's first dispatch loop iteration.

Tasks that were in STARTED state at cluster death will have stale execution-lease
heartbeats. The reconciliation slow path (first tick after restart) will fail them
with `"Task orphaned: processing state missing during reconciliation"`. Clients
receive FAILURE and can re-submit.

### Heartbeats: dispatcher heartbeat TTL is coupled to `dispatcher_interval`

**This is the most important behavioural change in this plan — it must be
handled explicitly.**

The dispatcher heartbeat TTL is computed in two places:

1. `redis_helper.py:116`: `_compute_dispatcher_heartbeat_ttl(dispatcher_interval)`
   → `max(ceil(dispatcher_interval * 3), 1)`
2. `dispatcher.py:107-110` in `refresh_runtime()`: same formula applied again.

With the old default of 2.0 s: TTL = 6 s → a dead dispatcher's Redis heartbeat
key expires in 6 s.

With the new default of 30.0 s: TTL = 90 s → the key expires in 90 s. Any
external monitoring or alerting that watches `get_dispatcher_heartbeat_age()` must
be updated to expect a stale threshold of ~90 s instead of ~6 s.

**Heartbeat update cadence after this change**: the heartbeat (`update_dispatcher_heartbeat()`)
is currently called at the top of every loop iteration. In the event-driven
design the loop only iterates on wake events or on the slow-path timeout. The
heartbeat must be updated in the reconciliation block (which runs on elapsed time)
so that an idle dispatcher does not let its heartbeat expire:

```python
# On reconciliation tick (every ~30 s)
if self.config.enable_heartbeat:
    await self.redis_manager.update_dispatcher_heartbeat()
self.last_heartbeat = datetime.datetime.now(datetime.timezone.utc)
```

The 90 s TTL is consistent with the "3 missed intervals" design intent: if the
slow-path fires every 30 s, three missed slow-path ticks = 90 s before the key
expires. External health checks should align with this window.

### Task execution heartbeats (staleness detection for STARTED tasks)

This plan does **not change** the task execution heartbeat logic. The relevant
config key is `heartbeat_interval` (default 30.0 s, separate from
`dispatcher_interval`), and the stale threshold is `heartbeat_interval * 4`
(returned by `_get_task_processing_stale_after()` in `dispatcher.py:531`).

The one change is indirect: reconciliation now runs every ~30 s instead of
every ~2 s. A STARTED task with a stale execution-lease heartbeat will be
failed up to ~30 s later than before. This is an acceptable tradeoff — the
stale cutoff is already `heartbeat_interval * 4 = 120 s`, so the additional
30 s detection delay is small relative to the existing stale window.

## Config changes

**`docling-jobkit` — `config.py`**: change description and default of `dispatcher_interval`:

```python
dispatcher_interval: float = Field(
    default=30.0,   # changed from 2.0
    description=(
        "Slow-path resync cadence in seconds. "
        "The dispatcher wakes immediately on new work; "
        "this is a fallback for reconciliation and missed-wakeup recovery."
    ),
)
```

**`docling-serve` — `settings.py`**: update `eng_ray_dispatcher_interval` to match:

```python
eng_ray_dispatcher_interval: float = 30.0  # changed from 2.0
```

This is required — otherwise `docling-serve` injects `2.0` into the config and
the default change in `docling-jobkit` has no effect in production.

Any deployed config files or Helm values that set `eng_ray_dispatcher_interval`
or `dispatcher_interval` explicitly must also be reviewed.

## Files to change

| File | What changes |
|------|-------------|
| `docling_jobkit/orchestrators/ray/dispatcher.py` | `__init__`: add `_wake_event`, `_active_tenant_ids` (set), `_active_tenant_order` (deque), `_last_reconcile`; add `wake(tenant_id)` method; rewrite `_run_dispatch_loop`; rewrite `_dispatch_round` to use ordered ring; update `_dispatch_tenant_task` to maintain ring; move reconciliation to elapsed-time guard on every iteration; set `_wake_event` after `response.result()` in `_process_task_async` |
| `docling_jobkit/orchestrators/ray/orchestrator.py` | `enqueue()`: call `dispatcher.wake.remote(tenant_id=tenant_id)` after `redis_manager.enqueue_task`; `_supervise_dispatcher()`: replace `max(1.0, self.config.dispatcher_interval)` with a fixed 5 s poll or new config field |
| `docling_jobkit/orchestrators/ray/config.py` | Change `dispatcher_interval` default to 30.0 and update description |
| `docling_serve/settings.py` | Change `eng_ray_dispatcher_interval` default from 2.0 to 30.0 |

## What does NOT change

- Redis data model (queue keys, active sets, limits, metadata) — unchanged.
- `RedisStateManager` — no new methods needed for this plan.
- Serve deployment — no changes.
- Task data model — unchanged.
- All existing tests should pass; the observable behaviour is identical except
  tasks start sooner.

## Testing

The key observable differences to verify:

1. **Latency**: submit a task with `dispatcher_interval=30.0`; task should reach
   STARTED status within <500 ms, not after 30 s.

2. **No idle SCAN**: with an idle queue and `dispatcher_interval=30.0`, verify
   Redis gets no `SCAN` traffic between the initial startup scan and the 30 s
   slow-path tick.

3. **Capacity-freed wakeup**: fill a tenant to its concurrency limit; complete
   one task; the next queued task should start within <500 ms (not up to
   `dispatcher_interval`).

4. **Missed-wakeup recovery**: manually add a task to Redis without going
   through `enqueue()` (simulating a crash-recovery scenario); it should be
   discovered within `dispatcher_interval` seconds via the slow-path resync.

5. **Existing reconciliation tests**: all tests in `test_ray_dispatcher_api.py`
   and related suites should pass unchanged.

6. **Restart recovery gap**: restart the dispatcher actor while a tenant has a
   task in STARTED state and a task in PENDING state. Verify the PENDING task
   starts within ≤ `_reconcile_interval` seconds (not blocked indefinitely).
   Verify the STARTED task is left alone until its execution-lease heartbeat
   goes stale.

7. **Fair round-robin**: enqueue tasks for three tenants simultaneously; verify
   each tenant gets dispatched in rotation rather than one tenant being starved.

8. **Supervisor poll cadence**: verify the supervisor health loop continues to
   check every ~5 s after `dispatcher_interval` is raised to 30 s.

## Rollout risk

Low. The change is entirely internal to the dispatcher actor. Redis data model,
API, and client-visible task states are unchanged. The only observable difference
is that tasks start faster and Redis sees fewer reads at idle.

If a bug in the in-memory ring causes a tenant to be missed, the 30 s slow-path
resync catches it. The worst case is a 30 s delay for an edge-case task — the
same latency as the old default interval. The slow path is a safety net, not
a correctness dependency.
