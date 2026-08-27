# Handoff: suspected Redis tenant-limits bookkeeping race

## Status

Confirmed. The suspected read-modify-write race is reproduced by a deterministic
regression test, `test_enqueue_limits_update_does_not_restore_stale_active_usage`
in `tests/test_ray_lifecycle_counters.py`. On the current code that test fails:
the final state is `active_tasks=1, queued_tasks=1` over an empty active set —
the production `BLOCKED_RACE_SIGNATURE`. It is checked in as `xfail(strict=True)`;
applying the fix in [How to fix](#how-to-fix) flips it to a real pass, at which
point the marker is removed. See [Confirmation](#confirmation) for details.

## Incident context

All identifiers in this document are synthetic. The affected tenant is called
`tenant-A`, the task being watched is `task-observed`, and earlier tasks are
named by their role in the sequence.

The client-facing trigger was accepted work remaining pending, the tenant queue
not moving, and no new tasks beginning processing.

For `tenant-A` we observed:

- `task-observed` was accepted and enqueued, but there was no dispatch or task
  start log for it. Repeated status polling continued to return the pending task.
- Redis still contained `task-observed` in the tenant queue. It was at position
  51 in a queue of 52 pending tasks, so it had not been removed or lost between
  Redis and Ray.
- The last two tasks dispatched before the queue stopped both reached durable
  `success`. Their execution completed normally and their results were fetched.
- The dispatcher heartbeat was current.
- The canonical active-task set was empty, and there were no remaining dispatch
  or execution lease hashes for the tenant.
- The tenant limits hash nevertheless reported:

  ```text
  max_concurrent_tasks = 12
  active_tasks         = 12
  queued_tasks         = 52
  active_documents     = 1
  converter_units      = 5
  ```

- The monotonic lifecycle counters agreed with the queue: enqueued minus
  dispatched was 52.

### Key observed symptom

The decisive Redis symptom was the disagreement between the canonical active
set and the derived limits hash while the queue remained non-empty:

```text
LLEN  tenant:tenant-A:tasks                         = 52
SCARD tenant:tenant-A:active_tasks                  = 0
HGET  tenant:tenant-A:limits active_tasks           = 12
HGET  tenant:tenant-A:limits max_concurrent_tasks   = 12
```

This state blocks all further dispatch. Redis has no active-task members, but
`check_tenant_can_process()` rejects the next queued task because the derived
limits hash says `active_tasks == max_concurrent_tasks`. Both the false capacity
gate and the race that produced the stale hash are now confirmed — the gate by
the production observation and recovery, the race by the regression test.

## Recovery observation

After confirming the active set was empty, no dispatch or execution leases
remained, the dispatcher heartbeat was fresh, and the queue count matched the
derived queued count, operations reset only these stale derived fields to zero:
`active_tasks`, `active_documents`, and `converter_units`.

The existing queue then drained without deleting or re-enqueuing any task. The
watched pending task advanced through start and success, all queued work
completed, and the queue, active set, and usage gauges returned to zero. The
monotonic lifecycle counters also converged with every dispatched task reaching
a terminal state.

This recovery confirmed the immediate causal mechanism: stale values in the
limits hash can falsely close the capacity gate while canonical Redis state has
no active work. The read-modify-write interleaving that produces the stale hash
is now independently confirmed by the regression test (see
[Confirmation](#confirmation)).

## Root cause (confirmed)

`RedisStateManager.update_tenant_limits()` performs a non-atomic
read-modify-write of the entire tenant limits hash:

1. `get_tenant_limits()` reads every field into a `TenantLimits` snapshot.
2. Python changes one or more fields on that snapshot.
3. `HSET ... mapping=limits.model_dump()` writes every field back.

`enqueue_task()` first appends the task and increments the monotonic enqueue
counter atomically, then separately calls `update_tenant_limits()` to increment
`queued_tasks`.

Terminalization uses a Redis transaction to remove the task from the active set
and decrement `active_tasks`. These two paths can interleave as follows:

```text
Initial state:
  active set = {task-running}
  limits.active_tasks = 1
  limits.queued_tasks = 0

Enqueue path:
  reads a full limits snapshot: active_tasks=1, queued_tasks=0
  pauses

Terminalization path:
  marks task-running successful
  removes task-running from the active set
  decrements limits.active_tasks to 0

Enqueue path resumes:
  increments queued_tasks in its stale snapshot
  writes the full snapshot:
    active_tasks=1, queued_tasks=1

Final state:
  active set = {}
  limits.active_tasks = 1
  limits.queued_tasks = 1
```

With a concurrency limit of one, this minimal interleaving wedges the tenant.
The production value of 12 is the same failure at a larger scale. Because the
full hash is rewritten, the same race can also restore stale
`active_documents` or `converter_units` values.

The inconsistency does not self-heal: reconciliation enumerates tenants from
non-empty active-task sets. A tenant whose canonical active set is already empty
is not selected for `resync_tenant_limits()`.

## Confirmation

Reproduced by `test_enqueue_limits_update_does_not_restore_stale_active_usage`
in `tests/test_ray_lifecycle_counters.py`, using the file's in-memory Redis fake
— no Ray, no live Redis, no sleeps. It seeds one running task
(`active_tasks=1`, `queued_tasks=0`, `max_concurrent_tasks=1`), pauses the
enqueue coroutine right after it captures its limits snapshot, runs
`finalize_task_success_atomic()` to completion (active set → empty,
`active_tasks` → 0), then releases enqueue and asserts the invariant that should
hold:

```python
assert await manager.get_tenant_active_task_count("tenant-A") == 0
assert (await manager.get_tenant_limits("tenant-A")).active_tasks == 0
assert (await manager.get_tenant_limits("tenant-A")).queued_tasks == 1
assert (await manager.check_tenant_can_process("tenant-A", 1))[0]
```

On current code the enqueue's final `HSET` restores `active_tasks=1`, so the
assertion fails with `active_tasks=1, queued_tasks=1` over an empty active set —
the runbook's `BLOCKED_RACE_SIGNATURE`. The test is checked in as
`xfail(strict=True)` so it stays green until the fix lands and then, on passing,
turns the strict-xfail into a failure that forces the marker's removal. Run it:

```bash
.venv/bin/pytest -q tests/test_ray_lifecycle_counters.py \
  -k enqueue_limits_update_does_not_restore_stale_active_usage
```

(Adding this test also required a direct awaited `hset` on the file's fake Redis;
earlier tests had stubbed `update_tenant_limits()` out, so the fake never
exercised it.)

## How to fix

Replace the whole-hash read-modify-write in `update_tenant_limits()` with atomic
per-field `HINCRBY`s — the same primitive terminalization already uses (the
`HINCRBY active_tasks -1` inside `_finalize_task_terminal_state_atomic()`). Each
caller then writes only the fields it owns via its deltas (`delta_queued_tasks`,
`delta_active_tasks`, `delta_docs`), never a field it did not change, so a
concurrent finalize's decrement can no longer be clobbered. Seed the immutable
config fields (`max_concurrent_tasks`, `max_queued_tasks`, `max_documents`) once
at tenant init rather than on every counter update.

One gotcha: `HINCRBY` cannot clamp at zero the way today's `max(0, ...)` does.
Once the writes are atomic a counter should not go negative; if a floor is still
wanted, apply it in a small Lua/`WATCH` step rather than reintroducing the
read-modify-write.

Do not add dispatcher retries or a periodic repair loop — those treat the
symptom. Both callers of `update_tenant_limits()` stay: enqueue increments
`queued_tasks`, the legacy dequeue path decrements it. Terminalization and
dispatch already mutate usage fields inside Redis transactions and must not be
overwritten by either caller. When the fix lands, remove the test's
`xfail(strict=True)` marker.
