# Plan: prevent Redis tenant-limit bookkeeping races

## Context

Ray admission reads usage counters from `tenant:<id>:limits`. The tenant queue
and active-task set are the canonical occupancy state; the limits hash contains
derived counters such as `queued_tasks`, `active_tasks`, `active_documents`, and
`converter_units`.

Two write paths can overwrite concurrent changes with stale values:

- `update_tenant_limits()` reads and rewrites the complete limits hash to change
  individual counters.
- `resync_tenant_limits()` derives counters from several Redis keys and writes
  them after the source state may have changed.

A stale `active_tasks` value can reach the tenant concurrency limit while the
active-task set is empty, preventing queued tasks from dispatching.

## Implementation

### 1. Make incremental updates atomic

Change `update_tenant_limits()` to update only the requested fields:

- Initialize missing configuration fields with `HSETNX`.
- Apply non-zero counter deltas with `HINCRBY`.
- Execute the commands in one transactional pipeline.
- Remove the full-hash `get_tenant_limits()` read and `HSET` rewrite.

This path needs no retry because it does not compute or write a snapshot.

### 2. Protect resynchronization from stale snapshots

Change `resync_tenant_limits()` to use Redis optimistic locking:

1. `WATCH` the tenant active-task set, queue, and limits hash.
2. Recompute active tasks, queued tasks, active documents, and converter units
   from the canonical Redis structures.
3. Start `MULTI` and write only those four derived counters. Initialize missing
   configuration fields with `HSETNX`.
4. Execute the transaction. If a watched key changed, retry from a fresh
   snapshot.

Limit resynchronization to three attempts. After three conflicts, log a warning
and return the current stored limits. The existing reconciliation cycle will
try again later; an unbounded retry loop could stall dispatch for a busy tenant.

### 3. Test against real Redis

Use the Redis service already provided by CI for transaction tests:

- Read `REDIS_URL`, defaulting to `redis://localhost:6379/0`.
- Require a successful `PING` rather than skipping the tests.
- Use unique tenant and task IDs and delete only their exact keys afterward.
- Keep the existing in-memory fake for ordinary counter tests; do not emulate
  `WATCH`, `MULTI`, or `EXEC` in it.

Cover these cases:

- Updating `queued_tasks` does not overwrite `active_tasks`.
- A concurrent finalization invalidates the first resync snapshot; the retry
  stores `active_tasks=0` and admission allows new work.
- Continuous contention stops after three attempts, does not write stale
  counters, logs the deferral, and allows a later uncontended resync to repair
  the hash.

Run:

```bash
.venv/bin/pytest -q \
  tests/test_ray_lifecycle_counters.py \
  tests/test_ray_dispatcher_hardening.py
```

## Acceptance criteria

- Incremental updates never rewrite unrelated limits fields.
- Resync commits only when its watched snapshot remains current.
- Resync retries are bounded and cannot stall the dispatcher indefinitely.
- The focused tests pass against real Redis.
- No new retry setting, repair loop, dependency, or Redis transaction emulator
  is added.

## Out of scope

Redis Cluster requires all transaction keys to share a hash slot. Migrating the
existing tenant key layout is separate work; this change targets standalone
Redis or Valkey and Sentinel deployments.
