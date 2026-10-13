"""Redis state management for Ray orchestrator."""

import asyncio
import datetime
import inspect
import json
import logging
import math
from typing import Optional

import msgpack
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool
from redis.exceptions import WatchError

from docling.datamodel.service.callbacks import CallbackSpec
from docling.datamodel.service.responses import PublicFailureInfo
from docling.datamodel.service.tasks import TaskType

from docling_jobkit.datamodel.result import DoclingTaskResult
from docling_jobkit.datamodel.stored_outcome import (
    StoredFailureOutcome,
    StoredSuccessOutcome,
    StoredTaskOutcome,
    stored_task_outcome_adapter,
)
from docling_jobkit.datamodel.task import Task, validate_task_json
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.ray.models import (
    RedisTaskMetadata,
    TaskCancellationOutcome,
    TaskCancellationResult,
    TaskTerminalizationResult,
    TaskUpdate,
    TenantLimits,
    TenantSnapshot,
    TenantStats,
    TenantTaskCounters,
)
from docling_jobkit.orchestrators.serialization import (
    dump_model_with_secrets,
    make_msgpack_safe,
)

_log = logging.getLogger(__name__)

# Durable statuses a task can never leave. Every status writer treats them as a
# fence so a late started/success/failure update cannot resurrect a task that
# was already terminalized - in particular a cancelled queued task.
_TERMINAL_STATUSES = frozenset(
    {
        TaskStatus.SUCCESS,
        TaskStatus.FAILURE,
        TaskStatus.CANCELLED,
    }
)

_UPDATE_TASK_EXECUTION_HEARTBEAT_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    redis.call('HSET', KEYS[1], 'heartbeat_at', ARGV[1])
    return 1
end
return 0
"""

# Reserve one in-flight converter unit for a task, bounded by the tenant ceiling
# ARGV[1] (max_concurrent_tasks). Units are tracked on both the tenant limits
# hash (KEYS[1]) and the task execution hash (KEYS[2]) so that
# terminalization/reconcile can release a parent's units crash-safely.
# Returns 1 if granted, 0 if the tenant is at its ceiling, or -1 if the task no
# longer has an execution lease (terminalized/reconciled) or its durable status
# is not STARTED (unclaimed PENDING/CANCELLED/terminal). The STARTED check keeps
# converter dispatch fail-closed when the claim never committed, without granting
# (and leaking) a permit. KEYS[2] is task:{id}:execution; the durable status is
# read from the sibling task:{id} hash derived from it so the 2-key call
# signature is unchanged.
_ACQUIRE_CONVERTER_UNIT_LUA = """
if redis.call('EXISTS', KEYS[2]) == 0 then
    return -1
end
local task_key = string.gsub(KEYS[2], ':execution$', '')
if redis.call('HGET', task_key, 'status') ~= 'started' then
    return -1
end
local current = tonumber(redis.call('HGET', KEYS[1], 'converter_units') or '0')
if current >= tonumber(ARGV[1]) then
    return 0
end
redis.call('HINCRBY', KEYS[1], 'converter_units', 1)
redis.call('HINCRBY', KEYS[2], 'converter_units', 1)
return 1
"""

# Release up to ARGV[1] converter units held by a task, clamped to the units the
# task still holds (per its execution hash KEYS[2]) so that explicit per-child
# releases and terminalization cannot double-release. Returns units released.
_RELEASE_CONVERTER_UNITS_LUA = """
local held = tonumber(redis.call('HGET', KEYS[2], 'converter_units') or '0')
local rel = tonumber(ARGV[1])
if rel > held then
    rel = held
end
if rel <= 0 then
    return 0
end
redis.call('HINCRBY', KEYS[1], 'converter_units', -rel)
redis.call('HINCRBY', KEYS[2], 'converter_units', -rel)
return rel
"""

# Atomically mark a task STARTED and bump the per-tenant started counter, but
# only on a genuine transition INTO started (i.e. not when the task is already
# started or already terminal). This guarantees tasks_started_total increments
# exactly once per task even if process_task is retried, and never counts a task
# that reconciliation already terminalized.
# A CANCELLED task counts as terminal here: a cancellation that won must never
# be overwritten by a worker that is only now claiming the task.
# KEYS[1] = task:{id}, KEYS[2] = tenant:{id}:task_counters
# ARGV[1] = "started", ARGV[2] = ISO timestamp
_MARK_TASK_STARTED_LUA = """
local cur = redis.call('HGET', KEYS[1], 'status')
if cur ~= 'started' and cur ~= 'success' and cur ~= 'failure' and cur ~= 'cancelled' then
    redis.call('HSET', KEYS[1], 'status', ARGV[1], 'last_update_at', ARGV[2], 'started_at', ARGV[2])
    redis.call('HINCRBY', KEYS[2], 'tasks_started_total', 1)
    return 1
end
return 0
"""

# Grant leases only for durable STARTED claims so failed claims cannot
# authorize converter dispatch.
# KEYS[1] = task:{id} (status), KEYS[2] = task:{id}:execution (lease)
# ARGV[1..4] = replica_id, tenant_id, claimed_at, heartbeat_at
_WRITE_TASK_EXECUTION_LEASE_LUA = """
if redis.call('HGET', KEYS[1], 'status') ~= 'started' then
    return 0
end
redis.call('HSET', KEYS[2], 'replica_id', ARGV[1], 'tenant_id', ARGV[2], 'claimed_at', ARGV[3], 'heartbeat_at', ARGV[4])
return 1
"""


# Set of every tenant that ever enqueued a task. Tenant discovery reads it
# instead of SCANning the keyspace, whose size grows with the number of task
# hashes rather than with the number of tenants.
TENANT_REGISTRY_KEY = "tenants:known"
# Marker set once the registry has been backfilled from the existing keys.
TENANT_REGISTRY_BACKFILLED_KEY = "tenants:known:backfilled"
# Keys per SCAN round trip when (re)building the registry.
_REGISTRY_SCAN_COUNT = 1000
# Tenant suffixes that identify a tenant when rebuilding the registry.
_REGISTRY_KEY_SUFFIXES = ("tasks", "active_tasks", "task_counters")
# Tenants per pipeline when reading per-tenant values in bulk.
_SNAPSHOT_CHUNK_SIZE = 200


class InvalidQueuedTaskError(Exception):
    """A queued task payload no longer validates against the ``Task`` model.

    Raised by ``peek_task`` so the dispatcher can drop the payload instead of
    retrying it forever and blocking every task queued behind it (e.g. a task
    enqueued by a newer API version with an input format unknown here).
    """

    def __init__(self, tenant_id: str, payload: bytes | str, cause: Exception) -> None:
        self.tenant_id = tenant_id
        self.payload = payload
        self.cause = cause
        # Best effort: the payload is still JSON, only the model rejected it.
        self.task_id: Optional[str] = None
        self.task_size = 1
        try:
            raw = json.loads(payload)
            if isinstance(raw, dict):
                if isinstance(raw.get("task_id"), str):
                    self.task_id = raw["task_id"]
                if isinstance(raw.get("sources"), list) and raw["sources"]:
                    self.task_size = len(raw["sources"])
        except (TypeError, ValueError):
            pass
        super().__init__(
            f"Queued task {self.task_id or '<unknown>'} for tenant {tenant_id} "
            f"is invalid: {cause}"
        )


class RedisStateManager:
    """Manages Redis state for Ray orchestrator.

    Handles all Redis operations including:
    - Per-user task queues
    - Task metadata and status
    - Task results storage
    - User limits and statistics
    - Pub/sub for task updates
    """

    def __init__(
        self,
        redis_url: str,
        results_ttl: int = 3600 * 4,
        task_metadata_ttl: Optional[int] = 3600 * 24 * 7,
        results_prefix: str = "docling:ray:results",
        sub_channel: str = "docling:ray:updates",
        max_connections: int = 50,
        socket_timeout: Optional[float] = None,
        socket_connect_timeout: Optional[float] = None,
        max_concurrent_tasks: int = 5,
        max_queued_tasks: Optional[int] = None,
        max_documents: Optional[int] = None,
        task_timeout: Optional[float] = None,
        dispatcher_interval: float = 2.0,
        log_level: str = "INFO",
        allow_external_plugins: bool = False,
    ):
        """Initialize Redis state manager.

        Args:
            redis_url: Redis connection URL (supports standard, sentinel, cluster)
            results_ttl: Time-to-live for task results in seconds
            task_metadata_ttl: Time-to-live for a finished task's metadata hash
                in seconds, never shorter than results_ttl (None or 0 = keep)
            results_prefix: Prefix for result keys
            sub_channel: Pub/sub channel name for task updates
            max_connections: Maximum Redis connections in pool
            socket_timeout: Socket timeout for Redis operations
            socket_connect_timeout: Socket connect timeout
            max_concurrent_tasks: Max concurrent tasks per user
            max_queued_tasks: Max queued tasks per user (None = unlimited)
            max_documents: Max documents per user (None = unlimited)
            task_timeout: Max expected task runtime in seconds
            dispatcher_interval: Dispatcher loop interval in seconds
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.redis_url = redis_url
        self.results_ttl = results_ttl
        self.task_metadata_ttl = task_metadata_ttl
        self.results_prefix = results_prefix
        self.sub_channel = sub_channel
        self.log_level = log_level
        self.allow_external_plugins = allow_external_plugins

        # Configure logging level for Redis helper
        _log.setLevel(log_level.upper())
        self.max_concurrent_tasks = max_concurrent_tasks
        self.max_queued_tasks = max_queued_tasks
        self.max_documents = max_documents
        self.processing_ttl = self._compute_processing_ttl(task_timeout)
        self.dispatcher_heartbeat_ttl = self._compute_dispatcher_heartbeat_ttl(
            dispatcher_interval
        )

        # Store connection parameters - pool will be created in connect()
        self.max_connections = max_connections
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout

        # Connection pool and client will be created in connect()
        self.pool: Optional[ConnectionPool] = None
        self.redis: Optional[Redis] = None
        self._tenant_registry_ready = False

        # Dedicated pub/sub client, created lazily in subscribe_to_updates().
        # Kept separate from the command pool so the long-lived, mostly-idle
        # subscriber is never subject to the command-path socket_timeout (which
        # would raise on every idle gap and kill the listener). A genuinely dead
        # connection is surfaced by PING health checks instead of a read deadline.
        self._pubsub_redis: Optional[Redis] = None
        self._pubsub_health_check_interval = 15.0
        self._pubsub_poll_timeout = 15.0
        self._pubsub_reconnect_backoff_max = 30.0

    def _compute_processing_ttl(self, task_timeout: Optional[float]) -> int:
        if task_timeout is not None:
            return max(int(task_timeout) + 300, 300)
        return max(self.results_ttl, 7200)

    @staticmethod
    def _compute_dispatcher_heartbeat_ttl(dispatcher_interval: float) -> int:
        return max(math.ceil(dispatcher_interval * 3), 1)

    async def connect(self):
        """Establish Redis connection.

        Creates the connection pool in the current event loop to avoid
        'Future attached to a different loop' errors.
        """
        if self.redis is None:
            # Create connection pool in the current event loop
            self.pool = ConnectionPool.from_url(
                self.redis_url,
                max_connections=self.max_connections,
                socket_timeout=self.socket_timeout,
                socket_connect_timeout=self.socket_connect_timeout,
                decode_responses=False,  # We handle encoding/decoding
            )
            self.redis = Redis(connection_pool=self.pool)
            _log.debug("Connected to Redis at %s", self.redis_url)

    async def disconnect(self):
        """Close Redis connection and pool."""
        if self.redis:
            await self.redis.aclose()
            self.redis = None
        if self.pool:
            await self.pool.aclose()
            self.pool = None
        if self._pubsub_redis:
            await self._pubsub_redis.aclose()
            self._pubsub_redis = None
        _log.debug("Disconnected from Redis")

    def _ensure_redis(self) -> Redis:
        """Ensure redis is connected and return it."""
        if self.redis is None:
            raise RuntimeError("Redis not connected. Call connect() first.")
        return self.redis

    async def ping(self) -> bool:
        """Check Redis connection health."""
        try:
            redis = self._ensure_redis()
            await redis.ping()  # type: ignore[misc]
            return True
        except Exception as e:
            _log.error(f"Redis ping failed: {e}")
        return False

    # Task Queue Operations

    async def enqueue_task(self, tenant_id: str, task: Task) -> None:
        """Add task to tenant's queue.

        Args:
            tenant_id: Tenant identifier
            task: Task to enqueue
        """
        if not self.redis:
            await self.connect()

        queue_key = f"tenant:{tenant_id}:tasks"
        task_counters_key = f"tenant:{tenant_id}:task_counters"
        task_json = json.dumps(dump_model_with_secrets(task, serialize_as_any=True))

        # Push the task, bump the monotonic enqueued counter and register the
        # tenant atomically so the counter can never drift from the queue
        # contents and tenant discovery never misses a queued task.
        redis = self._ensure_redis()
        async with redis.pipeline(transaction=True) as pipe:
            pipe.rpush(queue_key, task_json)
            pipe.hincrby(task_counters_key, "tasks_enqueued_total", 1)
            pipe.sadd(TENANT_REGISTRY_KEY, tenant_id)
            await pipe.execute()

        # Update tenant limits
        await self.update_tenant_limits(tenant_id, delta_queued_tasks=1)

        _log.debug(f"Enqueued task {task.task_id} for tenant {tenant_id}")

    async def dequeue_task(self, tenant_id: str) -> Optional[Task]:
        """Remove and return next task from tenant's queue.

        Args:
            tenant_id: Tenant identifier

        Returns:
            Next task or None if queue is empty
        """
        if not self.redis:
            await self.connect()

        queue_key = f"tenant:{tenant_id}:tasks"
        redis = self._ensure_redis()
        task_json = await redis.lpop(queue_key)  # type: ignore[misc]

        if task_json:
            task = validate_task_json(
                task_json,
                allow_external_plugins=self.allow_external_plugins,
            )
            # Update tenant limits
            await self.update_tenant_limits(tenant_id, delta_queued_tasks=-1)
            _log.debug(f"Dequeued task {task.task_id} for tenant {tenant_id}")
            return task

        return None

    async def peek_task(self, tenant_id: str) -> Optional[Task]:
        """View next task without removing it.

        Args:
            tenant_id: Tenant identifier

        Returns:
            Next task or None if queue is empty

        Raises:
            InvalidQueuedTaskError: If the queued payload fails validation.
        """
        if not self.redis:
            await self.connect()

        queue_key = f"tenant:{tenant_id}:tasks"
        redis = self._ensure_redis()
        task_json = await redis.lindex(queue_key, 0)  # type: ignore[misc]

        if task_json:
            try:
                return validate_task_json(
                    task_json,
                    allow_external_plugins=self.allow_external_plugins,
                )
            except ValidationError as exc:
                raise InvalidQueuedTaskError(tenant_id, task_json, exc) from exc

        return None

    async def remove_queued_task_payload(
        self, tenant_id: str, payload: bytes | str
    ) -> bool:
        """Remove ``payload`` from the front of the tenant queue.

        Only pops when the head is still exactly ``payload``, so a concurrent
        dispatch or enqueue never removes a different task.

        Returns:
            True if the payload was removed, False if the head changed.
        """
        if not self.redis:
            await self.connect()

        queue_key = f"tenant:{tenant_id}:tasks"
        limits_key = f"tenant:{tenant_id}:limits"
        expected = payload.encode("utf-8") if isinstance(payload, str) else payload
        redis = self._ensure_redis()

        async with redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(queue_key)
                    head = await pipe.lindex(queue_key, 0)  # type: ignore[misc]
                    if isinstance(head, str):
                        head = head.encode("utf-8")
                    if head != expected:
                        await pipe.unwatch()
                        return False
                    pipe.multi()
                    pipe.lpop(queue_key)
                    pipe.hincrby(limits_key, "queued_tasks", -1)
                    await pipe.execute()
                    return True
                except WatchError:
                    continue

    async def get_all_tenants_with_tasks(self) -> list[str]:
        """Get list of all tenants with pending tasks.

        Returns:
            List of tenant IDs
        """
        tenants = await self.get_known_tenants()
        sizes = await self._per_tenant_values(tenants, "llen", "tasks")
        return [t for t, size in zip(tenants, sizes) if size > 0]

    # Tenant Registry Operations

    async def get_known_tenants(self) -> list[str]:
        """Get every tenant that ever enqueued a task, from the tenant registry.

        Backfills the registry from the existing keys the first time it is used
        on a Redis instance that predates it.

        Returns:
            Sorted list of tenant IDs
        """
        if not self.redis:
            await self.connect()

        await self._ensure_tenant_registry()
        redis = self._ensure_redis()
        members = await redis.smembers(TENANT_REGISTRY_KEY)  # type: ignore[misc]
        return sorted(m.decode("utf-8") for m in members)

    async def reconcile_tenant_registry(self) -> int:
        """Add every tenant found in the keyspace to the tenant registry.

        This walks the whole keyspace with SCAN, so it only runs once to backfill
        the registry and periodically from the dispatcher as a safety net for
        tenants registered by an older version without the registry.

        Returns:
            Number of tenants found
        """
        if not self.redis:
            await self.connect()

        redis = self._ensure_redis()
        tenants: set[str] = set()
        async for key in redis.scan_iter(  # type: ignore[union-attr]
            match="tenant:*", count=_REGISTRY_SCAN_COUNT
        ):
            parts = key.decode("utf-8").split(":")
            if len(parts) == 3 and parts[2] in _REGISTRY_KEY_SUFFIXES:
                tenants.add(parts[1])
        if tenants:
            await redis.sadd(TENANT_REGISTRY_KEY, *tenants)  # type: ignore[misc]
        _log.debug(f"Reconciled tenant registry with {len(tenants)} tenants")
        return len(tenants)

    async def _ensure_tenant_registry(self) -> None:
        if self._tenant_registry_ready:
            return
        redis = self._ensure_redis()
        if not await redis.exists(TENANT_REGISTRY_BACKFILLED_KEY):
            count = await self.reconcile_tenant_registry()
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            await redis.set(TENANT_REGISTRY_BACKFILLED_KEY, timestamp)
            _log.info(f"Backfilled tenant registry with {count} tenants")
        self._tenant_registry_ready = True

    async def _per_tenant_values(
        self, tenants: list[str], command: str, suffix: str
    ) -> list[int]:
        """Run one single-key command per tenant in a pipeline (one round trip)."""
        if not tenants:
            return []
        redis = self._ensure_redis()
        async with redis.pipeline(transaction=False) as pipe:
            for tenant_id in tenants:
                getattr(pipe, command)(f"tenant:{tenant_id}:{suffix}")
            return [int(v) for v in await pipe.execute()]

    async def get_tenant_queue_size(self, tenant_id: str) -> int:
        """Get number of tasks in tenant's queue.

        Args:
            tenant_id: Tenant identifier

        Returns:
            Number of queued tasks
        """
        if not self.redis:
            await self.connect()

        queue_key = f"tenant:{tenant_id}:tasks"
        redis = self._ensure_redis()
        return await redis.llen(queue_key)  # type: ignore[misc,return-value]

    # Task Metadata Operations

    async def update_task_status(
        self,
        task_id: str,
        status: TaskStatus,
        error_message: Optional[str] = None,
        failure: Optional[PublicFailureInfo] = None,
        progress: Optional[dict] = None,
    ) -> None:
        """Update task status and metadata.

        Reject STARTED regressions and changes to terminal states. Allow
        same-status refreshes and forward transitions while preserving
        committed start and finish timestamps.

        Args:
            task_id: Task identifier
            status: New task status
            error_message: Error message if failed
            progress: Progress metadata
        """
        if not self.redis:
            await self.connect()

        task_key = f"task:{task_id}"
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        updates = {
            "status": status.value,
            "last_update_at": timestamp,
        }

        if error_message:
            updates["error_message"] = error_message
        if failure is not None:
            updates["failure"] = failure.model_dump_json()

        if progress:
            updates["progress"] = json.dumps(progress)

        if status == TaskStatus.STARTED:
            updates["started_at"] = timestamp
        if status in _TERMINAL_STATUSES:
            updates["finished_at"] = timestamp

        redis = self._ensure_redis()
        async with redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(task_key)
                    status_raw = await redis.hget(task_key, "status")  # type: ignore[misc]
                    current = (
                        TaskStatus(status_raw.decode("utf-8")) if status_raw else None
                    )
                    if (
                        current in _TERMINAL_STATUSES
                        and current is not None
                        and current != status
                    ):
                        await pipe.unwatch()
                        _log.debug(
                            f"Preserved terminal status for {task_id}: "
                            f"not overwriting {current.value} with {status.value}"
                        )
                        return
                    if current == TaskStatus.STARTED and status in (
                        TaskStatus.PENDING,
                        TaskStatus.CANCELLED,
                    ):
                        await pipe.unwatch()
                        _log.debug(
                            f"Preserved STARTED claim for {task_id}: "
                            f"not reopening with {status.value}"
                        )
                        return
                    # Preserve claim identity on idempotent refresh: a second
                    # STARTED write must not bump started_at, and a repeated
                    # terminal write must not bump finished_at. Forward
                    # progress (last_update_at/progress/error fields) still
                    # advances on allowed writes.
                    if current == TaskStatus.STARTED and status == TaskStatus.STARTED:
                        updates.pop("started_at", None)
                    if (
                        current is not None
                        and current in _TERMINAL_STATUSES
                        and current == status
                    ):
                        updates.pop("finished_at", None)
                    pipe.multi()
                    pipe.hset(task_key, mapping=updates)  # type: ignore[misc]
                    await pipe.execute()
                    break
                except WatchError:
                    # A claim, cancellation, or finalizer committed first;
                    # re-read and re-decide so the winner is preserved.
                    continue

        _log.debug(f"Updated task {task_id} status to {status}")

    async def mark_task_started(self, task_id: str, tenant_id: str) -> bool:
        """Atomically mark a task STARTED and bump the started counter once.

        Replaces ``update_task_status(STARTED)`` at the processing entry point.
        Uses a Lua script so the status write and the counter increment are atomic
        and the increment only happens on a genuine transition into STARTED
        (idempotent under Ray retries, and a no-op if the task was already
        terminalized by reconciliation or cancelled while still queued).

        Returns:
            True if the task transitioned into STARTED (counter bumped), False if
            it was already started/terminal.
        """
        if not self.redis:
            await self.connect()

        task_key = f"task:{task_id}"
        task_counters_key = f"tenant:{tenant_id}:task_counters"
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        redis = self._ensure_redis()
        result = await redis.eval(  # type: ignore[misc]
            _MARK_TASK_STARTED_LUA,
            2,
            task_key,
            task_counters_key,
            TaskStatus.STARTED.value,
            timestamp,
        )
        _log.debug(f"Marked task {task_id} STARTED (transitioned={bool(result)})")
        return bool(result)

    async def get_task_metadata(self, task_id: str) -> dict:
        """Get task metadata.

        Args:
            task_id: Task identifier

        Returns:
            Task metadata dictionary
        """
        if not self.redis:
            await self.connect()

        task_key = f"task:{task_id}"
        redis = self._ensure_redis()
        metadata = await redis.hgetall(task_key)  # type: ignore[misc]

        # Decode bytes to strings
        return {k.decode("utf-8"): v.decode("utf-8") for k, v in metadata.items()}

    async def set_task_metadata(
        self,
        task_id: str,
        tenant_id: str,
        task_type: TaskType,
        task_size: int,
        status: TaskStatus = TaskStatus.PENDING,
        target_document_version: Optional[str] = None,
    ) -> None:
        """Initialize task metadata.

        Init is idempotent and never reopens a won claim: when the task hash
        already carries a status (PENDING/STARTED/terminal from a prior init,
        claim, or cancellation), the existing durable state is left untouched
        so a retried init cannot regress STARTED back to PENDING.

        Args:
            task_id: Task identifier
            tenant_id: Tenant identifier
            task_type: Type of task
            task_size: Number of documents associated with the task
            status: Initial task status
            target_document_version: DoclingDocument version requested by the client
        """
        if not self.redis:
            await self.connect()

        task_key = f"task:{task_id}"
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        metadata = {
            "task_id": task_id,
            "tenant_id": tenant_id,
            "status": status.value,
            "task_type": task_type.value,
            "task_size": str(task_size),
            "created_at": timestamp,
            "last_update_at": timestamp,
        }
        if target_document_version:
            metadata["target_document_version"] = target_document_version

        redis = self._ensure_redis()
        async with redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(task_key)
                    existing = await redis.hget(task_key, "status")  # type: ignore[misc]
                    if existing is not None:
                        await pipe.unwatch()
                        _log.debug(
                            f"Preserved existing status for {task_id} on init "
                            f"(not overwriting with {status.value})"
                        )
                        return
                    pipe.multi()
                    pipe.hset(task_key, mapping=metadata)  # type: ignore[misc]
                    await pipe.execute()
                    break
                except WatchError:
                    continue

    async def get_task_metadata_model(
        self, task_id: str
    ) -> Optional[RedisTaskMetadata]:
        """Get typed task metadata for durable Ray task recovery."""
        metadata = await self.get_task_metadata(task_id)
        if not metadata:
            return None
        return RedisTaskMetadata.from_redis_mapping(metadata)

    # Task Results Operations

    async def store_task_result(self, task_id: str, result: DoclingTaskResult) -> str:
        """Store task result in Redis.

        Args:
            task_id: Task identifier
            result: Task result to store

        Returns:
            Redis key where result is stored
        """
        if not self.redis:
            await self.connect()

        result_key = f"{self.results_prefix}:task:{task_id}:result"
        result_data = self._serialize_stored_outcome(
            StoredSuccessOutcome(result=result)
        )

        # Store with TTL
        redis = self._ensure_redis()
        await redis.setex(result_key, self.results_ttl, result_data)  # type: ignore[union-attr]

        _log.debug(f"Stored result for task {task_id} with TTL {self.results_ttl}s")

        return result_key

    @staticmethod
    def _serialize_stored_outcome(outcome: StoredTaskOutcome) -> bytes:
        safe_data = make_msgpack_safe(stored_task_outcome_adapter.dump_python(outcome))
        return msgpack.packb(safe_data, use_bin_type=True)

    @staticmethod
    def decode_stored_outcome(raw: bytes) -> StoredTaskOutcome | DoclingTaskResult:
        """Decode Redis result payloads, including legacy raw task-result blobs."""
        payload = msgpack.unpackb(raw, raw=False, strict_map_key=False)
        if isinstance(payload, dict) and payload.get("kind") in {"success", "failure"}:
            return stored_task_outcome_adapter.validate_python(payload)
        return DoclingTaskResult.model_validate(payload)

    async def get_task_outcome(
        self, task_id: str
    ) -> Optional[StoredTaskOutcome | DoclingTaskResult]:
        if not self.redis:
            await self.connect()

        result_key = f"{self.results_prefix}:task:{task_id}:result"
        redis = self._ensure_redis()
        result_data = await redis.get(result_key)  # type: ignore[union-attr]

        if result_data:
            return self.decode_stored_outcome(result_data)

        return None

    async def expire_result(self, result_key: str, ttl: int) -> None:
        """Set TTL on an existing result key.

        Used by on_result_fetched() to implement crash-safe single-use deletion.
        Redis expires the key automatically — no asyncio sleeping needed.

        Args:
            result_key: Full Redis key of the result
            ttl: Seconds until the key expires
        """
        if not self.redis:
            await self.connect()
        redis = self._ensure_redis()
        await redis.expire(result_key, ttl)

    # User Limits Operations

    async def get_tenant_limits(self, tenant_id: str) -> TenantLimits:
        """Get tenant resource limits and current usage.

        Args:
            tenant_id: Tenant identifier

        Returns:
            Tenant limits object
        """
        if not self.redis:
            await self.connect()

        limits_key = f"tenant:{tenant_id}:limits"
        redis = self._ensure_redis()
        limits_data = await redis.hgetall(limits_key)  # type: ignore[misc]
        return self._parse_tenant_limits(limits_data)

    def _parse_tenant_limits(self, limits_data: dict) -> TenantLimits:
        if not limits_data:
            # Return defaults
            return TenantLimits(
                max_concurrent_tasks=self.max_concurrent_tasks,
                max_queued_tasks=self.max_queued_tasks,
                max_documents=self.max_documents,
            )

        # Decode and parse
        limits_dict: dict[str, int | None] = {}
        for k, v in limits_data.items():
            key = k.decode("utf-8")
            value = v.decode("utf-8")

            # Handle None values
            if value == "None":
                limits_dict[key] = None
            else:
                try:
                    limits_dict[key] = int(value)
                except ValueError:
                    limits_dict[key] = value

        return TenantLimits.model_validate(limits_dict)

    async def update_tenant_limits(
        self,
        tenant_id: str,
        delta_active_tasks: int = 0,
        delta_queued_tasks: int = 0,
        delta_docs: int = 0,
    ) -> None:
        """Update tenant resource usage counters.

        Args:
            tenant_id: Tenant identifier
            delta_active_tasks: Change in active tasks count
            delta_queued_tasks: Change in queued tasks count
            delta_docs: Change in active documents count
        """
        if not self.redis:
            await self.connect()

        limits_key = f"tenant:{tenant_id}:limits"
        redis = self._ensure_redis()

        # Change only the requested fields atomically. The old read/modify/write
        # of the whole hash could restore counters changed by another worker.
        async with redis.pipeline(transaction=True) as pipe:
            pipe.hsetnx(
                limits_key, "max_concurrent_tasks", str(self.max_concurrent_tasks)
            )
            pipe.hsetnx(limits_key, "max_queued_tasks", str(self.max_queued_tasks))
            pipe.hsetnx(limits_key, "max_documents", str(self.max_documents))
            if delta_active_tasks:
                pipe.hincrby(limits_key, "active_tasks", delta_active_tasks)
            if delta_queued_tasks:
                pipe.hincrby(limits_key, "queued_tasks", delta_queued_tasks)
            if delta_docs:
                pipe.hincrby(limits_key, "active_documents", delta_docs)
            await pipe.execute()

    async def check_tenant_can_enqueue(
        self, tenant_id: str, task_size: int
    ) -> tuple[bool, str]:
        """Check if tenant can enqueue a new task.

        Args:
            tenant_id: Tenant identifier
            task_size: Number of documents in task

        Returns:
            Tuple of (can_enqueue, reason_if_not)
        """
        limits = await self.get_tenant_limits(tenant_id)

        # Check queue limit
        if limits.max_queued_tasks is not None:
            if limits.queued_tasks >= limits.max_queued_tasks:
                return False, f"Queue limit reached ({limits.max_queued_tasks})"

        return True, ""

    async def check_tenant_can_process(
        self, tenant_id: str, task_size: int
    ) -> tuple[bool, str]:
        """Check if tenant can start processing a task.

        Args:
            tenant_id: Tenant identifier
            task_size: Number of documents in task

        Returns:
            Tuple of (can_process, reason_if_not)
        """
        limits = await self.get_tenant_limits(tenant_id)

        _log.debug(
            f"[CAPACITY-CHECK] {tenant_id}: "
            f"active_tasks={limits.active_tasks}/{limits.max_concurrent_tasks}, "
            f"active_docs={limits.active_documents}/{limits.max_documents or 'unlimited'}, "
            f"task_size={task_size}"
        )

        # Check concurrent task limit
        if limits.active_tasks >= limits.max_concurrent_tasks:
            reason = f"Concurrent task limit reached ({limits.max_concurrent_tasks})"
            _log.debug(f"[CAPACITY-CHECK] {tenant_id}: BLOCKED - {reason}")
            return False, reason

        # Check document limit if enabled
        if limits.max_documents is not None:
            if limits.active_documents + task_size > limits.max_documents:
                reason = f"Document limit would be exceeded ({limits.max_documents})"
                _log.debug(f"[CAPACITY-CHECK] {tenant_id}: BLOCKED - {reason}")
                return False, reason

        _log.debug(f"[CAPACITY-CHECK] {tenant_id}: ALLOWED")
        return True, ""

    # User Statistics Operations

    async def update_tenant_stats(
        self,
        tenant_id: str,
        delta_total_tasks: int = 0,
        delta_total_documents: int = 0,
        delta_successful_documents: int = 0,
        delta_failed_documents: int = 0,
    ) -> None:
        """Update tenant statistics.

        Args:
            tenant_id: Tenant identifier
            delta_total_tasks: Change in total tasks count
            delta_total_documents: Change in total documents count
            delta_successful_documents: Change in successful documents count
            delta_failed_documents: Change in failed documents count
        """
        if not self.redis:
            await self.connect()

        stats_key = f"tenant:{tenant_id}:stats"
        redis = self._ensure_redis()

        # Use HINCRBY for atomic increments
        if delta_total_tasks != 0:
            await redis.hincrby(stats_key, "total_tasks", delta_total_tasks)  # type: ignore[misc]
        if delta_total_documents != 0:
            await redis.hincrby(  # type: ignore[misc]
                stats_key, "total_documents", delta_total_documents
            )
        if delta_successful_documents != 0:
            await redis.hincrby(  # type: ignore[misc]
                stats_key, "successful_documents", delta_successful_documents
            )
        if delta_failed_documents != 0:
            await redis.hincrby(  # type: ignore[misc]
                stats_key, "failed_documents", delta_failed_documents
            )

    async def get_tenant_stats(self, tenant_id: str) -> TenantStats:
        """Get tenant statistics.

        Args:
            tenant_id: Tenant identifier

        Returns:
            Tenant statistics object
        """
        if not self.redis:
            await self.connect()

        stats_key = f"tenant:{tenant_id}:stats"
        redis = self._ensure_redis()
        stats_data = await redis.hgetall(stats_key)  # type: ignore[misc]
        return self._parse_tenant_stats(stats_data)

    @staticmethod
    def _parse_tenant_stats(stats_data: dict) -> TenantStats:
        if not stats_data:
            return TenantStats()

        # Decode and parse
        stats_dict = {}
        for k, v in stats_data.items():
            key = k.decode("utf-8")
            value = int(v.decode("utf-8"))
            stats_dict[key] = value

        return TenantStats.model_validate(stats_dict)

    async def get_tenant_task_counters(self, tenant_id: str) -> TenantTaskCounters:
        """Get the monotonic lifecycle counters for a tenant.

        Args:
            tenant_id: Tenant identifier

        Returns:
            Tenant counters object (all-zero if the tenant has no counters yet)
        """
        if not self.redis:
            await self.connect()

        task_counters_key = f"tenant:{tenant_id}:task_counters"
        redis = self._ensure_redis()
        task_counters_data = await redis.hgetall(task_counters_key)  # type: ignore[misc]
        return self._parse_tenant_task_counters(task_counters_data)

    @staticmethod
    def _parse_tenant_task_counters(task_counters_data: dict) -> TenantTaskCounters:
        if not task_counters_data:
            return TenantTaskCounters()

        # Decode bytes; pydantic coerces the string values to int and ignores any
        # unexpected fields.
        decoded = {
            k.decode("utf-8"): v.decode("utf-8") for k, v in task_counters_data.items()
        }
        return TenantTaskCounters.model_validate(decoded)

    async def get_tenant_snapshots(self, tenants: list[str]) -> list[TenantSnapshot]:
        """Read counters, stats, queue size, active count and limits of many tenants.

        All reads go through non-transactional pipelines, one round trip per
        chunk of tenants, instead of five round trips per tenant.

        Args:
            tenants: Tenant identifiers

        Returns:
            One snapshot per tenant, in the same order
        """
        if not self.redis:
            await self.connect()

        redis = self._ensure_redis()
        snapshots: list[TenantSnapshot] = []
        for start in range(0, len(tenants), _SNAPSHOT_CHUNK_SIZE):
            chunk = tenants[start : start + _SNAPSHOT_CHUNK_SIZE]
            async with redis.pipeline(transaction=False) as pipe:
                for tenant_id in chunk:
                    pipe.hgetall(f"tenant:{tenant_id}:task_counters")
                    pipe.hgetall(f"tenant:{tenant_id}:stats")
                    pipe.llen(f"tenant:{tenant_id}:tasks")
                    pipe.scard(f"tenant:{tenant_id}:active_tasks")
                    pipe.hgetall(f"tenant:{tenant_id}:limits")
                values = await pipe.execute()
            for i, tenant_id in enumerate(chunk):
                counters, stats, queue_size, active_count, limits = values[
                    i * 5 : i * 5 + 5
                ]
                snapshots.append(
                    TenantSnapshot(
                        tenant_id=tenant_id,
                        counters=self._parse_tenant_task_counters(counters),
                        stats=self._parse_tenant_stats(stats),
                        queue_size=int(queue_size),
                        active_count=int(active_count),
                        limits=self._parse_tenant_limits(limits),
                    )
                )
        return snapshots

    # Atomic Task Dispatch Operations

    async def dispatch_task_atomic(
        self, tenant_id: str, task_id: str, task_size: int
    ) -> bool:
        """Atomically dispatch a task: pop from queue, add to active set, update limits.

        Uses Redis transaction (MULTI/EXEC) for atomicity to prevent race conditions.
        The task's durable status is watched as well, so a queued-only
        cancellation that commits first aborts the dispatch instead of handing a
        cancelled task to a replica. A cancelled entry that is already at the
        front is dropped here (returning False) so it cannot block the tasks
        queued behind it.

        Args:
            tenant_id: Tenant identifier
            task_id: Task identifier
            task_size: Number of documents in task

        Returns:
            True if successful, False if failed (e.g., race condition)
        """
        queue_key = f"tenant:{tenant_id}:tasks"
        active_key = f"tenant:{tenant_id}:active_tasks"
        limits_key = f"tenant:{tenant_id}:limits"
        task_counters_key = f"tenant:{tenant_id}:task_counters"
        dispatch_key = f"task:{task_id}:dispatch"
        task_key = f"task:{task_id}"

        redis = self._ensure_redis()

        # Use pipeline for atomic operations
        async with redis.pipeline(transaction=True) as pipe:
            try:
                # Watch keys for changes
                await pipe.watch(queue_key, active_key, limits_key, task_key)

                # Check if task is still at front of queue
                front_task_json = await redis.lindex(queue_key, 0)  # type: ignore[misc]
                if not front_task_json:
                    await pipe.unwatch()
                    return False

                front_task = validate_task_json(
                    front_task_json,
                    allow_external_plugins=self.allow_external_plugins,
                )
                if front_task.task_id != task_id:
                    # Race condition: someone else popped it
                    await pipe.unwatch()
                    _log.debug(
                        f"[REDIS-ATOMIC] Race condition: task {task_id} not at front"
                    )
                    return False

                status_raw = await redis.hget(task_key, "status")  # type: ignore[misc]
                front_status = (
                    TaskStatus(status_raw.decode("utf-8")) if status_raw else None
                )
                if front_status == TaskStatus.CANCELLED:
                    # Cancelled while queued: drop the stale payload so the queue
                    # keeps moving, and leave the durable terminal state alone.
                    pipe.multi()
                    pipe.lpop(queue_key)
                    pipe.hincrby(limits_key, "queued_tasks", -1)
                    await pipe.execute()
                    _log.info(
                        f"[REDIS-ATOMIC] Dropped cancelled task {task_id} "
                        f"from tenant {tenant_id} queue"
                    )
                    return False

                # Start transaction
                pipe.multi()

                # 1. Pop task from queue
                pipe.lpop(queue_key)

                # 2. Add to active set
                pipe.sadd(active_key, task_id)

                # 3. Update limits
                pipe.hincrby(limits_key, "active_tasks", 1)
                pipe.hincrby(limits_key, "queued_tasks", -1)
                if self.max_documents is not None:
                    pipe.hincrby(limits_key, "active_documents", task_size)

                # 4. Create processing state
                now_timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()
                dispatch_state = {
                    "tenant_id": tenant_id,
                    "dispatched_at": str(now_timestamp),
                    "task_size": str(task_size),
                }
                if front_task.callbacks:
                    # The task payload leaves the queue here, so this hash is the
                    # last place the callback specs survive. Reconciliation needs
                    # them to notify a client whose replica died without ever
                    # reporting. Kept out of the long-lived task:{id} metadata
                    # hash on purpose: callback headers can carry credentials, and
                    # this key is TTL-bounded and deleted at terminalization.
                    dispatch_state["callbacks"] = json.dumps(
                        [
                            dump_model_with_secrets(callback)
                            for callback in front_task.callbacks
                        ]
                    )
                pipe.hset(dispatch_key, mapping=dispatch_state)
                pipe.expire(dispatch_key, self.processing_ttl)

                # 5. Bump the monotonic dispatched counter (atomic with the
                # pop/add). On a raced dispatch we return before reaching
                # pipe.execute(), so this is never committed.
                pipe.hincrby(task_counters_key, "tasks_dispatched_total", 1)

                # Execute transaction
                await pipe.execute()

                _log.debug(
                    f"[REDIS-ATOMIC] Dispatched task {task_id} for tenant {tenant_id}"
                )
                return True

            except Exception as e:
                # Transaction failed due to concurrent modification or other error
                _log.debug(f"[REDIS-ATOMIC] Failed to dispatch task {task_id}: {e}")
                return False

    async def finalize_task_success_atomic(
        self,
        tenant_id: str,
        task_id: str,
        task_size: int,
        result: DoclingTaskResult,
    ) -> TaskTerminalizationResult:
        """Durably finalize a task to SUCCESS exactly once."""
        result_key = f"{self.results_prefix}:task:{task_id}:result"
        result_data = self._serialize_stored_outcome(
            StoredSuccessOutcome(result=result)
        )
        return await self._finalize_task_terminal_state_atomic(
            tenant_id=tenant_id,
            task_id=task_id,
            task_size=task_size,
            terminal_status=TaskStatus.SUCCESS,
            result_key=result_key,
            result_data=result_data,
        )

    async def finalize_task_failure_atomic(
        self,
        tenant_id: str,
        task_id: str,
        task_size: int,
        error_message: str,
        failure: PublicFailureInfo,
    ) -> TaskTerminalizationResult:
        """Durably finalize a task to FAILURE exactly once."""
        result_key = f"{self.results_prefix}:task:{task_id}:result"
        result_data = self._serialize_stored_outcome(
            StoredFailureOutcome(failure=failure)
        )
        return await self._finalize_task_terminal_state_atomic(
            tenant_id=tenant_id,
            task_id=task_id,
            task_size=task_size,
            terminal_status=TaskStatus.FAILURE,
            error_message=error_message,
            failure=failure,
            result_key=result_key,
            result_data=result_data,
        )

    async def _finalize_task_terminal_state_atomic(
        self,
        tenant_id: str,
        task_id: str,
        task_size: int,
        terminal_status: TaskStatus,
        error_message: Optional[str] = None,
        failure: Optional[PublicFailureInfo] = None,
        result_key: Optional[str] = None,
        result_data: Optional[bytes] = None,
    ) -> TaskTerminalizationResult:
        """Finalize metadata + cleanup atomically while preserving the first terminal state."""
        if not self.redis:
            await self.connect()

        task_key = f"task:{task_id}"
        active_key = f"tenant:{tenant_id}:active_tasks"
        limits_key = f"tenant:{tenant_id}:limits"
        task_counters_key = f"tenant:{tenant_id}:task_counters"
        dispatch_key = f"task:{task_id}:dispatch"
        execution_key = f"task:{task_id}:execution"
        redis = self._ensure_redis()

        async with redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(task_key, active_key, limits_key)

                    status_raw = await redis.hget(task_key, "status")  # type: ignore[misc]
                    current_status = (
                        TaskStatus(status_raw.decode("utf-8")) if status_raw else None
                    )

                    # Release any converter units the parent still holds. The held
                    # count lives on the execution hash (deleted below in the same
                    # transaction), so a second finalize reads 0 — exactly-once.
                    held_units_raw = await redis.hget(  # type: ignore[misc]
                        execution_key, "converter_units"
                    )
                    held_units = (
                        int(held_units_raw.decode("utf-8")) if held_units_raw else 0
                    )

                    sismember_result = pipe.sismember(active_key, task_id)
                    if inspect.isawaitable(sismember_result):
                        was_active = bool(await sismember_result)
                    else:
                        was_active = bool(sismember_result)

                    pipe.multi()

                    if (
                        result_key is not None
                        and result_data is not None
                        and current_status not in _TERMINAL_STATUSES
                    ):
                        pipe.setex(result_key, self.results_ttl, result_data)

                    if current_status not in _TERMINAL_STATUSES:
                        timestamp = datetime.datetime.now(
                            datetime.timezone.utc
                        ).isoformat()
                        updates = {
                            "status": terminal_status.value,
                            "last_update_at": timestamp,
                            "finished_at": timestamp,
                        }
                        if (
                            terminal_status == TaskStatus.FAILURE
                            and error_message is not None
                        ):
                            updates["error_message"] = error_message
                        if (
                            terminal_status == TaskStatus.FAILURE
                            and failure is not None
                        ):
                            updates["failure"] = failure.model_dump_json()
                        pipe.hset(task_key, mapping=updates)

                        # Bump the monotonic terminal counter exactly once: this
                        # block only runs on the first transition into a terminal
                        # state (the status_changed guard), so duplicate finalizes
                        # from replica + dispatcher + reconciliation never
                        # double-count.
                        if terminal_status == TaskStatus.SUCCESS:
                            pipe.hincrby(task_counters_key, "tasks_succeeded_total", 1)
                        elif terminal_status == TaskStatus.FAILURE:
                            pipe.hincrby(task_counters_key, "tasks_failed_total", 1)

                    if terminal_status == TaskStatus.SUCCESS:
                        pipe.hdel(task_key, "error_message", "failure")
                    if held_units > 0:
                        pipe.hincrby(limits_key, "converter_units", -held_units)
                    pipe.delete(dispatch_key, execution_key)
                    # Finished task hashes would otherwise pile up forever.
                    if self.task_metadata_ttl:
                        pipe.expire(
                            task_key, max(self.task_metadata_ttl, self.results_ttl)
                        )

                    if was_active:
                        pipe.srem(active_key, task_id)
                        pipe.hincrby(limits_key, "active_tasks", -1)
                        if self.max_documents is not None:
                            pipe.hincrby(limits_key, "active_documents", -task_size)

                    await pipe.execute()

                    status_changed = current_status not in _TERMINAL_STATUSES
                    final_status = (
                        terminal_status
                        if status_changed or current_status is None
                        else current_status
                    )
                    return TaskTerminalizationResult(
                        final_status=final_status,
                        status_changed=status_changed,
                        capacity_released=was_active,
                        result_key=(
                            result_key
                            if terminal_status == TaskStatus.SUCCESS
                            and final_status == TaskStatus.SUCCESS
                            else None
                        ),
                    )
                except WatchError:
                    continue

    async def cancel_queued_task_atomic(self, task_id: str) -> TaskCancellationResult:
        """Atomically terminalize a queued task as CANCELLED.

        This is the queued-only cancellation primitive and it competes with the
        worker's PENDING -> STARTED claim (mark_task_started) on the very same
        durable status field, so exactly one of them wins:

        * cancellation wins: the task becomes CANCELLED, its queue entry and any
          dispatcher reservation are released in the same transaction, and the
          execution lease is deleted so a replica can no longer acquire a
          converter unit for it;
        * the claim wins: the status is no longer PENDING and the caller gets a
          conflict, leaving the task untouched.

        Args:
            task_id: Task identifier

        Returns:
            A TaskCancellationResult describing the outcome.
        """
        if not self.redis:
            await self.connect()

        redis = self._ensure_redis()
        task_key = f"task:{task_id}"

        tenant_raw = await redis.hget(task_key, "tenant_id")  # type: ignore[misc]
        if tenant_raw is None:
            return TaskCancellationResult(outcome=TaskCancellationOutcome.NOT_FOUND)
        tenant_id = tenant_raw.decode("utf-8")

        queue_key = f"tenant:{tenant_id}:tasks"
        active_key = f"tenant:{tenant_id}:active_tasks"
        limits_key = f"tenant:{tenant_id}:limits"
        task_counters_key = f"tenant:{tenant_id}:task_counters"
        dispatch_key = f"task:{task_id}:dispatch"
        execution_key = f"task:{task_id}:execution"

        async with redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(task_key, queue_key, active_key, limits_key)

                    status_raw = await redis.hget(task_key, "status")  # type: ignore[misc]
                    if status_raw is None:
                        await pipe.unwatch()
                        return TaskCancellationResult(
                            outcome=TaskCancellationOutcome.NOT_FOUND
                        )
                    current_status = TaskStatus(status_raw.decode("utf-8"))

                    if current_status == TaskStatus.CANCELLED:
                        await pipe.unwatch()
                        return TaskCancellationResult(
                            outcome=TaskCancellationOutcome.ALREADY_CANCELLED,
                            status=current_status,
                        )
                    if current_status != TaskStatus.PENDING:
                        await pipe.unwatch()
                        return TaskCancellationResult(
                            outcome=TaskCancellationOutcome.CONFLICT,
                            status=current_status,
                        )

                    payload = await self._find_queued_task_payload(tenant_id, task_id)
                    was_active = bool(
                        await redis.sismember(active_key, task_id)  # type: ignore[misc]
                    )
                    task_size_raw = await redis.hget(task_key, "task_size")  # type: ignore[misc]
                    task_size = (
                        int(task_size_raw.decode("utf-8")) if task_size_raw else 0
                    )
                    held_units_raw = await redis.hget(  # type: ignore[misc]
                        execution_key, "converter_units"
                    )
                    held_units = (
                        int(held_units_raw.decode("utf-8")) if held_units_raw else 0
                    )

                    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

                    pipe.multi()
                    pipe.hset(
                        task_key,
                        mapping={
                            "status": TaskStatus.CANCELLED.value,
                            "finished_at": timestamp,
                            "last_update_at": timestamp,
                        },
                    )
                    if payload is not None:
                        pipe.lrem(queue_key, 1, payload)
                        pipe.hincrby(limits_key, "queued_tasks", -1)
                    if was_active:
                        pipe.srem(active_key, task_id)
                        pipe.hincrby(limits_key, "active_tasks", -1)
                        if self.max_documents is not None:
                            pipe.hincrby(
                                limits_key, "active_documents", -max(task_size, 1)
                            )
                    if held_units > 0:
                        pipe.hincrby(limits_key, "converter_units", -held_units)
                    pipe.delete(dispatch_key, execution_key)
                    pipe.hincrby(task_counters_key, "tasks_cancelled_total", 1)
                    await pipe.execute()

                    _log.info(
                        "Cancelled queued task %s for tenant %s "
                        "(queue_entry_removed=%s, reservation_released=%s)",
                        task_id,
                        tenant_id,
                        payload is not None,
                        was_active,
                    )
                    return TaskCancellationResult(
                        outcome=TaskCancellationOutcome.CANCELLED,
                        status=TaskStatus.CANCELLED,
                        queue_entry_removed=payload is not None,
                        reservation_released=was_active,
                    )
                except WatchError:
                    # A dispatch, a claim, or another cancellation committed in
                    # between; re-read the durable state and decide again.
                    continue

    async def _find_queued_task_payload(
        self, tenant_id: str, task_id: str
    ) -> Optional[str]:
        """Return the exact queued payload of a task, or None if it is not queued.

        The payload is immutable, so the value found here stays valid for the
        LREM that the cancellation transaction issues under the same WATCH.
        """
        queue_key = f"tenant:{tenant_id}:tasks"
        redis = self._ensure_redis()
        for raw_entry in await redis.lrange(queue_key, 0, -1):  # type: ignore[misc]
            try:
                entry = raw_entry.decode("utf-8")
                if json.loads(entry).get("task_id") == task_id:
                    return entry
            except (UnicodeDecodeError, ValueError, AttributeError):
                continue
        return None

    async def get_tenant_active_task_count(self, tenant_id: str) -> int:
        """Get number of active tasks for tenant from Redis set.

        This is the source of truth, not the counter in limits.

        Args:
            tenant_id: Tenant identifier

        Returns:
            Number of active tasks
        """
        active_key = f"tenant:{tenant_id}:active_tasks"
        redis = self._ensure_redis()
        count = await redis.scard(active_key)  # type: ignore[misc]
        return int(count)

    async def get_tenant_active_task_ids(self, tenant_id: str) -> list[str]:
        """Get list of active task IDs for tenant.

        Args:
            tenant_id: Tenant identifier

        Returns:
            List of task IDs
        """
        active_key = f"tenant:{tenant_id}:active_tasks"
        redis = self._ensure_redis()
        task_ids = await redis.smembers(active_key)  # type: ignore[misc]
        return [tid.decode("utf-8") for tid in task_ids]

    async def get_user_active_task_ids(self, user_id: str) -> list[str]:
        """Backward-compatible alias for the tenant-based helper."""
        return await self.get_tenant_active_task_ids(user_id)

    async def get_all_tenants_with_active_tasks(self) -> list[str]:
        """Get list of all tenants with active tasks.

        Returns:
            List of tenant IDs
        """
        tenants = await self.get_known_tenants()
        counts = await self._per_tenant_values(tenants, "scard", "active_tasks")
        return [t for t, count in zip(tenants, counts) if count > 0]

    async def get_all_tenants_with_any_tasks(self) -> list[str]:
        """Get list of all tenants with pending OR active tasks.

        This combines tenants from both queued tasks and active tasks,
        providing complete visibility for metrics and monitoring.

        Returns:
            List of unique tenant IDs with any tasks (queued or active)
        """
        tenants = await self.get_known_tenants()
        sizes = await self._per_tenant_values(tenants, "llen", "tasks")
        counts = await self._per_tenant_values(tenants, "scard", "active_tasks")
        return [t for t, size, count in zip(tenants, sizes, counts) if size or count]

    async def get_all_tenants_with_task_counters(self) -> list[str]:
        """Get list of all tenants that have lifecycle counters.

        Counters persist after a tenant's queue and active set drain, so this is
        needed (in addition to get_all_tenants_with_any_tasks) to keep scraping a
        tenant's cumulative counters even when it is currently idle. A counter
        that disappears and later reappears would look like a reset to Prometheus
        and corrupt rate()/increase().

        Returns:
            List of tenant IDs that have a counters hash
        """
        tenants = await self.get_known_tenants()
        exists = await self._per_tenant_values(tenants, "exists", "task_counters")
        return [t for t, found in zip(tenants, exists) if found]

    async def _get_task_size_for_resync(self, task_id: str) -> int:
        metadata = await self.get_task_metadata_model(task_id)
        if metadata is not None and metadata.task_size > 0:
            return metadata.task_size

        _log.warning(
            "[REDIS-RESYNC] Missing durable task_size for task %s; falling back to 1",
            task_id,
        )
        return 1

    async def resync_tenant_limits(self, tenant_id: str) -> TenantLimits:
        """Resynchronize tenant counters from canonical Redis structures."""
        limits_key = f"tenant:{tenant_id}:limits"
        active_key = f"tenant:{tenant_id}:active_tasks"
        queue_key = f"tenant:{tenant_id}:tasks"
        redis = self._ensure_redis()

        # Unlike the atomic increments above, resync derives one snapshot from
        # several keys. WATCH rejects a stale snapshot; three attempts absorb a
        # transient conflict without letting a hot tenant stall reconciliation.
        for _ in range(3):
            async with redis.pipeline(transaction=True) as pipe:
                try:
                    await pipe.watch(active_key, queue_key, limits_key)

                    active_task_ids = await self.get_tenant_active_task_ids(tenant_id)
                    active_documents = 0
                    converter_units = 0
                    for task_id in active_task_ids:
                        active_documents += await self._get_task_size_for_resync(
                            task_id
                        )
                        lease = await self.get_task_execution_lease(task_id)
                        if lease is not None:
                            converter_units += int(lease.get("converter_units") or 0)

                    limits = await self.get_tenant_limits(tenant_id)
                    limits.active_tasks = len(active_task_ids)
                    limits.queued_tasks = await self.get_tenant_queue_size(tenant_id)
                    limits.active_documents = active_documents
                    limits.converter_units = converter_units

                    pipe.multi()
                    pipe.hsetnx(
                        limits_key,
                        "max_concurrent_tasks",
                        str(self.max_concurrent_tasks),
                    )
                    pipe.hsetnx(
                        limits_key, "max_queued_tasks", str(self.max_queued_tasks)
                    )
                    pipe.hsetnx(limits_key, "max_documents", str(self.max_documents))
                    pipe.hset(
                        limits_key,
                        mapping={
                            "active_tasks": str(limits.active_tasks),
                            "queued_tasks": str(limits.queued_tasks),
                            "active_documents": str(limits.active_documents),
                            "converter_units": str(limits.converter_units),
                        },
                    )
                    await pipe.execute()
                except WatchError:
                    continue
                return limits

        _log.warning(
            "Tenant limits resync deferred after repeated contention: %s", tenant_id
        )
        return await self.get_tenant_limits(tenant_id)

    async def get_task_dispatch_hash(self, task_id: str) -> dict:
        """Get the dispatcher-owned dispatch state hash for a task.

        Args:
            task_id: Task identifier

        Returns:
            Dispatch state dictionary containing tenant_id, dispatched_at, and task_size
        """
        dispatch_key = f"task:{task_id}:dispatch"
        redis = self._ensure_redis()
        state = await redis.hgetall(dispatch_key)  # type: ignore[misc]
        if not state:
            return {}
        return {k.decode("utf-8"): v.decode("utf-8") for k, v in state.items()}

    @staticmethod
    def decode_dispatch_callbacks(dispatch_hash: dict) -> list[CallbackSpec]:
        """Read the callback specs persisted by dispatch_task_atomic.

        Returns an empty list for tasks without callbacks, for dispatch hashes
        written before this field existed, and for unparseable payloads — a
        client notification is best-effort and must never block reconciliation.
        """
        raw = dispatch_hash.get("callbacks")
        if not raw:
            return []
        try:
            return [CallbackSpec.model_validate(entry) for entry in json.loads(raw)]
        except (ValueError, ValidationError) as exc:
            _log.warning("Could not decode persisted callbacks: %s", exc)
            return []

    async def write_task_execution_lease(
        self, task_id: str, tenant_id: str, replica_id: str
    ) -> bool:
        """Write the replica-owned execution lease for a task.

        Called by the replica when it begins executing a task. The lease proves
        a live replica owns this work. It is refreshed by
        update_task_execution_heartbeat() and deleted by finalize_task_*_atomic().
        No TTL — the key is cleaned up explicitly at terminalization.

        Returns False without writing unless the durable task status is already
        STARTED (authoritative committed claim). The lease is what authorizes
        converter units, so a PENDING task (including a transient
        mark_task_started failure with no durable claim) or a cancelled/terminal
        task must never get one; its conversion is therefore skipped by the
        replica via the missing-lease (-1) converter-unit path.
        """
        if not self.redis:
            await self.connect()

        task_key = f"task:{task_id}"
        execution_key = f"task:{task_id}:execution"
        redis = self._ensure_redis()
        now_timestamp = str(datetime.datetime.now(datetime.timezone.utc).timestamp())

        granted = await redis.register_script(_WRITE_TASK_EXECUTION_LEASE_LUA)(
            keys=[task_key, execution_key],
            args=[replica_id, tenant_id, now_timestamp, now_timestamp],
        )
        if not granted:
            _log.info(
                "Not writing execution lease for unclaimed task %s "
                "(durable status is not STARTED)",
                task_id,
            )
        return bool(granted)

    async def update_task_execution_heartbeat(self, task_id: str) -> bool:
        """Refresh the execution lease heartbeat timestamp.

        Uses a Lua script so the update is a no-op when the key is gone
        (replica died or task was already terminalized). Returns False when
        the key no longer exists — the caller should stop heartbeating.
        """
        if not self.redis:
            await self.connect()

        execution_key = f"task:{task_id}:execution"
        redis = self._ensure_redis()
        now_timestamp = str(datetime.datetime.now(datetime.timezone.utc).timestamp())

        # register_script caches the SHA and transparently reloads on NOSCRIPT.
        updated = await redis.register_script(_UPDATE_TASK_EXECUTION_HEARTBEAT_LUA)(
            keys=[execution_key], args=[now_timestamp]
        )
        return bool(updated)

    async def acquire_converter_unit(
        self, tenant_id: str, task_id: str, ceiling: int
    ) -> int:
        """Atomically reserve one in-flight converter unit for a task.

        Units are bounded by the tenant ceiling (``max_concurrent_tasks``) and
        tracked on the task's execution lease so terminalization/reconcile can
        release them crash-safely. Returns 1 if granted, 0 if the tenant is at
        its ceiling, or -1 if the task no longer has an execution lease
        (terminalized/reconciled) or its durable status is not STARTED
        (unclaimed PENDING/CANCELLED/terminal, including a transient claim
        failure with no committed arbitration) — the caller should stop
        launching children. No permit is granted (or leaked) on the -1 path.
        """
        if not self.redis:
            await self.connect()

        limits_key = f"tenant:{tenant_id}:limits"
        execution_key = f"task:{task_id}:execution"
        redis = self._ensure_redis()

        granted = await redis.register_script(_ACQUIRE_CONVERTER_UNIT_LUA)(
            keys=[limits_key, execution_key], args=[ceiling]
        )
        return int(granted)

    async def release_converter_units(
        self, tenant_id: str, task_id: str, count: int
    ) -> int:
        """Release up to ``count`` converter units held by a task.

        Clamped to the units the task still holds (per its execution hash), so
        explicit per-child releases and terminalization cannot double-release.
        Returns the number of units actually released.
        """
        if count <= 0:
            return 0
        if not self.redis:
            await self.connect()

        limits_key = f"tenant:{tenant_id}:limits"
        execution_key = f"task:{task_id}:execution"
        redis = self._ensure_redis()

        released = await redis.register_script(_RELEASE_CONVERTER_UNITS_LUA)(
            keys=[limits_key, execution_key], args=[count]
        )
        return int(released)

    async def get_task_execution_lease(self, task_id: str) -> Optional[dict]:
        """Return the replica-owned execution lease, or None if it does not exist.

        Used by reconciliation to determine whether a STARTED task still has
        a live replica owner.
        """
        if not self.redis:
            await self.connect()
        execution_key = f"task:{task_id}:execution"
        redis = self._ensure_redis()
        state = await redis.hgetall(execution_key)  # type: ignore[misc]
        if not state:
            return None
        return {k.decode("utf-8"): v.decode("utf-8") for k, v in state.items()}

    # Dispatcher Heartbeat Operations

    async def update_dispatcher_heartbeat(self) -> None:
        """Update dispatcher heartbeat timestamp."""
        heartbeat_key = "dispatcher:heartbeat"
        redis = self._ensure_redis()
        timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()
        await redis.setex(  # type: ignore[union-attr]
            heartbeat_key, self.dispatcher_heartbeat_ttl, str(timestamp)
        )

    async def get_dispatcher_heartbeat_age(self) -> float:
        """Get age of dispatcher heartbeat in seconds.

        Returns:
            Age in seconds, or infinity if no heartbeat exists
        """
        import datetime

        heartbeat_key = "dispatcher:heartbeat"
        redis = self._ensure_redis()
        timestamp_str = await redis.get(heartbeat_key)  # type: ignore[union-attr]

        if not timestamp_str:
            return float("inf")

        timestamp = float(timestamp_str.decode("utf-8"))
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        return now - timestamp

    # Pub/Sub Operations

    async def publish_update(self, update: TaskUpdate) -> None:
        """Publish task update to pub/sub channel.

        Args:
            update: Task update message
        """
        if not self.redis:
            await self.connect()

        update_json = update.model_dump_json()
        redis = self._ensure_redis()
        await redis.publish(self.sub_channel, update_json)  # type: ignore[union-attr]

        _log.debug(f"Published update for task {update.task_id}")

    def _build_pubsub_redis(self) -> Redis:
        """Build the dedicated pub/sub client.

        Unlike the command pool, this connection has no read ``socket_timeout``:
        a subscriber blocks waiting for the next published message, so an idle
        gap is expected, not a failure. Liveness of an otherwise-idle connection
        is detected via PING ``health_check_interval`` plus TCP keepalive, so a
        truly dead Redis still surfaces promptly without conflating "quiet" with
        "broken".
        """
        return Redis.from_url(
            self.redis_url,
            socket_timeout=None,
            socket_connect_timeout=self.socket_connect_timeout,
            socket_keepalive=True,
            health_check_interval=self._pubsub_health_check_interval,
            decode_responses=False,
        )

    async def subscribe_to_updates(self):
        """Yield task updates from the pub/sub channel.

        Resilient by design: reconnects with exponential backoff if the
        subscription drops, skips malformed messages, and treats idle gaps as
        normal rather than fatal. The generator only ends on cancellation, so a
        transient Redis hiccup can never silently kill WebSocket push delivery.

        Yields:
            TaskUpdate messages as they are published.
        """
        backoff = 1.0
        while True:
            pubsub = None
            try:
                if self._pubsub_redis is None:
                    self._pubsub_redis = self._build_pubsub_redis()
                pubsub = self._pubsub_redis.pubsub()
                await pubsub.subscribe(self.sub_channel)
                _log.info("Subscribed to pub/sub channel %s", self.sub_channel)
                backoff = 1.0  # reset after a successful (re)subscribe
                while True:
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=self._pubsub_poll_timeout,
                    )
                    if message is None:
                        # Idle gap: no message within the poll window. The health
                        # check keeps the connection warm; just keep waiting.
                        continue
                    if message.get("type") != "message":
                        continue
                    try:
                        update = TaskUpdate.model_validate_json(message["data"])
                    except ValidationError as exc:
                        _log.warning(
                            "Skipping malformed pub/sub message on %s: %s",
                            self.sub_channel,
                            exc,
                        )
                        continue
                    yield update
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log.warning(
                    "Pub/sub subscription on %s dropped (%s); reconnecting in %.1fs",
                    self.sub_channel,
                    exc,
                    backoff,
                )
                # Discard the (possibly broken) connection so the next iteration
                # rebuilds it from scratch.
                if self._pubsub_redis is not None:
                    try:
                        await self._pubsub_redis.aclose()
                    except Exception:
                        pass
                    self._pubsub_redis = None
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._pubsub_reconnect_backoff_max)
            finally:
                if pubsub is not None:
                    try:
                        await pubsub.aclose()
                    except Exception:
                        pass
