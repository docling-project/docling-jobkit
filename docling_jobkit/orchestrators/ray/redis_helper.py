"""Redis state management for Ray orchestrator."""

import datetime
import inspect
import json
import logging
import math
from typing import Optional

import msgpack
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool
from redis.exceptions import NoScriptError, WatchError

from docling.datamodel.service.tasks import TaskType

from docling_jobkit.datamodel.result import DoclingTaskResult
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.ray.models import (
    RedisTaskMetadata,
    TaskTerminalizationResult,
    TaskUpdate,
    TenantLimits,
    TenantStats,
)
from docling_jobkit.orchestrators.serialization import make_msgpack_safe

_log = logging.getLogger(__name__)

_UPDATE_TASK_EXECUTION_HEARTBEAT_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    redis.call('HSET', KEYS[1], 'heartbeat_at', ARGV[1])
    return 1
end
return 0
"""


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
    ):
        """Initialize Redis state manager.

        Args:
            redis_url: Redis connection URL (supports standard, sentinel, cluster)
            results_ttl: Time-to-live for task results in seconds
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
        self.results_prefix = results_prefix
        self.sub_channel = sub_channel
        self.log_level = log_level

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
        self._update_task_execution_heartbeat_sha: Optional[str] = None

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
            self._update_task_execution_heartbeat_sha = None
            _log.info(f"Connected to Redis at {self.redis_url}")

    async def disconnect(self):
        """Close Redis connection and pool."""
        if self.redis:
            await self.redis.aclose()
            self.redis = None
            self._update_task_execution_heartbeat_sha = None
        if self.pool:
            await self.pool.aclose()
            self.pool = None
        _log.info("Disconnected from Redis")

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
        task_json = task.model_dump_json()

        await self.redis.rpush(queue_key, task_json)  # type: ignore[misc, union-attr]

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
            task = Task.model_validate_json(task_json)
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
        """
        if not self.redis:
            await self.connect()

        queue_key = f"tenant:{tenant_id}:tasks"
        redis = self._ensure_redis()
        task_json = await redis.lindex(queue_key, 0)  # type: ignore[misc]

        if task_json:
            return Task.model_validate_json(task_json)

        return None

    async def get_all_tenants_with_tasks(self) -> list[str]:
        """Get list of all tenants with pending tasks.

        Returns:
            List of tenant IDs
        """
        if not self.redis:
            await self.connect()

        # Scan for all tenant task queue keys
        tenants = []
        redis = self._ensure_redis()
        async for key in redis.scan_iter(match="tenant:*:tasks"):  # type: ignore[union-attr]
            key_str = key.decode("utf-8")
            # Extract tenant_id from "tenant:{tenant_id}:tasks"
            parts = key_str.split(":")
            if len(parts) == 3:
                tenant_id = parts[1]
                # Check if queue has tasks
                queue_len = await redis.llen(key)  # type: ignore[misc]
                if queue_len > 0:
                    tenants.append(tenant_id)

        return tenants

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
        progress: Optional[dict] = None,
    ) -> None:
        """Update task status and metadata.

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

        if progress:
            updates["progress"] = json.dumps(progress)

        if status == TaskStatus.STARTED:
            updates["started_at"] = timestamp
        if status in (TaskStatus.SUCCESS, TaskStatus.FAILURE):
            updates["finished_at"] = timestamp

        redis = self._ensure_redis()
        await redis.hset(task_key, mapping=updates)  # type: ignore[misc]

        _log.debug(f"Updated task {task_id} status to {status}")

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
    ) -> None:
        """Initialize task metadata.

        Args:
            task_id: Task identifier
            tenant_id: Tenant identifier
            task_type: Type of task
            task_size: Number of documents associated with the task
            status: Initial task status
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

        redis = self._ensure_redis()
        await redis.hset(task_key, mapping=metadata)  # type: ignore[misc]

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
        result_data = self._serialize_task_result(result)

        # Store with TTL
        redis = self._ensure_redis()
        await redis.setex(result_key, self.results_ttl, result_data)  # type: ignore[union-attr]

        _log.debug(f"Stored result for task {task_id} with TTL {self.results_ttl}s")

        return result_key

    @staticmethod
    def _serialize_task_result(result: DoclingTaskResult) -> bytes:
        """Serialize a task result for Redis storage."""
        safe_data = make_msgpack_safe(result.model_dump())
        return msgpack.packb(safe_data, use_bin_type=True)

    async def get_task_result(self, task_id: str) -> Optional[DoclingTaskResult]:
        """Retrieve task result from Redis.

        Args:
            task_id: Task identifier

        Returns:
            Task result or None if not found
        """
        if not self.redis:
            await self.connect()

        result_key = f"{self.results_prefix}:task:{task_id}:result"
        redis = self._ensure_redis()
        result_data = await redis.get(result_key)  # type: ignore[union-attr]

        if result_data:
            # Use strict_map_key=False to allow integer keys in dicts
            result_dict = msgpack.unpackb(result_data, raw=False, strict_map_key=False)
            return DoclingTaskResult.model_validate(result_dict)

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

        # Get current limits or initialize
        limits = await self.get_tenant_limits(tenant_id)

        # Update counters
        limits.active_tasks = max(0, limits.active_tasks + delta_active_tasks)
        limits.queued_tasks = max(0, limits.queued_tasks + delta_queued_tasks)
        limits.active_documents = max(0, limits.active_documents + delta_docs)

        # Store updated limits
        limits_dict = limits.model_dump()
        # Convert None to string for Redis
        limits_dict = {k: str(v) for k, v in limits_dict.items()}

        redis = self._ensure_redis()
        await redis.hset(limits_key, mapping=limits_dict)  # type: ignore[misc]

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

        # Use HINCRBY for atomic increments
        if delta_total_tasks != 0:
            redis = self._ensure_redis()
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

        if not stats_data:
            return TenantStats()

        # Decode and parse
        stats_dict = {}
        for k, v in stats_data.items():
            key = k.decode("utf-8")
            value = int(v.decode("utf-8"))
            stats_dict[key] = value

        return TenantStats.model_validate(stats_dict)

    # Atomic Task Dispatch Operations

    async def dispatch_task_atomic(
        self, tenant_id: str, task_id: str, task_size: int
    ) -> bool:
        """Atomically dispatch a task: pop from queue, add to active set, update limits.

        Uses Redis transaction (MULTI/EXEC) for atomicity to prevent race conditions.

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
        dispatch_key = f"task:{task_id}:dispatch"

        redis = self._ensure_redis()

        # Use pipeline for atomic operations
        async with redis.pipeline(transaction=True) as pipe:
            try:
                # Watch keys for changes
                await pipe.watch(queue_key, active_key, limits_key)

                # Check if task is still at front of queue
                front_task_json = await redis.lindex(queue_key, 0)  # type: ignore[misc]
                if not front_task_json:
                    await pipe.unwatch()
                    return False

                front_task = Task.model_validate_json(front_task_json)
                if front_task.task_id != task_id:
                    # Race condition: someone else popped it
                    await pipe.unwatch()
                    _log.debug(
                        f"[REDIS-ATOMIC] Race condition: task {task_id} not at front"
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
                pipe.hset(
                    dispatch_key,
                    mapping={
                        "tenant_id": tenant_id,
                        "dispatched_at": str(now_timestamp),
                        "task_size": str(task_size),
                    },
                )
                pipe.expire(dispatch_key, self.processing_ttl)

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
        result_data = self._serialize_task_result(result)
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
    ) -> TaskTerminalizationResult:
        """Durably finalize a task to FAILURE exactly once."""
        return await self._finalize_task_terminal_state_atomic(
            tenant_id=tenant_id,
            task_id=task_id,
            task_size=task_size,
            terminal_status=TaskStatus.FAILURE,
            error_message=error_message,
        )

    async def _finalize_task_terminal_state_atomic(
        self,
        tenant_id: str,
        task_id: str,
        task_size: int,
        terminal_status: TaskStatus,
        error_message: Optional[str] = None,
        result_key: Optional[str] = None,
        result_data: Optional[bytes] = None,
    ) -> TaskTerminalizationResult:
        """Finalize metadata + cleanup atomically while preserving the first terminal state."""
        if not self.redis:
            await self.connect()

        task_key = f"task:{task_id}"
        active_key = f"tenant:{tenant_id}:active_tasks"
        limits_key = f"tenant:{tenant_id}:limits"
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

                    sismember_result = pipe.sismember(active_key, task_id)
                    if inspect.isawaitable(sismember_result):
                        was_active = bool(await sismember_result)
                    else:
                        was_active = bool(sismember_result)

                    pipe.multi()

                    if (
                        terminal_status == TaskStatus.SUCCESS
                        and current_status != TaskStatus.FAILURE
                        and result_key is not None
                        and result_data is not None
                    ):
                        pipe.setex(result_key, self.results_ttl, result_data)

                    if current_status not in (
                        TaskStatus.SUCCESS,
                        TaskStatus.FAILURE,
                    ):
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
                        pipe.hset(task_key, mapping=updates)

                    if terminal_status == TaskStatus.SUCCESS:
                        pipe.hdel(task_key, "error_message")
                    pipe.delete(dispatch_key, execution_key)

                    if was_active:
                        pipe.srem(active_key, task_id)
                        pipe.hincrby(limits_key, "active_tasks", -1)
                        if self.max_documents is not None:
                            pipe.hincrby(limits_key, "active_documents", -task_size)

                    await pipe.execute()

                    status_changed = current_status not in (
                        TaskStatus.SUCCESS,
                        TaskStatus.FAILURE,
                    )
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
        tenants = []
        redis = self._ensure_redis()
        async for key in redis.scan_iter(match="tenant:*:active_tasks"):  # type: ignore[union-attr]
            key_str = key.decode("utf-8")
            parts = key_str.split(":")
            if len(parts) == 3:
                tenant_id = parts[1]
                # Check if set is non-empty
                count = await redis.scard(key)  # type: ignore[misc]
                if count > 0:
                    tenants.append(tenant_id)
        return tenants

    async def get_all_tenants_with_any_tasks(self) -> list[str]:
        """Get list of all tenants with pending OR active tasks.

        This combines tenants from both queued tasks and active tasks,
        providing complete visibility for metrics and monitoring.

        Returns:
            List of unique tenant IDs with any tasks (queued or active)
        """
        tenants_set = set()

        # Get tenants with queued tasks (waiting to be dispatched)
        queued_tenants = await self.get_all_tenants_with_tasks()
        tenants_set.update(queued_tenants)

        # Get tenants with active tasks (currently being processed)
        active_tenants = await self.get_all_tenants_with_active_tasks()
        tenants_set.update(active_tenants)

        return list(tenants_set)

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
        active_task_ids = await self.get_tenant_active_task_ids(tenant_id)
        active_documents = 0
        for task_id in active_task_ids:
            active_documents += await self._get_task_size_for_resync(task_id)

        limits = await self.get_tenant_limits(tenant_id)
        limits.active_tasks = len(active_task_ids)
        limits.queued_tasks = await self.get_tenant_queue_size(tenant_id)
        limits.active_documents = active_documents

        limits_key = f"tenant:{tenant_id}:limits"
        limits_dict = {key: str(value) for key, value in limits.model_dump().items()}
        redis = self._ensure_redis()
        await redis.hset(limits_key, mapping=limits_dict)  # type: ignore[misc]
        return limits

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

    async def write_task_execution_lease(
        self, task_id: str, tenant_id: str, replica_id: str
    ) -> None:
        """Write the replica-owned execution lease for a task.

        Called by the replica when it begins executing a task. The lease proves
        a live replica owns this work. It is refreshed by
        update_task_execution_heartbeat() and deleted by finalize_task_*_atomic().
        No TTL — the key is cleaned up explicitly at terminalization.
        """
        execution_key = f"task:{task_id}:execution"
        redis = self._ensure_redis()
        now_timestamp = str(datetime.datetime.now(datetime.timezone.utc).timestamp())
        await redis.hset(  # type: ignore[misc]
            execution_key,
            mapping={
                "replica_id": replica_id,
                "tenant_id": tenant_id,
                "claimed_at": now_timestamp,
                "heartbeat_at": now_timestamp,
            },
        )

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

        if self._update_task_execution_heartbeat_sha is None:
            self._update_task_execution_heartbeat_sha = await redis.script_load(  # type: ignore[misc]
                _UPDATE_TASK_EXECUTION_HEARTBEAT_LUA
            )

        try:
            updated = await redis.evalsha(  # type: ignore[misc]
                self._update_task_execution_heartbeat_sha,
                1,
                execution_key,
                now_timestamp,
            )
        except NoScriptError:
            self._update_task_execution_heartbeat_sha = await redis.script_load(  # type: ignore[misc]
                _UPDATE_TASK_EXECUTION_HEARTBEAT_LUA
            )
            updated = await redis.evalsha(  # type: ignore[misc]
                self._update_task_execution_heartbeat_sha,
                1,
                execution_key,
                now_timestamp,
            )

        return bool(updated)

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

    async def subscribe_to_updates(self):
        """Subscribe to task updates channel.

        Returns:
            Async iterator of TaskUpdate messages
        """
        if not self.redis:
            await self.connect()

        pubsub = self.redis.pubsub()
        await pubsub.subscribe(self.sub_channel)

        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    update_json = message["data"]
                    update = TaskUpdate.model_validate_json(update_json)
                    yield update
        finally:
            await pubsub.unsubscribe(self.sub_channel)
            await pubsub.aclose()
