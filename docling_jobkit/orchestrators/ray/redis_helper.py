"""Redis state management for Ray orchestrator."""

import json
import logging
from typing import Optional

import msgpack
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool

from docling_jobkit.datamodel.result import DoclingTaskResult
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.ray.models import (
    TaskUpdate,
    UserLimits,
    UserStats,
)
from docling_jobkit.orchestrators.serialization import make_msgpack_safe

_log = logging.getLogger(__name__)


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

        # Store connection parameters - pool will be created in connect()
        self.max_connections = max_connections
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout

        # Connection pool and client will be created in connect()
        self.pool: Optional[ConnectionPool] = None
        self.redis: Optional[Redis] = None

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
            _log.info(f"Connected to Redis at {self.redis_url}")

    async def disconnect(self):
        """Close Redis connection and pool."""
        if self.redis:
            await self.redis.aclose()
            self.redis = None
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

    async def enqueue_task(self, user_id: str, task: Task) -> None:
        """Add task to user's queue.

        Args:
            user_id: User identifier
            task: Task to enqueue
        """
        if not self.redis:
            await self.connect()

        queue_key = f"user:{user_id}:tasks"
        task_json = task.model_dump_json()

        await self.redis.rpush(queue_key, task_json)  # type: ignore[misc, union-attr]

        # Update user limits
        await self.update_user_limits(user_id, delta_queued_tasks=1)

        _log.debug(f"Enqueued task {task.task_id} for user {user_id}")

    async def dequeue_task(self, user_id: str) -> Optional[Task]:
        """Remove and return next task from user's queue.

        Args:
            user_id: User identifier

        Returns:
            Next task or None if queue is empty
        """
        if not self.redis:
            await self.connect()

        queue_key = f"user:{user_id}:tasks"
        redis = self._ensure_redis()
        task_json = await redis.lpop(queue_key)  # type: ignore[misc]

        if task_json:
            task = Task.model_validate_json(task_json)
            # Update user limits
            await self.update_user_limits(user_id, delta_queued_tasks=-1)
            _log.debug(f"Dequeued task {task.task_id} for user {user_id}")
            return task

        return None

    async def peek_task(self, user_id: str) -> Optional[Task]:
        """View next task without removing it.

        Args:
            user_id: User identifier

        Returns:
            Next task or None if queue is empty
        """
        if not self.redis:
            await self.connect()

        queue_key = f"user:{user_id}:tasks"
        redis = self._ensure_redis()
        task_json = await redis.lindex(queue_key, 0)  # type: ignore[misc]

        if task_json:
            return Task.model_validate_json(task_json)

        return None

    async def get_all_users_with_tasks(self) -> list[str]:
        """Get list of all users with pending tasks.

        Returns:
            List of user IDs
        """
        if not self.redis:
            await self.connect()

        # Scan for all user task queue keys
        users = []
        redis = self._ensure_redis()
        async for key in redis.scan_iter(match="user:*:tasks"):  # type: ignore[union-attr]
            key_str = key.decode("utf-8")
            # Extract user_id from "user:{user_id}:tasks"
            parts = key_str.split(":")
            if len(parts) == 3:
                user_id = parts[1]
                # Check if queue has tasks
                queue_len = await redis.llen(key)  # type: ignore[misc]
                if queue_len > 0:
                    users.append(user_id)

        return users

    async def get_user_queue_size(self, user_id: str) -> int:
        """Get number of tasks in user's queue.

        Args:
            user_id: User identifier

        Returns:
            Number of queued tasks
        """
        if not self.redis:
            await self.connect()

        queue_key = f"user:{user_id}:tasks"
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

        updates = {
            "status": status.value,
            "last_update_at": str(json.dumps(None)),  # Will be set by caller
        }

        if error_message:
            updates["error_message"] = error_message

        if progress:
            updates["progress"] = json.dumps(progress)

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
        user_id: str,
        status: TaskStatus = TaskStatus.PENDING,
    ) -> None:
        """Initialize task metadata.

        Args:
            task_id: Task identifier
            user_id: User identifier
            status: Initial task status
        """
        if not self.redis:
            await self.connect()

        task_key = f"task:{task_id}"

        metadata = {
            "task_id": task_id,
            "user_id": user_id,
            "status": status.value,
            "created_at": str(json.dumps(None)),  # Will be set by caller
        }

        redis = self._ensure_redis()
        await redis.hset(task_key, mapping=metadata)  # type: ignore[misc]

    async def set_task_dispatch_state(
        self, task_id: str, dispatch_state: Optional[str]
    ) -> None:
        """Set internal dispatch state for a task.

        Args:
            task_id: Task identifier
            dispatch_state: One of "queued", "dispatched", or None to clear
        """
        if not self.redis:
            await self.connect()

        task_key = f"task:{task_id}"
        redis = self._ensure_redis()

        if dispatch_state is None:
            # Clear the dispatch_state field
            await redis.hdel(task_key, "dispatch_state")  # type: ignore[misc]
        else:
            await redis.hset(task_key, "dispatch_state", dispatch_state)  # type: ignore[misc]

    async def get_task_dispatch_state(self, task_id: str) -> Optional[str]:
        """Get internal dispatch state for a task.

        Args:
            task_id: Task identifier

        Returns:
            "queued", "dispatched", or None if not set
        """
        if not self.redis:
            await self.connect()

        task_key = f"task:{task_id}"
        redis = self._ensure_redis()
        state = await redis.hget(task_key, "dispatch_state")  # type: ignore[misc]
        return state.decode("utf-8") if state else None

    async def get_user_dispatched_task_count(self, user_id: str) -> int:
        """Count tasks in dispatched state for a user.

        Returns:
            Number of tasks with dispatch_state="dispatched" and status=PENDING
        """
        if not self.redis:
            await self.connect()

        active_key = f"user:{user_id}:active_tasks"
        redis = self._ensure_redis()
        task_ids = await redis.smembers(active_key)  # type: ignore[misc]

        count = 0
        for task_id_bytes in task_ids:
            task_id = task_id_bytes.decode("utf-8")
            metadata = await self.get_task_metadata(task_id)

            # Count if status is PENDING and dispatch_state is "dispatched"
            if (
                metadata.get("status") == TaskStatus.PENDING.value
                and metadata.get("dispatch_state") == "dispatched"
            ):
                count += 1

        return count

    async def get_user_running_task_count(self, user_id: str) -> int:
        """Count tasks actively running for a user.

        Uses processing state instead of task metadata for more reliable counting.
        A task is considered "running" if it has processing state with status="processing".

        Returns:
            Number of tasks with processing status="processing" (actively being processed)
        """
        if not self.redis:
            await self.connect()

        active_key = f"user:{user_id}:active_tasks"
        redis = self._ensure_redis()
        task_ids = await redis.smembers(active_key)  # type: ignore[misc]

        count = 0
        for task_id_bytes in task_ids:
            task_id = task_id_bytes.decode("utf-8")

            # Check processing state instead of task metadata
            # Processing state is deleted when task completes, so it's more reliable
            processing_state = await self.get_task_processing_state(task_id)

            # Count if processing state exists and status is "processing"
            # (status="dispatched" means sent to actor but not yet started)
            if processing_state and processing_state.get("status") == "processing":
                count += 1

        return count

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

        # Serialize result using msgpack (make safe for msgpack serialization)
        safe_data = make_msgpack_safe(result.model_dump())
        result_data = msgpack.packb(safe_data, use_bin_type=True)

        # Store with TTL
        redis = self._ensure_redis()
        await redis.setex(result_key, self.results_ttl, result_data)  # type: ignore[union-attr]

        _log.debug(f"Stored result for task {task_id} with TTL {self.results_ttl}s")

        return result_key

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

    # User Limits Operations

    async def get_user_limits(self, user_id: str) -> UserLimits:
        """Get user resource limits and current usage.

        Args:
            user_id: User identifier

        Returns:
            User limits object
        """
        if not self.redis:
            await self.connect()

        limits_key = f"user:{user_id}:limits"
        redis = self._ensure_redis()
        limits_data = await redis.hgetall(limits_key)  # type: ignore[misc]

        if not limits_data:
            # Return defaults
            return UserLimits(
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

        return UserLimits.model_validate(limits_dict)

    async def update_user_limits(
        self,
        user_id: str,
        delta_active_tasks: int = 0,
        delta_queued_tasks: int = 0,
        delta_docs: int = 0,
    ) -> None:
        """Update user resource usage counters.

        Args:
            user_id: User identifier
            delta_active_tasks: Change in active tasks count
            delta_queued_tasks: Change in queued tasks count
            delta_docs: Change in active documents count
        """
        if not self.redis:
            await self.connect()

        limits_key = f"user:{user_id}:limits"

        # Get current limits or initialize
        limits = await self.get_user_limits(user_id)

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

    async def check_user_can_enqueue(
        self, user_id: str, task_size: int
    ) -> tuple[bool, str]:
        """Check if user can enqueue a new task.

        Args:
            user_id: User identifier
            task_size: Number of documents in task

        Returns:
            Tuple of (can_enqueue, reason_if_not)
        """
        limits = await self.get_user_limits(user_id)

        # Check queue limit
        if limits.max_queued_tasks is not None:
            if limits.queued_tasks >= limits.max_queued_tasks:
                return False, f"Queue limit reached ({limits.max_queued_tasks})"

        return True, ""

    async def check_user_can_process(
        self, user_id: str, task_size: int
    ) -> tuple[bool, str]:
        """Check if user can start processing a task.

        Args:
            user_id: User identifier
            task_size: Number of documents in task

        Returns:
            Tuple of (can_process, reason_if_not)
        """
        limits = await self.get_user_limits(user_id)

        _log.debug(
            f"[CAPACITY-CHECK] {user_id}: "
            f"active_tasks={limits.active_tasks}/{limits.max_concurrent_tasks}, "
            f"active_docs={limits.active_documents}/{limits.max_documents or 'unlimited'}, "
            f"task_size={task_size}"
        )

        # Check concurrent task limit
        if limits.active_tasks >= limits.max_concurrent_tasks:
            reason = f"Concurrent task limit reached ({limits.max_concurrent_tasks})"
            _log.debug(f"[CAPACITY-CHECK] {user_id}: BLOCKED - {reason}")
            return False, reason

        # Check document limit if enabled
        if limits.max_documents is not None:
            if limits.active_documents + task_size > limits.max_documents:
                reason = f"Document limit would be exceeded ({limits.max_documents})"
                _log.debug(f"[CAPACITY-CHECK] {user_id}: BLOCKED - {reason}")
                return False, reason

        _log.debug(f"[CAPACITY-CHECK] {user_id}: ALLOWED")
        return True, ""

    # User Statistics Operations

    async def update_user_stats(
        self,
        user_id: str,
        delta_total_tasks: int = 0,
        delta_total_documents: int = 0,
        delta_successful_documents: int = 0,
        delta_failed_documents: int = 0,
    ) -> None:
        """Update user statistics.

        Args:
            user_id: User identifier
            delta_total_tasks: Change in total tasks count
            delta_total_documents: Change in total documents count
            delta_successful_documents: Change in successful documents count
            delta_failed_documents: Change in failed documents count
        """
        if not self.redis:
            await self.connect()

        stats_key = f"user:{user_id}:stats"

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

    async def get_user_stats(self, user_id: str) -> UserStats:
        """Get user statistics.

        Args:
            user_id: User identifier

        Returns:
            User statistics object
        """
        if not self.redis:
            await self.connect()

        stats_key = f"user:{user_id}:stats"
        redis = self._ensure_redis()
        stats_data = await redis.hgetall(stats_key)  # type: ignore[misc]

        if not stats_data:
            return UserStats()

        # Decode and parse
        stats_dict = {}
        for k, v in stats_data.items():
            key = k.decode("utf-8")
            value = int(v.decode("utf-8"))
            stats_dict[key] = value

        return UserStats.model_validate(stats_dict)

    # Atomic Task Dispatch Operations

    async def dispatch_task_atomic(
        self, user_id: str, task_id: str, task_size: int
    ) -> bool:
        """Atomically dispatch a task: pop from queue, add to active set, update limits.

        Uses Redis transaction (MULTI/EXEC) for atomicity to prevent race conditions.

        Args:
            user_id: User identifier
            task_id: Task identifier
            task_size: Number of documents in task

        Returns:
            True if successful, False if failed (e.g., race condition)
        """
        import datetime

        queue_key = f"user:{user_id}:tasks"
        active_key = f"user:{user_id}:active_tasks"
        limits_key = f"user:{user_id}:limits"
        processing_key = f"task:{task_id}:processing"

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
                    processing_key,
                    mapping={
                        "user_id": user_id,
                        "status": "dispatched",
                        "dispatched_at": str(now_timestamp),
                        "task_size": str(task_size),
                    },
                )
                # TTL with buffer (default 1 hour + 1 hour buffer)
                pipe.expire(processing_key, 7200)

                # Execute transaction
                await pipe.execute()

                _log.debug(
                    f"[REDIS-ATOMIC] Dispatched task {task_id} for user {user_id}"
                )
                return True

            except Exception as e:
                # Transaction failed due to concurrent modification or other error
                _log.debug(f"[REDIS-ATOMIC] Failed to dispatch task {task_id}: {e}")
                return False

    async def complete_task_atomic(
        self, user_id: str, task_id: str, task_size: int
    ) -> None:
        """Atomically complete a task: remove from active set, update limits.

        Uses Redis transaction for atomicity.

        NOTE: Processing state is cleaned up by the actor, not here.
        The actor creates the processing state when it starts, so it's responsible
        for deleting it when done.

        Args:
            user_id: User identifier
            task_id: Task identifier
            task_size: Number of documents in task
        """
        active_key = f"user:{user_id}:active_tasks"
        limits_key = f"user:{user_id}:limits"

        redis = self._ensure_redis()

        async with redis.pipeline(transaction=True) as pipe:
            pipe.multi()

            # 1. Remove from active set
            pipe.srem(active_key, task_id)

            # 2. Update limits
            pipe.hincrby(limits_key, "active_tasks", -1)
            if self.max_documents is not None:
                pipe.hincrby(limits_key, "active_documents", -task_size)

            await pipe.execute()

        _log.debug(f"[REDIS-ATOMIC] Completed task {task_id} for user {user_id}")

    async def get_user_active_task_count(self, user_id: str) -> int:
        """Get number of active tasks for user from Redis set.

        This is the source of truth, not the counter in limits.

        Args:
            user_id: User identifier

        Returns:
            Number of active tasks
        """
        active_key = f"user:{user_id}:active_tasks"
        redis = self._ensure_redis()
        count = await redis.scard(active_key)  # type: ignore[misc]
        return int(count)

    async def get_user_active_task_ids(self, user_id: str) -> list[str]:
        """Get list of active task IDs for user.

        Args:
            user_id: User identifier

        Returns:
            List of task IDs
        """
        active_key = f"user:{user_id}:active_tasks"
        redis = self._ensure_redis()
        task_ids = await redis.smembers(active_key)  # type: ignore[misc]
        return [tid.decode("utf-8") for tid in task_ids]

    async def get_all_users_with_active_tasks(self) -> list[str]:
        """Get list of all users with active tasks.

        Returns:
            List of user IDs
        """
        users = []
        redis = self._ensure_redis()
        async for key in redis.scan_iter(match="user:*:active_tasks"):  # type: ignore[union-attr]
            key_str = key.decode("utf-8")
            parts = key_str.split(":")
            if len(parts) == 3:
                user_id = parts[1]
                # Check if set is non-empty
                count = await redis.scard(key)  # type: ignore[misc]
                if count > 0:
                    users.append(user_id)
        return users

    async def get_all_users_with_any_tasks(self) -> list[str]:
        """Get list of all users with pending OR active tasks.

        This combines users from both queued tasks and active tasks,
        providing complete visibility for metrics and monitoring.

        Returns:
            List of unique user IDs with any tasks (queued or active)
        """
        users_set = set()

        # Get users with queued tasks (waiting to be dispatched)
        queued_users = await self.get_all_users_with_tasks()
        users_set.update(queued_users)

        # Get users with active tasks (currently being processed)
        active_users = await self.get_all_users_with_active_tasks()
        users_set.update(active_users)

        return list(users_set)

    async def mark_task_processing(self, task_id: str, user_id: str) -> None:
        """Mark task as actively processing.

        Args:
            task_id: Task identifier
            user_id: User identifier
        """
        import datetime

        processing_key = f"task:{task_id}:processing"
        redis = self._ensure_redis()
        now_timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()
        await redis.hset(processing_key, "status", "processing")  # type: ignore[misc]
        await redis.hset(  # type: ignore[misc]
            processing_key, "processing_started_at", str(now_timestamp)
        )

    async def get_task_processing_state(self, task_id: str) -> dict:
        """Get task processing state.

        Args:
            task_id: Task identifier

        Returns:
            Processing state dictionary
        """
        processing_key = f"task:{task_id}:processing"
        redis = self._ensure_redis()
        state = await redis.hgetall(processing_key)  # type: ignore[misc]
        if not state:
            return {}
        return {k.decode("utf-8"): v.decode("utf-8") for k, v in state.items()}

    # Dispatcher Heartbeat Operations

    async def update_dispatcher_heartbeat(self) -> None:
        """Update dispatcher heartbeat timestamp."""
        import datetime

        heartbeat_key = "dispatcher:heartbeat"
        redis = self._ensure_redis()
        timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()
        # TTL = 3x dispatcher interval (will be set by config)
        await redis.setex(heartbeat_key, 60, str(timestamp))  # type: ignore[union-attr]

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
