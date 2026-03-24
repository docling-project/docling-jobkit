"""Ray Task Dispatcher - Ray Actor for round-robin task scheduling."""

import asyncio
import datetime
import logging
from typing import Any

import ray

from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.logging_utils import (
    configure_ray_actor_logging,
)
from docling_jobkit.orchestrators.ray.models import TaskUpdate
from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager

_log = logging.getLogger(__name__)


@ray.remote
class RayTaskDispatcher:
    """Ray Task Dispatcher - Round-robin scheduling at TASK level.

    This Ray Actor runs a continuous dispatch loop that:
    1. Discovers all users with pending tasks
    2. For each user (round-robin):
       - Peeks at next task in user's queue
       - Checks if user has capacity
       - If yes: pops task, updates limits, schedules with Ray Serve
       - If no: skips user this round

    The dispatcher ensures fair resource allocation by giving each user
    an equal opportunity to have their tasks processed, regardless of
    queue size.
    """

    def __init__(
        self,
        config: RayOrchestratorConfig,
        deployment_handle: Any,  # Ray Serve deployment handle
    ):
        """Initialize the Ray Task Dispatcher.

        Args:
            config: Orchestrator configuration
            deployment_handle: Ray Serve deployment handle for processing tasks
        """
        configure_ray_actor_logging(config.log_level)

        self.config = config
        self.deployment_handle = deployment_handle

        self.redis_manager = RedisStateManager(
            redis_url=config.redis_url,
            results_ttl=config.results_ttl,
            results_prefix=config.results_prefix,
            sub_channel=config.sub_channel,
            max_connections=config.redis_max_connections,
            socket_timeout=config.redis_socket_timeout,
            socket_connect_timeout=config.redis_socket_connect_timeout,
            max_concurrent_tasks=config.max_concurrent_tasks,
            max_queued_tasks=config.max_queued_tasks,
            max_documents=config.max_documents,
            log_level=config.log_level,
        )
        self.active = False
        self.last_heartbeat = datetime.datetime.now(datetime.timezone.utc)

        # Track background tasks to prevent garbage collection
        self._background_tasks: set[asyncio.Task] = set()

        # Configure logging level for dispatcher
        _log.setLevel(self.config.log_level.upper())

        _log.info("RayTaskDispatcher initialized with Ray Serve deployment")

    async def start_dispatching(self):
        """Start the dispatcher loop."""
        if self.active:
            _log.warning("Dispatcher already active")
            return

        _log.info("Starting Ray Task Dispatcher")
        self.active = True

        # Connect to Redis
        await self.redis_manager.connect()

        # Start dispatch loop
        await self._dispatch_loop()

    async def stop_dispatching(self):
        """Stop the dispatcher loop."""
        _log.info("Stopping Fair Task Dispatcher")
        self.active = False

        # Disconnect from Redis
        await self.redis_manager.disconnect()

    async def get_heartbeat(self) -> datetime.datetime:
        """Get last heartbeat timestamp for health monitoring.

        Returns:
            Last heartbeat timestamp
        """
        return self.last_heartbeat

    async def is_active(self) -> bool:
        """Check if dispatcher is active.

        Returns:
            True if dispatcher is running
        """
        return self.active

    async def _dispatch_loop(self):
        """Main dispatcher loop - implements fair task scheduling.

        Continuously runs dispatch rounds at configured intervals,
        with error handling and heartbeat updates.
        """
        round_count = 0

        while self.active:
            try:
                round_count += 1

                # Update heartbeat
                if self.config.enable_heartbeat:
                    self.last_heartbeat = datetime.datetime.now(datetime.timezone.utc)

                # Log stats every 10 rounds
                if round_count % 10 == 0:
                    await self._log_dispatcher_stats()

                # Execute one dispatch round
                await self._dispatch_round()

            except Exception as e:
                _log.error(f"Error in dispatch round: {e}", exc_info=True)

            # Wait before next round
            await asyncio.sleep(self.config.dispatcher_interval)

    async def _log_dispatcher_stats(self):
        """Log comprehensive dispatcher statistics."""
        users = await self.redis_manager.get_all_users_with_tasks()

        _log.info("=" * 60)
        _log.info("[DISPATCHER-STATS] Current State:")

        total_active = 0
        total_queued = 0

        for user_id in users:
            active_count = await self.redis_manager.get_user_active_task_count(user_id)
            limits = await self.redis_manager.get_user_limits(user_id)
            queue_size = await self.redis_manager.get_user_queue_size(user_id)

            total_active += active_count
            total_queued += queue_size

            _log.info(
                f"  User {user_id}: "
                f"active={active_count}/{limits.max_concurrent_tasks}, "
                f"queued={queue_size}"
            )

        _log.info(
            f"  TOTAL: active={total_active}, queued={total_queued}, users={len(users)}"
        )
        _log.info("=" * 60)

    async def _dispatch_round(self):
        """Execute one round of fair task dispatching.

        Algorithm:
        1. Update dispatcher heartbeat
        2. Check for orphaned tasks (from previous dispatcher crash)
        3. Discover all users with pending tasks
        4. For each user (round-robin):
           a. Check current active task count
           b. Launch tasks until max_concurrent_tasks limit reached
           c. Skip user if at capacity or no more tasks

        This ensures fair scheduling while maximizing concurrency - each user
        can have up to max_concurrent_tasks running simultaneously.
        """
        # Update dispatcher heartbeat
        await self.redis_manager.update_dispatcher_heartbeat()

        # Check for orphaned tasks (from previous dispatcher crash)
        await self._recover_orphaned_tasks()

        # Get all users with tasks
        users = await self.redis_manager.get_all_users_with_tasks()

        if not users:
            _log.debug("[DISPATCH-ROUND] No users with pending tasks")
            return

        _log.info(f"[DISPATCH-ROUND] Starting: {len(users)} users with tasks")

        # Round-robin: launch UP TO max_concurrent_tasks per user
        for user_id in users:
            try:
                # Get current active count from Redis (source of truth)
                active_count = await self.redis_manager.get_user_active_task_count(
                    user_id
                )
                limits = await self.redis_manager.get_user_limits(user_id)
                queue_size = await self.redis_manager.get_user_queue_size(user_id)

                capacity_available = limits.max_concurrent_tasks - active_count

                _log.info(
                    f"[DISPATCH-USER] {user_id}: "
                    f"active={active_count}/{limits.max_concurrent_tasks}, "
                    f"queued={queue_size}, "
                    f"capacity={capacity_available}"
                )

                # Launch tasks until we hit the limit
                tasks_launched = 0
                while active_count < limits.max_concurrent_tasks and queue_size > 0:
                    dispatched = await self._dispatch_user_task(user_id)
                    if not dispatched:
                        break  # No more tasks or dispatch failed

                    tasks_launched += 1
                    active_count += 1
                    queue_size -= 1

                if tasks_launched > 0:
                    _log.info(
                        f"[DISPATCH-USER] {user_id}: Launched {tasks_launched} tasks this round"
                    )

            except Exception as e:
                _log.error(
                    f"Error dispatching tasks for user {user_id}: {e}", exc_info=True
                )

        _log.info("[DISPATCH-ROUND] Completed")

    async def _dispatch_user_task(self, user_id: str) -> bool:
        """Dispatch ONE task for a user (fire-and-forget with Redis tracking).

        Args:
            user_id: User identifier

        Returns:
            True if task was dispatched, False otherwise
        """
        # Peek at next task
        task = await self.redis_manager.peek_task(user_id)
        if not task:
            _log.debug(f"[DISPATCH] User {user_id}: No tasks in queue")
            return False

        task_size = len(task.sources)

        # Check if user can process (has capacity for active tasks)
        can_process, reason = await self.redis_manager.check_user_can_process(
            user_id, task_size
        )
        if not can_process:
            _log.debug(f"[DISPATCH] User {user_id}: SKIP - {reason}")
            return False

        # ATOMIC: Pop task + Add to active set + Update limits
        success = await self.redis_manager.dispatch_task_atomic(
            user_id, task.task_id, task_size
        )
        if not success:
            _log.warning(
                f"[DISPATCH] User {user_id}: Failed atomic dispatch for {task.task_id}"
            )
            return False

        # Launch task asynchronously (fire-and-forget)
        # The task state is now in Redis, so restart-safe
        # Store reference to prevent premature garbage collection
        bg_task = asyncio.create_task(self._process_task_async(task, user_id))
        self._background_tasks.add(bg_task)
        bg_task.add_done_callback(self._background_tasks.discard)

        _log.info(
            f"[DISPATCH] User {user_id}: LAUNCHED task {task.task_id} ({task_size} docs)"
        )

        return True

    async def _process_task_async(self, task: Task, user_id: str):
        """Process task asynchronously with Redis state tracking.

        This method is fire-and-forget but all state is persisted in Redis,
        making it resilient to dispatcher restarts.

        Args:
            task: Task to process
            user_id: User identifier
        """
        task_id = task.task_id
        task_size = len(task.sources)

        try:
            _log.info(f"[TASK-START] {task_id}: Processing {task_size} documents")

            # Update dispatch state to "dispatched" (task sent to actor but not yet running)
            await self.redis_manager.set_task_dispatch_state(task_id, "dispatched")
            # Keep status as PENDING - actor will update to STARTED when it begins processing
            # NOTE: Actor will call mark_task_processing() when it actually starts

            # Process with Ray Serve (this is the long-running operation)
            # Ray handles its own fault tolerance and retries
            # The actor has Redis access and will update status to STARTED when it begins processing
            result = await self.deployment_handle.process_task.remote(task)

            # Store results
            result_key = await self.redis_manager.store_task_result(task_id, result)

            # Update status to SUCCESS
            await self.redis_manager.update_task_status(task_id, TaskStatus.SUCCESS)

            # Publish update
            await self.redis_manager.publish_update(
                TaskUpdate(
                    task_id=task_id,
                    task_status=TaskStatus.SUCCESS,
                    result_key=result_key,
                    progress=None,  # No progress object in DoclingTaskResult
                )
            )

            # Update user stats
            await self.redis_manager.update_user_stats(
                user_id,
                delta_total_tasks=1,
                delta_total_documents=task_size,
                delta_successful_documents=result.num_succeeded,
                delta_failed_documents=result.num_failed,
            )

            _log.info(f"[TASK-SUCCESS] {task_id}: Completed successfully")

        except Exception as e:
            _log.error(f"[TASK-FAILURE] {task_id}: {e}", exc_info=True)

            # Update status to FAILURE
            await self.redis_manager.update_task_status(
                task_id, TaskStatus.FAILURE, error_message=str(e)
            )

            # Publish update
            await self.redis_manager.publish_update(
                TaskUpdate(
                    task_id=task_id,
                    task_status=TaskStatus.FAILURE,
                    error_message=str(e),
                )
            )

            # Update user stats
            await self.redis_manager.update_user_stats(
                user_id,
                delta_total_tasks=1,
                delta_total_documents=task_size,
                delta_failed_documents=task_size,
            )

        finally:
            # CRITICAL: Remove from active set and decrement counters (ATOMIC)
            await self.redis_manager.complete_task_atomic(user_id, task_id, task_size)
            _log.info(f"[TASK-CLEANUP] {task_id}: Released capacity for user {user_id}")

    async def _recover_orphaned_tasks(self):
        """Recover tasks orphaned by dispatcher crash.

        Checks for tasks in 'active' state but dispatcher heartbeat is stale.
        Re-queues or marks as failed based on task age and retry count.
        """
        # Check if dispatcher heartbeat is stale
        heartbeat_age = await self.redis_manager.get_dispatcher_heartbeat_age()

        # Only recover if heartbeat is very stale (3x interval)
        if heartbeat_age < (self.config.dispatcher_interval * 3):
            return

        _log.warning(
            f"[RECOVERY] Stale heartbeat detected ({heartbeat_age:.1f}s), checking for orphaned tasks"
        )

        # Get all users with active tasks
        users = await self.redis_manager.get_all_users_with_active_tasks()

        recovered_count = 0
        failed_count = 0

        for user_id in users:
            # Get active task IDs for this user
            active_task_ids = await self.redis_manager.get_user_active_task_ids(user_id)

            for task_id in active_task_ids:
                # Check task processing state
                processing_state = await self.redis_manager.get_task_processing_state(
                    task_id
                )

                if not processing_state:
                    # No processing state = orphaned
                    _log.warning(
                        f"[RECOVERY] Orphaned task {task_id} for user {user_id}"
                    )

                    # Get task metadata to check retry count
                    metadata = await self.redis_manager.get_task_metadata(task_id)
                    retry_count = int(metadata.get("retry_count", 0))

                    if retry_count < self.config.max_task_retries:
                        # Mark as failed and let client retry
                        _log.info(
                            f"[RECOVERY] Marking task {task_id} as failed (retry {retry_count + 1})"
                        )

                        await self.redis_manager.update_task_status(
                            task_id,
                            TaskStatus.FAILURE,
                            error_message=f"Task orphaned by dispatcher restart (retry {retry_count})",
                        )

                        # Remove from active set
                        task_size = int(processing_state.get("task_size", 1))
                        await self.redis_manager.complete_task_atomic(
                            user_id, task_id, task_size
                        )

                        recovered_count += 1
                    else:
                        # Max retries exceeded
                        _log.error(
                            f"[RECOVERY] Task {task_id} exceeded max retries, marking as failed"
                        )

                        await self.redis_manager.update_task_status(
                            task_id,
                            TaskStatus.FAILURE,
                            error_message=f"Task failed after {retry_count} retries (dispatcher restarts)",
                        )

                        # Remove from active set
                        task_size = int(processing_state.get("task_size", 1))
                        await self.redis_manager.complete_task_atomic(
                            user_id, task_id, task_size
                        )

                        failed_count += 1

        if recovered_count > 0 or failed_count > 0:
            _log.warning(
                f"[RECOVERY] Completed: recovered={recovered_count}, failed={failed_count}"
            )

    async def get_stats(self) -> dict:
        """Get comprehensive dispatcher statistics.

        Returns:
            Dictionary with dispatcher stats including per-user details
        """
        users = await self.redis_manager.get_all_users_with_tasks()

        # Aggregate user stats
        total_active_tasks = 0
        total_queued_tasks = 0
        total_capacity_available = 0
        user_details = []

        for user_id in users:
            active_count = await self.redis_manager.get_user_active_task_count(user_id)
            limits = await self.redis_manager.get_user_limits(user_id)
            queue_size = await self.redis_manager.get_user_queue_size(user_id)

            total_active_tasks += active_count
            total_queued_tasks += queue_size
            capacity_available = limits.max_concurrent_tasks - active_count
            total_capacity_available += capacity_available

            utilization_pct = (
                (active_count / limits.max_concurrent_tasks * 100)
                if limits.max_concurrent_tasks > 0
                else 0
            )

            user_details.append(
                {
                    "user_id": user_id,
                    "active_tasks": active_count,
                    "max_concurrent_tasks": limits.max_concurrent_tasks,
                    "queued_tasks": queue_size,
                    "capacity_available": capacity_available,
                    "utilization_pct": round(utilization_pct, 1),
                }
            )

        # Get Ray Serve deployment stats
        deployment_stats = {
            "min_replicas": self.config.min_actors,
            "max_replicas": self.config.max_actors,
            "target_requests_per_replica": self.config.target_requests_per_replica,
        }

        stats = {
            "active": self.active,
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "users_with_tasks": len(users),
            "total_active_tasks": total_active_tasks,
            "total_queued_tasks": total_queued_tasks,
            "total_capacity_available": total_capacity_available,
            "user_details": user_details,
            "ray_serve_deployment": deployment_stats,
            "config": {
                "dispatcher_interval": self.config.dispatcher_interval,
                "max_concurrent_tasks": self.config.max_concurrent_tasks,
                "max_queued_tasks": self.config.max_queued_tasks,
            },
        }

        return stats
