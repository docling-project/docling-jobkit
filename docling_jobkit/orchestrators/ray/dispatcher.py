"""Ray Task Dispatcher - Ray Actor for round-robin task scheduling."""

import asyncio
import datetime
import logging
import math
from typing import Any, Optional

import ray

from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.logging_utils import (
    configure_ray_actor_logging,
)
from docling_jobkit.orchestrators.ray.models import (
    RedisTaskMetadata,
    TaskUpdate,
)
from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager

_log = logging.getLogger(__name__)


@ray.remote
class RayTaskDispatcher:
    """Ray Task Dispatcher - round-robin scheduling at TASK level.

    This detached Ray actor runs a continuous dispatch loop that:
    1. Discovers all tenants with pending tasks.
    2. For each tenant in round-robin order:
       - Peeks at the next task in that tenant's queue.
       - Checks whether the tenant still has admission capacity.
       - If yes: pops the task, updates Redis-backed limits, and submits it to Ray Serve.
       - If no: skips that tenant for the current round.

    The dispatcher exists to enforce tenant fairness and admission control in
    front of Ray Serve. It is not the system's durable queue; Redis is.
    """

    def __init__(
        self,
        config: RayOrchestratorConfig,
        deployment_handle: Any,
    ) -> None:
        """Initialize the shared dispatcher actor.

        Args:
            config: Orchestrator configuration.
            deployment_handle: Ray Serve deployment handle used to process tasks.
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
            task_timeout=config.task_timeout,
            dispatcher_interval=config.dispatcher_interval,
            log_level=config.log_level,
        )

        self.active = False
        self.last_heartbeat = datetime.datetime.now(datetime.timezone.utc)
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._dispatch_loop_task: Optional[asyncio.Task[None]] = None
        self._runtime_lock = asyncio.Lock()

        _log.setLevel(self.config.log_level.upper())
        _log.info("RayTaskDispatcher initialized")

    async def refresh_runtime(
        self,
        deployment_handle: Any,
        config: RayOrchestratorConfig,
    ) -> None:
        """Refresh Serve handle and runtime-derived settings after API startup."""
        async with self._runtime_lock:
            self.deployment_handle = deployment_handle
            self.config = config

            # Processing keys must outlive the typical task runtime so the
            # dispatcher can tell whether a STARTED task still has a live worker.
            if config.task_timeout is not None:
                self.redis_manager.processing_ttl = max(
                    int(config.task_timeout) + 300,
                    300,
                )
            else:
                self.redis_manager.processing_ttl = max(
                    self.redis_manager.results_ttl,
                    7200,
                )

            # Treat 3 missed dispatch intervals as a stale dispatcher heartbeat.
            self.redis_manager.dispatcher_heartbeat_ttl = max(
                math.ceil(config.dispatcher_interval * 3),
                1,
            )
            _log.setLevel(self.config.log_level.upper())

    async def get_health(self) -> bool:
        """Ensure the dispatch loop is running and report health."""
        async with self._runtime_lock:
            await self._ensure_dispatch_loop_started_locked()
            return self._dispatch_loop_running()

    async def stop_dispatching(self) -> None:
        """Explicit test-only shutdown for the detached dispatcher actor."""
        async with self._runtime_lock:
            self.active = False
            dispatch_loop_task = self._dispatch_loop_task
            self._dispatch_loop_task = None

        if dispatch_loop_task is not None:
            dispatch_loop_task.cancel()
            try:
                await dispatch_loop_task
            except asyncio.CancelledError:
                pass

        await self._cancel_background_tasks()
        await self.redis_manager.disconnect()

    async def get_heartbeat(self) -> datetime.datetime:
        """Get the last dispatcher heartbeat timestamp for health monitoring."""
        return self.last_heartbeat

    async def is_active(self) -> bool:
        """Check whether the background dispatch loop is currently running."""
        return self._dispatch_loop_running()

    async def _ensure_dispatch_loop_started_locked(self) -> None:
        if self._dispatch_loop_running():
            return

        await self.redis_manager.connect()
        self.active = True
        self._dispatch_loop_task = asyncio.create_task(self._run_dispatch_loop())
        _log.info("Started Ray dispatcher background loop")

    def _dispatch_loop_running(self) -> bool:
        return (
            self._dispatch_loop_task is not None and not self._dispatch_loop_task.done()
        )

    async def _run_dispatch_loop(self) -> None:
        """Run the long-lived dispatcher loop with heartbeat and reconciliation."""
        round_count = 0
        current_task = asyncio.current_task()

        try:
            await self._reconcile_active_tasks()

            while self.active:
                round_count += 1
                self.last_heartbeat = datetime.datetime.now(datetime.timezone.utc)

                if self.config.enable_heartbeat:
                    await self.redis_manager.update_dispatcher_heartbeat()

                if round_count % 10 == 0:
                    await self._log_dispatcher_stats()

                await self._dispatch_round()
                await asyncio.sleep(self.config.dispatcher_interval)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log.error("Dispatcher loop crashed: %s", exc, exc_info=True)
        finally:
            await self._cancel_background_tasks()
            self.active = False
            if self._dispatch_loop_task is current_task:
                self._dispatch_loop_task = None

    async def _cancel_background_tasks(self) -> None:
        background_tasks = list(self._background_tasks)
        if not background_tasks:
            return

        for task in background_tasks:
            task.cancel()

        await asyncio.gather(*background_tasks, return_exceptions=True)
        self._background_tasks.clear()

    async def _log_dispatcher_stats(self) -> None:
        """Log a compact view of tenant scheduling pressure."""
        tenants = await self.redis_manager.get_all_tenants_with_tasks()

        _log.debug("=" * 60)
        _log.debug("[DISPATCHER-STATS] Current State:")

        total_active = 0
        total_queued = 0

        for tenant_id in tenants:
            active_count = await self.redis_manager.get_tenant_active_task_count(
                tenant_id
            )
            limits = await self.redis_manager.get_tenant_limits(tenant_id)
            queue_size = await self.redis_manager.get_tenant_queue_size(tenant_id)

            total_active += active_count
            total_queued += queue_size

            _log.debug(
                "  Tenant %s: active=%s/%s, queued=%s",
                tenant_id,
                active_count,
                limits.max_concurrent_tasks,
                queue_size,
            )

        _log.debug(
            "  TOTAL: active=%s, queued=%s, tenants=%s",
            total_active,
            total_queued,
            len(tenants),
        )
        _log.debug("=" * 60)

    async def _dispatch_round(self) -> None:
        """Execute one round of fair tenant dispatching.

        Algorithm:
        1. Reconcile Redis active-task state against live processing keys.
        2. Discover all tenants with queued work.
        3. For each tenant in round-robin order:
           a. Read current active-task usage from Redis.
           b. Launch tasks until the tenant reaches its concurrency limit.
           c. Skip the tenant if it is already at capacity or no task can be dispatched.

        Reconciliation intentionally only fails tasks that had already reached
        STARTED but no longer have processing state. Tasks still sitting in
        Ray Serve's backlog may remain in "dispatched" state for a long time,
        so they are left unresolved rather than being aged out heuristically.
        """
        await self._reconcile_active_tasks()

        tenants = await self.redis_manager.get_all_tenants_with_tasks()
        if not tenants:
            _log.debug("[DISPATCH-ROUND] No tenants with pending tasks")
            return

        _log.debug("[DISPATCH-ROUND] Starting: %s tenants with tasks", len(tenants))

        for tenant_id in tenants:
            try:
                active_count = await self.redis_manager.get_tenant_active_task_count(
                    tenant_id
                )
                limits = await self.redis_manager.get_tenant_limits(tenant_id)
                queue_size = await self.redis_manager.get_tenant_queue_size(tenant_id)

                _log.debug(
                    "[DISPATCH-TENANT] %s: active=%s/%s, queued=%s, capacity=%s",
                    tenant_id,
                    active_count,
                    limits.max_concurrent_tasks,
                    queue_size,
                    limits.max_concurrent_tasks - active_count,
                )

                tasks_launched = 0
                while active_count < limits.max_concurrent_tasks and queue_size > 0:
                    dispatched = await self._dispatch_tenant_task(tenant_id)
                    if not dispatched:
                        break

                    tasks_launched += 1
                    active_count += 1
                    queue_size -= 1

                if tasks_launched > 0:
                    _log.debug(
                        "[DISPATCH-TENANT] %s: launched %s tasks this round",
                        tenant_id,
                        tasks_launched,
                    )
            except Exception as exc:
                _log.error(
                    "Error dispatching tasks for tenant %s: %s",
                    tenant_id,
                    exc,
                    exc_info=True,
                )

        _log.debug("[DISPATCH-ROUND] Completed")

    async def _dispatch_tenant_task(self, tenant_id: str) -> bool:
        """Dispatch one task for a tenant using Redis as the source of truth.

        Args:
            tenant_id: Tenant identifier.

        Returns:
            True if a task was dispatched, False otherwise.
        """
        task = await self.redis_manager.peek_task(tenant_id)
        if task is None:
            _log.debug("[DISPATCH] Tenant %s: no tasks in queue", tenant_id)
            return False

        task_size = len(task.sources)
        can_process, reason = await self.redis_manager.check_tenant_can_process(
            tenant_id, task_size
        )
        if not can_process:
            _log.debug("[DISPATCH] Tenant %s: skip - %s", tenant_id, reason)
            return False

        success = await self.redis_manager.dispatch_task_atomic(
            tenant_id, task.task_id, task_size
        )
        if not success:
            _log.warning(
                "[DISPATCH] Tenant %s: failed atomic dispatch for %s",
                tenant_id,
                task.task_id,
            )
            return False

        # Launch task asynchronously (fire-and-forget). The durable task state is
        # already in Redis, so dispatcher restarts do not lose ownership metadata.
        background_task = asyncio.create_task(self._process_task_async(task, tenant_id))
        # Store a strong reference to prevent premature garbage collection.
        self._background_tasks.add(background_task)
        background_task.add_done_callback(self._background_tasks.discard)

        _log.info(
            "[DISPATCH] Tenant %s: launched task %s (%s docs)",
            tenant_id,
            task.task_id,
            task_size,
        )
        return True

    async def _process_task_async(self, task: Task, tenant_id: str) -> None:
        """Process a dispatched task while keeping status durable in Redis.

        This coroutine is intentionally fire-and-forget from the dispatch loop.
        All ownership, status, and cleanup state needed for restart recovery is
        persisted in Redis before and during execution.
        """
        task_id = task.task_id
        task_size = len(task.sources)

        try:
            _log.info("[TASK-START] %s: processing %s documents", task_id, task_size)

            response = self.deployment_handle.process_task.remote(task)
            await asyncio.to_thread(
                response.result,
                timeout_s=self.config.task_timeout,
                _skip_asyncio_check=True,
            )
            _log.info(
                "[TASK-SUCCESS] %s: replica completed; durable success is replica-owned",
                task_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error_message = str(exc) or exc.__class__.__name__
            _log.error("[TASK-FAILURE] %s: %s", task_id, error_message, exc_info=True)

            terminalization = await self.redis_manager.finalize_task_failure_atomic(
                tenant_id=tenant_id,
                task_id=task_id,
                task_size=task_size,
                error_message=error_message,
            )
            if (
                terminalization.status_changed
                and terminalization.final_status == TaskStatus.FAILURE
            ):
                await self.redis_manager.publish_update(
                    TaskUpdate(
                        task_id=task_id,
                        task_status=TaskStatus.FAILURE,
                        error_message=error_message,
                    )
                )
                await self.redis_manager.update_tenant_stats(
                    tenant_id,
                    delta_total_tasks=1,
                    delta_total_documents=task_size,
                    delta_failed_documents=task_size,
                )
            elif terminalization.final_status == TaskStatus.SUCCESS:
                _log.info(
                    "[TASK-FAILURE] %s: preserving existing durable SUCCESS",
                    task_id,
                )

    async def _reconcile_active_tasks(self) -> None:
        """Reconcile active-task bookkeeping after dispatcher startup and per round.

        This replaces the old stale-heartbeat orphan recovery path. The current
        rule is intentionally conservative:
        - STARTED tasks missing processing state are failed and released.
        - Pre-start dispatched tasks are left unresolved because Ray Serve may
          legitimately keep them queued for a long time.
        """
        tenants = await self.redis_manager.get_all_tenants_with_active_tasks()
        for tenant_id in tenants:
            await self._reconcile_tenant_active_tasks(tenant_id)

    async def _reconcile_tenant_active_tasks(self, tenant_id: str) -> None:
        """Reconcile one tenant's active-task set against durable task metadata."""
        active_task_ids = await self.redis_manager.get_tenant_active_task_ids(tenant_id)
        if not active_task_ids:
            await self.redis_manager.resync_tenant_limits(tenant_id)
            return

        for task_id in active_task_ids:
            metadata = await self.redis_manager.get_task_metadata_model(task_id)
            dispatch_hash = await self.redis_manager.get_task_dispatch_hash(task_id)

            if not dispatch_hash:
                if metadata is None or metadata.status != TaskStatus.STARTED:
                    # No processing state + non-STARTED metadata: already terminal or
                    # was never properly dispatched. No action needed.
                    continue
                await self._fail_reconciled_task(
                    tenant_id=tenant_id,
                    task_id=task_id,
                    metadata=metadata,
                    error_message="Task orphaned: processing state missing during reconciliation",
                )
                continue

            # D3-owned durable SUCCESS is the terminal fence. Reconciliation only
            # applies stale-heartbeat failure logic to tasks that are still
            # durably STARTED; once durable status has moved to SUCCESS or
            # FAILURE, this path must leave the task alone.
            if metadata is None or metadata.status != TaskStatus.STARTED:
                continue

            execution_lease = await self.redis_manager.get_task_execution_lease(task_id)
            if execution_lease is None:
                # No lease written yet: narrow window between dispatch and replica claim,
                # or an old in-flight task from before this code was deployed.
                # Leave unresolved — conservative, no false positives.
                continue

            heartbeat_at_raw = execution_lease.get("heartbeat_at")
            if heartbeat_at_raw is None:
                # Lease exists but no heartbeat field — should not happen with current code.
                # Leave unresolved rather than risk a false positive.
                continue

            try:
                heartbeat_age = datetime.datetime.now(
                    datetime.timezone.utc
                ).timestamp() - float(heartbeat_at_raw)
            except ValueError:
                _log.warning(
                    "[RECONCILE] %s: invalid execution lease heartbeat %r",
                    task_id,
                    heartbeat_at_raw,
                )
                continue

            if heartbeat_age > self._get_task_processing_stale_after():
                await self._fail_reconciled_task(
                    tenant_id=tenant_id,
                    task_id=task_id,
                    metadata=metadata,
                    error_message=(
                        "Task orphaned: replica execution lease stale during reconciliation"
                    ),
                )

        await self.redis_manager.resync_tenant_limits(tenant_id)

    async def _fail_reconciled_task(
        self,
        tenant_id: str,
        task_id: str,
        metadata: RedisTaskMetadata,
        error_message: str,
    ) -> None:
        """Fail a reconciled task and release any capacity it still consumes."""
        task_size = metadata.task_size if metadata.task_size > 0 else 1
        if task_size == 1 and metadata.task_size <= 0:
            _log.warning(
                "[RECONCILE] Missing durable task_size for %s; falling back to 1",
                task_id,
            )

        _log.warning("[RECONCILE] %s: %s", task_id, error_message)

        terminalization = await self.redis_manager.finalize_task_failure_atomic(
            tenant_id=tenant_id,
            task_id=task_id,
            task_size=task_size,
            error_message=error_message,
        )
        if (
            terminalization.status_changed
            and terminalization.final_status == TaskStatus.FAILURE
        ):
            await self.redis_manager.publish_update(
                TaskUpdate(
                    task_id=task_id,
                    task_status=TaskStatus.FAILURE,
                    error_message=error_message,
                )
            )

    def _get_task_processing_stale_after(self) -> float:
        """Return the stale cutoff for task heartbeats.

        A fixed multiplier keeps configuration simple and avoids hidden coupling
        between heartbeat cadence and stale-task reconciliation.
        """
        return self.config.heartbeat_interval * 4

    async def get_stats(self) -> dict[str, Any]:
        """Get comprehensive dispatcher statistics.

        Returns:
            Dictionary with dispatcher stats including per-tenant details.
        """
        tenants = await self.redis_manager.get_all_tenants_with_tasks()

        total_active_tasks = 0
        total_queued_tasks = 0
        total_capacity_available = 0
        tenant_details = []

        for tenant_id in tenants:
            active_count = await self.redis_manager.get_tenant_active_task_count(
                tenant_id
            )
            limits = await self.redis_manager.get_tenant_limits(tenant_id)
            queue_size = await self.redis_manager.get_tenant_queue_size(tenant_id)

            total_active_tasks += active_count
            total_queued_tasks += queue_size
            capacity_available = limits.max_concurrent_tasks - active_count
            total_capacity_available += capacity_available

            utilization_pct = (
                (active_count / limits.max_concurrent_tasks * 100)
                if limits.max_concurrent_tasks > 0
                else 0
            )

            tenant_details.append(
                {
                    "tenant_id": tenant_id,
                    "active_tasks": active_count,
                    "max_concurrent_tasks": limits.max_concurrent_tasks,
                    "queued_tasks": queue_size,
                    "capacity_available": capacity_available,
                    "utilization_pct": round(utilization_pct, 1),
                }
            )

        deployment_stats = {
            "min_replicas": self.config.min_actors,
            "max_replicas": self.config.max_actors,
            "target_requests_per_replica": self.config.target_requests_per_replica,
        }

        return {
            "active": self._dispatch_loop_running(),
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "tenants_with_tasks": len(tenants),
            "total_active_tasks": total_active_tasks,
            "total_queued_tasks": total_queued_tasks,
            "total_capacity_available": total_capacity_available,
            "tenant_details": tenant_details,
            "ray_serve_deployment": deployment_stats,
            "config": {
                "dispatcher_interval": self.config.dispatcher_interval,
                "max_concurrent_tasks": self.config.max_concurrent_tasks,
                "max_queued_tasks": self.config.max_queued_tasks,
            },
        }
