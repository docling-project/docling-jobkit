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
    """Ray Task Dispatcher - Round-robin scheduling at TASK level."""

    def __init__(
        self,
        config: RayOrchestratorConfig,
        deployment_handle: Any,
    ) -> None:
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
        return self.last_heartbeat

    async def is_active(self) -> bool:
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

        background_task = asyncio.create_task(self._process_task_async(task, tenant_id))
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
        task_id = task.task_id
        task_size = len(task.sources)

        try:
            _log.info("[TASK-START] %s: processing %s documents", task_id, task_size)
            await self.redis_manager.set_task_dispatch_state(task_id, "dispatched")

            result = await self.deployment_handle.process_task.remote(task)
            result_key = await self.redis_manager.store_task_result(task_id, result)

            await self.redis_manager.update_task_status(task_id, TaskStatus.SUCCESS)
            await self.redis_manager.set_task_dispatch_state(task_id, None)
            await self.redis_manager.publish_update(
                TaskUpdate(
                    task_id=task_id,
                    task_status=TaskStatus.SUCCESS,
                    result_key=result_key,
                    progress=None,
                )
            )

            await self.redis_manager.update_tenant_stats(
                tenant_id,
                delta_total_tasks=1,
                delta_total_documents=task_size,
                delta_successful_documents=result.num_succeeded,
                delta_failed_documents=result.num_failed,
            )

            _log.info("[TASK-SUCCESS] %s: completed successfully", task_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error_message = str(exc)
            _log.error("[TASK-FAILURE] %s: %s", task_id, error_message, exc_info=True)

            await self.redis_manager.update_task_status(
                task_id,
                TaskStatus.FAILURE,
                error_message=error_message,
            )
            await self.redis_manager.set_task_dispatch_state(task_id, None)
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
        finally:
            await self.redis_manager.complete_task_atomic(tenant_id, task_id, task_size)
            _log.info(
                "[TASK-CLEANUP] %s: released capacity for tenant %s",
                task_id,
                tenant_id,
            )

    async def _reconcile_active_tasks(self) -> None:
        tenants = await self.redis_manager.get_all_tenants_with_active_tasks()
        for tenant_id in tenants:
            await self._reconcile_tenant_active_tasks(tenant_id)

    async def _reconcile_tenant_active_tasks(self, tenant_id: str) -> None:
        active_task_ids = await self.redis_manager.get_tenant_active_task_ids(tenant_id)
        if not active_task_ids:
            await self.redis_manager.resync_tenant_limits(tenant_id)
            return

        now_timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()

        for task_id in active_task_ids:
            metadata = await self.redis_manager.get_task_metadata_model(task_id)
            processing_state = await self.redis_manager.get_task_processing_state(
                task_id
            )

            if not processing_state:
                await self._fail_reconciled_task(
                    tenant_id=tenant_id,
                    task_id=task_id,
                    metadata=metadata,
                    error_message="Task orphaned: processing state missing during reconciliation",
                )
                continue

            processing_status = processing_state.get("status")
            if processing_status == "dispatched":
                dispatched_at_raw = processing_state.get("dispatched_at")
                dispatched_at = float(dispatched_at_raw) if dispatched_at_raw else 0.0
                if (
                    now_timestamp - dispatched_at
                    > self.config.dispatcher_handoff_timeout
                ):
                    await self._fail_reconciled_task(
                        tenant_id=tenant_id,
                        task_id=task_id,
                        metadata=metadata,
                        error_message=(
                            "Task dispatch handoff timed out during reconciliation"
                        ),
                    )

        await self.redis_manager.resync_tenant_limits(tenant_id)

    async def _fail_reconciled_task(
        self,
        tenant_id: str,
        task_id: str,
        metadata: Optional[RedisTaskMetadata],
        error_message: str,
    ) -> None:
        task_size = self._task_size_for_cleanup(task_id, metadata)

        _log.warning("[RECONCILE] %s: %s", task_id, error_message)

        await self.redis_manager.update_task_status(
            task_id,
            TaskStatus.FAILURE,
            error_message=error_message,
        )
        await self.redis_manager.set_task_dispatch_state(task_id, None)
        await self.redis_manager.publish_update(
            TaskUpdate(
                task_id=task_id,
                task_status=TaskStatus.FAILURE,
                error_message=error_message,
            )
        )
        await self.redis_manager.complete_task_atomic(tenant_id, task_id, task_size)

    @staticmethod
    def _task_size_for_cleanup(
        task_id: str, metadata: Optional[RedisTaskMetadata]
    ) -> int:
        if metadata is not None and metadata.task_size > 0:
            return metadata.task_size

        _log.warning(
            "[RECONCILE] Missing durable task_size for %s; falling back to 1",
            task_id,
        )
        return 1

    async def get_stats(self) -> dict[str, Any]:
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
            "loop_running": self._dispatch_loop_running(),
            "total_active_tasks": total_active_tasks,
            "total_queued_tasks": total_queued_tasks,
            "total_capacity_available": total_capacity_available,
            "tenants": tenant_details,
            "deployment": deployment_stats,
        }
