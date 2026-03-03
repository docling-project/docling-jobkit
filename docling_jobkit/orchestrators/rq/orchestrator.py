import asyncio
import base64
import datetime
import logging
import uuid
import warnings
from pathlib import Path
from typing import Optional

import msgpack
import redis
import redis.asyncio as async_redis
from pydantic import BaseModel
from rq import Queue
from rq.job import Job, JobStatus
from rq.registry import StartedJobRegistry

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.datamodel.chunking import BaseChunkerOptions, ChunkingExportOptions
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
from docling_jobkit.datamodel.result import DoclingTaskResult
from docling_jobkit.datamodel.task import Task, TaskSource, TaskTarget
from docling_jobkit.datamodel.task_meta import TaskStatus, TaskType
from docling_jobkit.orchestrators.base_orchestrator import (
    BaseOrchestrator,
    TaskNotFoundError,
)

_log = logging.getLogger(__name__)


class RQOrchestratorConfig(BaseModel):
    redis_url: str = "redis://localhost:6379/"
    results_ttl: int = 3_600 * 4
    failure_ttl: int = 3_600 * 4
    results_prefix: str = "docling:results"
    sub_channel: str = "docling:updates"
    scratch_dir: Optional[Path] = None
    redis_max_connections: int = 50
    redis_socket_timeout: Optional[float] = None
    redis_socket_connect_timeout: Optional[float] = None


class _TaskUpdate(BaseModel):
    task_id: str
    task_status: TaskStatus
    result_key: Optional[str] = None
    error_message: Optional[str] = None


_HEARTBEAT_KEY_PREFIX = "docling:job:alive"
_HEARTBEAT_TTL = 60  # seconds before an unrefreshed key expires
_HEARTBEAT_INTERVAL = 20  # seconds between heartbeat writes
_WATCHDOG_INTERVAL = 30.0  # seconds between watchdog scans
_WATCHDOG_GRACE_PERIOD = (
    90.0  # don't flag tasks started less than this many seconds ago
)


class RQOrchestrator(BaseOrchestrator):
    @staticmethod
    def make_rq_queue(config: RQOrchestratorConfig) -> tuple[redis.Redis, Queue]:
        # Create connection pool with configurable size
        pool = redis.ConnectionPool.from_url(
            config.redis_url,
            max_connections=config.redis_max_connections,
            socket_timeout=config.redis_socket_timeout,
            socket_connect_timeout=config.redis_socket_connect_timeout,
        )
        conn = redis.Redis(connection_pool=pool)
        rq_queue = Queue(
            "convert",
            connection=conn,
            default_timeout=14400,
            result_ttl=config.results_ttl,
            failure_ttl=config.failure_ttl,
        )
        _log.info(
            f"RQ Redis connection pool initialized with max_connections="
            f"{config.redis_max_connections}, socket_timeout={config.redis_socket_timeout}, "
            f"socket_connect_timeout={config.redis_socket_connect_timeout}"
        )
        return conn, rq_queue

    def __init__(
        self,
        config: RQOrchestratorConfig,
    ):
        super().__init__()
        self.config = config
        self._redis_conn, self._rq_queue = self.make_rq_queue(self.config)

        # Create async connection pool with same configuration
        self._async_redis_pool = async_redis.ConnectionPool.from_url(
            self.config.redis_url,
            max_connections=config.redis_max_connections,
            socket_timeout=config.redis_socket_timeout,
            socket_connect_timeout=config.redis_socket_connect_timeout,
        )
        self._async_redis_conn = async_redis.Redis(
            connection_pool=self._async_redis_pool
        )
        self._task_result_keys: dict[str, str] = {}
        _log.info(
            f"RQ async Redis connection pool initialized with max_connections="
            f"{config.redis_max_connections}"
        )

    async def notify_end_job(self, task_id):
        # TODO: check if this is necessary
        pass

    async def enqueue(
        self,
        sources: list[TaskSource],
        target: TaskTarget,
        task_type: TaskType = TaskType.CONVERT,
        options: ConvertDocumentsOptions | None = None,
        convert_options: ConvertDocumentsOptions | None = None,
        chunking_options: BaseChunkerOptions | None = None,
        chunking_export_options: ChunkingExportOptions | None = None,
    ) -> Task:
        if options is not None and convert_options is None:
            convert_options = options
            warnings.warn(
                "'options' is deprecated and will be removed in a future version. "
                "Use 'conversion_options' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        task_id = str(uuid.uuid4())
        rq_sources: list[HttpSource | FileSource] = []
        for source in sources:
            if isinstance(source, DocumentStream):
                encoded_doc = base64.b64encode(source.stream.read()).decode()
                rq_sources.append(
                    FileSource(filename=source.name, base64_string=encoded_doc)
                )
            elif isinstance(source, (HttpSource | FileSource)):
                rq_sources.append(source)
        chunking_export_options = chunking_export_options or ChunkingExportOptions()
        task = Task(
            task_id=task_id,
            task_type=task_type,
            sources=rq_sources,
            convert_options=convert_options,
            chunking_options=chunking_options,
            chunking_export_options=chunking_export_options,
            target=target,
        )
        self.tasks.update({task.task_id: task})
        task_data = task.model_dump(mode="json", serialize_as_any=True)
        self._rq_queue.enqueue(
            "docling_jobkit.orchestrators.rq.worker.docling_task",
            kwargs={"task_data": task_data},
            job_id=task_id,
            timeout=14400,
            failure_ttl=self.config.failure_ttl,
        )
        await self.init_task_tracking(task)

        return task

    async def queue_size(self) -> int:
        return self._rq_queue.count

    async def _update_task_from_rq(self, task_id: str) -> None:
        task = await self.get_raw_task(task_id=task_id)
        if task.is_completed():
            return

        job = Job.fetch(task_id, connection=self._redis_conn)
        status = job.get_status()

        if status == JobStatus.FINISHED:
            result = job.latest_result()
            if result is not None and result.type == result.Type.SUCCESSFUL:
                task.set_status(TaskStatus.SUCCESS)
                task_result_key = str(result.return_value)
                self._task_result_keys[task_id] = task_result_key
            else:
                task.set_status(TaskStatus.FAILURE)

        elif status in (
            JobStatus.QUEUED,
            JobStatus.SCHEDULED,
            JobStatus.STOPPED,
            JobStatus.DEFERRED,
        ):
            task.set_status(TaskStatus.PENDING)
        elif status == JobStatus.STARTED:
            task.set_status(TaskStatus.STARTED)
        else:
            task.set_status(TaskStatus.FAILURE)

    async def task_status(self, task_id: str, wait: float = 0.0) -> Task:
        await self._update_task_from_rq(task_id=task_id)
        return await self.get_raw_task(task_id=task_id)

    async def get_queue_position(self, task_id: str) -> Optional[int]:
        try:
            job = Job.fetch(task_id, connection=self._redis_conn)
            queue_pos = job.get_position()
            return queue_pos + 1 if queue_pos is not None else None
        except Exception as e:
            _log.error("An error occour getting queue position.", exc_info=e)
            return None

    async def task_result(
        self,
        task_id: str,
    ) -> Optional[DoclingTaskResult]:
        if task_id not in self._task_result_keys:
            return None
        result_key = self._task_result_keys[task_id]
        packed = await self._async_redis_conn.get(result_key)
        result = DoclingTaskResult.model_validate(
            msgpack.unpackb(packed, raw=False, strict_map_key=False)
        )
        return result

    async def _on_task_status_changed(self, task: Task) -> None:
        """Called after every in-memory status update from pub/sub.

        No-op by default. Subclasses should override to persist the updated
        task to durable storage so that terminal states survive pod restarts
        and are visible to pods that missed the pub/sub event.
        """

    async def _listen_for_updates(self):
        pubsub = self._async_redis_conn.pubsub()

        # Subscribe to a single channel
        await pubsub.subscribe(self.config.sub_channel)

        _log.debug("Listening for updates...")

        # Listen for messages
        async for message in pubsub.listen():
            if message["type"] == "message":
                data = _TaskUpdate.model_validate_json(message["data"])
                try:
                    task = await self.get_raw_task(task_id=data.task_id)
                    if task.is_completed():
                        _log.debug("Task already completed. No update will be done.")
                        continue

                    # Update the status
                    task.set_status(data.task_status)
                    # Store error message on failure
                    if (
                        data.task_status == TaskStatus.FAILURE
                        and data.error_message is not None
                    ):
                        task.error_message = data.error_message
                    # Update the results lookup
                    if (
                        data.task_status == TaskStatus.SUCCESS
                        and data.result_key is not None
                    ):
                        self._task_result_keys[data.task_id] = data.result_key

                    await self._on_task_status_changed(task)

                    if self.notifier:
                        try:
                            await self.notifier.notify_task_subscribers(task.task_id)
                            await self.notifier.notify_queue_positions()
                        except Exception as e:
                            _log.error(f"Notifier error for task {data.task_id}: {e}")

                except TaskNotFoundError:
                    _log.warning(f"Task {data.task_id} not found.")

    async def _watchdog_task(self) -> None:
        """Detect orphaned STARTED tasks whose worker heartbeat key has expired.

        Runs every _WATCHDOG_INTERVAL seconds. For each task in STARTED state
        that is older than _WATCHDOG_GRACE_PERIOD, checks whether the worker's
        liveness key (docling:job:alive:{task_id}) still exists in Redis. If the
        key is absent the worker process has died — publishes a FAILURE update to
        the pub/sub channel so that polling clients and WebSocket subscribers are
        notified within ~90 seconds of the kill instead of waiting 4 hours.

        Note: the grace period is tracked by the watchdog itself (first_seen_started)
        rather than task.started_at. Orchestrator subclasses (e.g. RedisTaskStatusMixin)
        may recreate Task objects on every poll, resetting started_at to the current
        time and making a task.started_at-based check unreliable.
        """
        _log.warning("Watchdog starting")
        # Maps task_id -> time the watchdog first observed the task in STARTED state.
        # Independent of task.started_at which may be reset by polling machinery.
        first_seen_started: dict[str, datetime.datetime] = {}
        while True:
            await asyncio.sleep(_WATCHDOG_INTERVAL)
            try:
                now = datetime.datetime.now(datetime.timezone.utc)

                all_statuses = {
                    tid: t.task_status for tid, t in list(self.tasks.items())
                }
                _log.warning(
                    f"Watchdog scan: {len(self.tasks)} tasks in memory, "
                    f"statuses={all_statuses}"
                )

                # Determine which tasks are currently in STARTED state by
                # querying StartedJobRegistry directly — the authoritative,
                # cross-pod, durable source that is independent of request
                # routing and pod lifecycle.
                registry = StartedJobRegistry(
                    queue=self._rq_queue, connection=self._redis_conn
                )
                # cleanup=False: skip RQ's own abandoned-job sweep (ZRANGEBYSCORE
                # + pipeline) — the heartbeat watchdog handles dead jobs via a
                # separate signal. Saves one Redis round trip per scan.
                rq_started_ids = await asyncio.to_thread(
                    registry.get_job_ids, cleanup=False
                )
                currently_started = set(rq_started_ids)

                # Remove tasks that are no longer STARTED (completed, failed, gone).
                for task_id in list(first_seen_started.keys()):
                    if task_id not in currently_started:
                        _log.warning(
                            f"Watchdog: task {task_id} left STARTED, removing from tracking"
                        )
                        del first_seen_started[task_id]

                # Record first observation time for newly STARTED tasks.
                for task_id in currently_started:
                    if task_id not in first_seen_started:
                        _log.warning(
                            f"Watchdog: first observation of STARTED task {task_id}"
                        )
                        first_seen_started[task_id] = now

                # Check tasks that have been STARTED long enough to be past grace period.
                candidates = [
                    task_id
                    for task_id, first_seen in list(first_seen_started.items())
                    if (now - first_seen).total_seconds() > _WATCHDOG_GRACE_PERIOD
                ]

                _log.warning(
                    f"Watchdog: {len(currently_started)} started, "
                    f"{len(first_seen_started)} tracked, "
                    f"{len(candidates)} past grace period"
                )

                for task_id in candidates:
                    key = f"{_HEARTBEAT_KEY_PREFIX}:{task_id}"
                    alive = await self._async_redis_conn.exists(key)
                    age = (now - first_seen_started[task_id]).total_seconds()
                    _log.warning(
                        f"Watchdog: checking task {task_id} "
                        f"(age={age:.0f}s), heartbeat key alive={bool(alive)}"
                    )
                    if not alive:
                        _log.warning(
                            f"Task {task_id} heartbeat key missing — "
                            f"worker likely dead, publishing FAILURE"
                        )
                        await self._async_redis_conn.publish(
                            self.config.sub_channel,
                            _TaskUpdate(
                                task_id=task_id,
                                task_status=TaskStatus.FAILURE,
                                error_message=(
                                    "Worker heartbeat expired: worker pod "
                                    "likely killed mid-job"
                                ),
                            ).model_dump_json(),
                        )
                        # Remove from tracking so we don't re-publish if pub/sub is slow.
                        del first_seen_started[task_id]
            except Exception as e:
                _log.error(f"Watchdog error: {e}")

    async def process_queue(self):
        # Create a pool of workers
        _log.debug("PubSub worker starting.")
        pubsub_task = asyncio.create_task(self._listen_for_updates())
        watchdog_task = asyncio.create_task(self._watchdog_task())

        # Wait for all workers to complete
        await asyncio.gather(pubsub_task, watchdog_task)
        _log.debug("PubSub worker completed.")

    async def delete_task(self, task_id: str):
        _log.info(f"Deleting result of task {task_id=}")

        # Delete the result data from Redis if it exists
        if task_id in self._task_result_keys:
            await self._async_redis_conn.delete(self._task_result_keys[task_id])
            del self._task_result_keys[task_id]

        # Delete the RQ job itself to free up Redis memory
        # This includes the job metadata and result stream
        try:
            job = Job.fetch(task_id, connection=self._redis_conn)
            job.delete()
            _log.debug(f"Deleted RQ job {task_id=}")
        except Exception as e:
            # Job may not exist or already be deleted - this is not an error
            _log.debug(f"Could not delete RQ job {task_id=}: {e}")

        await super().delete_task(task_id)

    async def warm_up_caches(self):
        pass

    async def check_connection(self):
        # Check redis connection is up
        try:
            self._redis_conn.ping()
        except Exception:
            raise RuntimeError("No connection to Redis")

    async def clear_converters(self):
        self._rq_queue.enqueue(
            "docling_jobkit.orchestrators.rq.worker.clear_cache_task",
        )

    async def close(self):
        """Close Redis connection pools and release resources."""
        try:
            # Close async connection pool
            await self._async_redis_conn.aclose()
            await self._async_redis_pool.aclose()
            _log.info("Async Redis connection pool closed")
        except Exception as e:
            _log.error(f"Error closing async Redis connection pool: {e}")

        try:
            # Close sync connection pool
            self._redis_conn.close()
            _log.info("Sync Redis connection pool closed")
        except Exception as e:
            _log.error(f"Error closing sync Redis connection pool: {e}")
