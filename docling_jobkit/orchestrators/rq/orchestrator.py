import asyncio
import base64
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
    results_prefix: str = "docling:results"
    sub_channel: str = "docling:updates"
    scratch_dir: Optional[Path] = None


class _TaskUpdate(BaseModel):
    task_id: str
    task_status: TaskStatus
    result_key: Optional[str] = None


class RQOrchestrator(BaseOrchestrator):
    @staticmethod
    def make_rq_queue(config: RQOrchestratorConfig) -> tuple[redis.Redis, Queue]:
        conn = redis.from_url(config.redis_url)
        rq_queue = Queue(
            "convert",
            connection=conn,
            default_timeout=14400,
            result_ttl=config.results_ttl,
        )
        return conn, rq_queue

    def __init__(
        self,
        config: RQOrchestratorConfig,
    ):
        super().__init__()
        self.config = config
        self._redis_conn, self._rq_queue = self.make_rq_queue(self.config)
        self._async_redis_conn = async_redis.from_url(self.config.redis_url)
        self._task_result_keys: dict[str, str] = {}

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
                    # Update the results lookup
                    if (
                        data.task_status == TaskStatus.SUCCESS
                        and data.result_key is not None
                    ):
                        self._task_result_keys[data.task_id] = data.result_key

                    if self.notifier:
                        try:
                            await self.notifier.notify_task_subscribers(task.task_id)
                            await self.notifier.notify_queue_positions()
                        except Exception as e:
                            _log.error(f"Notifier error for task {data.task_id}: {e}")

                except TaskNotFoundError:
                    _log.warning(f"Task {data.task_id} not found.")

    async def process_queue(self):
        # Create a pool of workers
        pubsub_worker = []
        _log.debug("PubSub worker starting.")
        pubsub_worker = asyncio.create_task(self._listen_for_updates())

        # Wait for all worker to complete
        await asyncio.gather(pubsub_worker)
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
