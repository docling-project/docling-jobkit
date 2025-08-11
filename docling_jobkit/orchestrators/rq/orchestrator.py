import logging
import uuid
from pathlib import Path
from typing import Optional

import msgpack
from pydantic import BaseModel
from redis import Redis
from redis.asyncio import Redis as AsyncRedis
from rq import Queue
from rq.job import Job, JobStatus

from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.result import ConvertDocumentResult
from docling_jobkit.datamodel.task import Task, TaskSource, TaskTarget
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.base_orchestrator import BaseOrchestrator

# from docling_jobkit.orchestrators.rq.worker import conversion_task

_log = logging.getLogger(__name__)


class RQOrchestratorConfig(BaseModel):
    redis_host: str = "localhost"
    redis_port: int = 6379
    results_ttl: int = 3_600 * 4
    results_prefix: str = "docling:results"
    scratch_dir: Optional[Path] = None


class RQOrchestrator(BaseOrchestrator):
    @staticmethod
    def make_rq_queue(config: RQOrchestratorConfig) -> tuple[Redis, Queue]:
        conn = Redis(
            host=config.redis_host,
            port=config.redis_port,
        )
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
        self._async_redis_conn = AsyncRedis(
            host=self.config.redis_host,
            port=self.config.redis_port,
        )
        self._task_result_keys: dict[str, str] = {}

    async def notify_end_job(self, task_id):
        # TODO: check if this is necessary
        pass

    async def enqueue(
        self,
        sources: list[TaskSource],
        options: ConvertDocumentsOptions,
        target: TaskTarget,
    ) -> Task:
        task_id = str(uuid.uuid4())
        task = Task(task_id=task_id, sources=sources, options=options, target=target)
        self.tasks.update({task.task_id: task})
        task_data = task.model_dump(mode="json")
        self._rq_queue.enqueue(
            "docling_jobkit.orchestrators.rq.worker.conversion_task",
            kwargs={"task_data": task_data},
            job_id=task_id,
            timeout=14400,
        )
        await self.init_task_tracking(task)

        return task

    async def queue_size(self) -> int:
        return self._rq_queue.count

    async def _update_task_from_run(self, task_id: str) -> None:
        task = await self.get_raw_task(task_id=task_id)
        if task.is_completed():
            return

        print("GOING TO UPDATE TASK...")

        job = Job.fetch(task_id, connection=self._redis_conn)
        status = job.get_status()
        print(f"{status=}")

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
        await self._update_task_from_run(task_id=task_id)
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
    ) -> Optional[ConvertDocumentResult]:
        if task_id not in self._task_result_keys:
            return None
        result_key = self._task_result_keys[task_id]
        packed = await self._async_redis_conn.get(result_key)
        result = ConvertDocumentResult.model_validate(
            msgpack.unpackb(packed, raw=False)
        )
        return result

    async def process_queue(self):
        pass

    async def warm_up_caches(self):
        pass

    async def check_connection(self):
        # Check redis connection is up
        try:
            self._redis_conn.ping()
        except Exception:
            raise RuntimeError("No connection to Redis")

    async def clear_converters(self):
        pass
