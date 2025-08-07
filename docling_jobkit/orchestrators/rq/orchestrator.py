import logging
import uuid
from subprocess import Popen
from typing import Optional

from pydantic import BaseModel
from redis import Redis
from rq import Queue
from rq.job import Job, JobStatus

from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.task import Task, TaskSource, TaskTarget
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.base_orchestrator import BaseOrchestrator
from docling_jobkit.orchestrators.rq.worker import conversion_task

_log = logging.getLogger(__name__)


class RQOrchestratorConfig(BaseModel):
    redis_host: str = "localhost"
    redis_port: int = 6379


class RQOrchestrator(BaseOrchestrator):
    def __init__(
        self,
        config: RQOrchestratorConfig,
    ):
        super().__init__()
        self.config = config
        self.worker_processes: list[Popen] = []
        self.redis_conn = Redis(
            host=self.config.redis_host,
            port=self.config.redis_port,
        )
        self.task_queue = Queue(
            "conversion_queue", connection=self.redis_conn, default_timeout=14400
        )

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
        self.task_queue.enqueue(
            conversion_task,
            kwargs={"task_data": task_data},
            job_id=task_id,
            timeout=14400,
        )
        await self.init_task_tracking(task)

        return task

    async def queue_size(self) -> int:
        return self.task_queue.count

    async def get_queue_position(self, task_id: str) -> Optional[int]:
        try:
            # On fetching Job to get queue position, we also get the status
            # in order to keep the status updated in the tasks list
            job = Job.fetch(task_id, connection=self.redis_conn)
            status = job.get_status()
            queue_pos = job.get_position()
            if status == JobStatus.FINISHED:
                task = self.tasks[task_id]
                task.task_status = TaskStatus.SUCCESS
                task.results = job.return_value()
                self.tasks.update({task.task_id: task})
            elif status == JobStatus.QUEUED or status == JobStatus.SCHEDULED:
                task = self.tasks[task_id]
                task.task_status = TaskStatus.PENDING
                self.tasks.update({task.task_id: task})
            elif status == JobStatus.STARTED:
                task = self.tasks[task_id]
                task.task_status = TaskStatus.STARTED
                self.tasks.update({task.task_id: task})
            else:
                task = self.tasks[task_id]
                task.task_status = TaskStatus.FAILURE
                self.tasks.update({task.task_id: task})
            return queue_pos + 1 if queue_pos is not None else 0
        except Exception as e:
            _log.error("An error occour getting queue position.", exc_info=e)
            return None

    async def process_queue(self):
        pass

    async def warm_up_caches(self):
        pass

    async def check_connection(self):
        # Check redis connection is up
        try:
            self.redis_conn.ping()
        except Exception:
            raise RuntimeError("No connection to Redis")

    async def clear_converters(self):
        pass
