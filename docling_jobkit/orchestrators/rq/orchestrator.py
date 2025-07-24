import logging
import multiprocessing
import os
import uuid
from subprocess import Popen
from typing import Optional

from pydantic import BaseModel
from redis import Redis
from rq import Queue, Worker
from rq.job import Job, JobStatus

from docling.datamodel.base_models import InputFormat

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.task import Task, TaskSource, TaskTarget
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.base_orchestrator import BaseOrchestrator
from docling_jobkit.orchestrators.rq.worker import CustomRQWorker, conversion_task

_log = logging.getLogger(__name__)


def run_worker(conversion_manager_config):
    # create a new connection in thread, Redis and ConversionManager are not pickle
    redis_conn = Redis(
        host=os.environ.get("DOCLING_SERVE_eng_rq_host", "localhost"),
        port=os.environ.get("DOCLING_SERVE_eng_rq_port", 6379),
    )
    queue = Queue(name="conversion_queue", connection=redis_conn, default_timeout=14400)
    cm = DoclingConverterManager(config=conversion_manager_config)
    pdf_format_option = cm.get_pdf_pipeline_opts(ConvertDocumentsOptions())
    converter = cm.get_converter(pdf_format_option)
    converter.initialize_pipeline(InputFormat.PDF)
    worker = CustomRQWorker([queue], conversion_manager=cm, connection=redis_conn)
    worker.work()


class RQOrchestratorConfig(BaseModel):
    num_workers: int = 2


class RQOrchestrator(BaseOrchestrator):
    def __init__(
        self,
        config: RQOrchestratorConfig,
        converter_manager_config: DoclingConverterManagerConfig,
        api_only=False,
    ):
        super().__init__()
        self.config = config
        self.api_only = api_only
        self.worker_processes: list[Popen] = []
        self.redis_conn = Redis(
            host=os.environ.get("DOCLING_SERVE_eng_rq_host", "localhost"),
            port=os.environ.get("DOCLING_SERVE_eng_rq_port", 6379),
        )
        self.task_queue = Queue(
            "conversion_queue", connection=self.redis_conn, default_timeout=14400
        )
        self.cm_conf = converter_manager_config

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
        if not self.api_only:
            for i in range(self.config.num_workers):
                _log.info(f"Starting worker {i}")
                multiprocessing.Process(target=run_worker, args=(self.cm_conf,)).start()

    async def warm_up_caches(self):
        pass

    async def check_connection(self):
        # Check redis connection is up
        try:
            self.redis_conn.ping()
        except Exception:
            raise RuntimeError("No connection to Redis")

        if not self.api_only:
            # Count the number of workers in redis connection
            workers = Worker.count(connection=self.redis_conn)
            if workers == 0:
                raise RuntimeError("No workers connected to Redis")

    async def clear_converters(self):
        pass
