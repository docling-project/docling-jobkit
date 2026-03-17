import logging
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Any, Optional, Union

import msgpack
import redis as sync_redis
from rq import SimpleWorker, get_current_job

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.convert.chunking import process_chunk_results
from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.convert.results import process_export_results
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
from docling_jobkit.datamodel.result import DoclingTaskResult
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus, TaskType
from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker
from docling_jobkit.orchestrators.rq.orchestrator import (
    _HEARTBEAT_INTERVAL,
    _HEARTBEAT_TTL,
    RQOrchestrator,
    RQOrchestratorConfig,
    _TaskUpdate,
)
from docling_jobkit.orchestrators.serialization import make_msgpack_safe

_log = logging.getLogger(__name__)


class CustomRQWorker(SimpleWorker):
    def __init__(
        self,
        *args,
        orchestrator_config: RQOrchestratorConfig,
        cm_config: DoclingConverterManagerConfig,
        scratch_dir: Path,
        **kwargs,
    ):
        self.orchestrator_config = orchestrator_config
        self.conversion_manager = DoclingConverterManager(cm_config)
        self.scratch_dir = scratch_dir

        if "default_result_ttl" not in kwargs:
            kwargs["default_result_ttl"] = self.orchestrator_config.results_ttl

        # Call parent class constructor
        super().__init__(*args, **kwargs)

    def _heartbeat_loop(self, job_id: str, stop_event: threading.Event) -> None:
        """Write a liveness key to Redis every _HEARTBEAT_INTERVAL seconds.

        Runs in a daemon thread for the duration of a single job. SimpleWorker
        blocks its main thread during job execution, so the RQ-level heartbeat
        is not maintained. This thread provides the equivalent liveness signal
        that the standard forking Worker gets for free from its parent process.

        The key expires after _HEARTBEAT_TTL seconds without a refresh. If the
        worker process is killed the thread dies with it and the key expires
        naturally, allowing the orchestrator watchdog to detect the dead job.
        """
        key = f"{self.orchestrator_config.heartbeat_key_prefix}:{job_id}"
        conn = None
        try:
            conn = sync_redis.Redis.from_url(self.orchestrator_config.redis_url)
            # Write immediately so the key exists before the first watchdog scan.
            conn.set(key, "1", ex=_HEARTBEAT_TTL)
            while not stop_event.wait(timeout=_HEARTBEAT_INTERVAL):
                conn.set(key, "1", ex=_HEARTBEAT_TTL)
        except Exception as e:
            _log.error(f"Heartbeat thread error for {job_id}: {e}")
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def perform_job(self, job, queue):
        stop_event = threading.Event()
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(job.id, stop_event),
            daemon=True,
            name=f"heartbeat-{job.id}",
        )
        heartbeat_thread.start()
        try:
            # Add to job's kwargs conversion manager
            if hasattr(job, "kwargs"):
                job.kwargs["conversion_manager"] = self.conversion_manager
                job.kwargs["orchestrator_config"] = self.orchestrator_config
                job.kwargs["scratch_dir"] = self.scratch_dir

            return super().perform_job(job, queue)
        except Exception as e:
            # Custom error handling for individual jobs
            self.log.error(f"Job {job.id} failed: {e}")
            raise
        finally:
            stop_event.set()
            heartbeat_thread.join(timeout=5)


def docling_task(
    task_data: dict,
    conversion_manager: DoclingConverterManager,
    orchestrator_config: RQOrchestratorConfig,
    scratch_dir: Path,
):
    _log.debug("started task")
    task = Task.model_validate(task_data)
    task_id = task.task_id

    job = get_current_job()
    assert job is not None
    conn = job.connection

    # Notify task status
    conn.publish(
        orchestrator_config.sub_channel,
        _TaskUpdate(
            task_id=task_id,
            task_status=TaskStatus.STARTED,
        ).model_dump_json(),
    )

    workdir = scratch_dir / task_id

    # Initialize callback invoker if callbacks are configured
    callback_invoker = None
    if task.callbacks:
        callback_invoker = CallbackInvoker(
            max_retries=3,
            timeout=30.0,
            retry_delay=1.0,
        )

    try:
        _log.debug(f"task_id inside task is: {task_id}")
        convert_sources: list[Union[str, DocumentStream]] = []
        headers: Optional[dict[str, Any]] = None
        for source in task.sources:
            if isinstance(source, DocumentStream):
                convert_sources.append(source)
            elif isinstance(source, FileSource):
                convert_sources.append(source.to_document_stream())
            elif isinstance(source, HttpSource):
                convert_sources.append(str(source.url))
                if headers is None and source.headers:
                    headers = source.headers

        if not conversion_manager:
            raise RuntimeError("No converter")
        if not task.convert_options:
            raise RuntimeError("No conversion options")

        # TODO: potentially move the below code into thread wrapper and wait for it.
        conv_results = conversion_manager.convert_documents(
            sources=convert_sources,
            options=task.convert_options,
            headers=headers,
        )

        processed_results: DoclingTaskResult
        if task.task_type == TaskType.CONVERT:
            processed_results = process_export_results(
                task=task,
                conv_results=conv_results,
                work_dir=workdir,
                callback_invoker=callback_invoker,
            )
        elif task.task_type == TaskType.CHUNK:
            processed_results = process_chunk_results(
                task=task,
                conv_results=conv_results,
                work_dir=workdir,
                callback_invoker=callback_invoker,
            )
        safe_data = make_msgpack_safe(processed_results.model_dump())
        packed = msgpack.packb(safe_data, use_bin_type=True)
        result_key = f"{orchestrator_config.results_prefix}:{task_id}"
        conn.setex(result_key, orchestrator_config.results_ttl, packed)

        # Notify task status
        conn.publish(
            orchestrator_config.sub_channel,
            _TaskUpdate(
                task_id=task_id,
                task_status=TaskStatus.SUCCESS,
                result_key=result_key,
            ).model_dump_json(),
        )

        _log.debug("ended task")
    except Exception as e:
        _log.error(f"Conversion task {task_id} failed: {e}")
        # Notify task status
        conn.publish(
            orchestrator_config.sub_channel,
            _TaskUpdate(
                task_id=task_id,
                task_status=TaskStatus.FAILURE,
            ).model_dump_json(),
        )
        raise e

    finally:
        if workdir.exists():
            shutil.rmtree(workdir)

    return result_key


def clear_cache_task(conversion_manager: DoclingConverterManager, **_):
    """RQ job that clears the converter cache on the worker."""
    _log.info("Clearing converter cache on worker")
    conversion_manager.clear_cache()
    import gc

    gc.collect()
    _log.info("Converter cache cleared")


def run_worker(
    rq_config: Optional[RQOrchestratorConfig] = None,
    cm_config: Optional[DoclingConverterManagerConfig] = None,
):
    # create a new connection in thread, Redis and ConversionManager are not pickle
    rq_config = rq_config or RQOrchestratorConfig()
    scratch_dir = rq_config.scratch_dir or Path(tempfile.mkdtemp(prefix="docling_"))
    redis_conn, rq_queue = RQOrchestrator.make_rq_queue(rq_config)
    cm_config = cm_config or DoclingConverterManagerConfig()
    worker = CustomRQWorker(
        [rq_queue],
        connection=redis_conn,
        orchestrator_config=rq_config,
        cm_config=cm_config,
        scratch_dir=scratch_dir,
    )
    worker.work()


if __name__ == "__main__":
    run_worker()
