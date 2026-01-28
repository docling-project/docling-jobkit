import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any, Optional, Union

import msgpack
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
from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestrator,
    RQOrchestratorConfig,
    _TaskUpdate,
)

_log = logging.getLogger(__name__)


def make_msgpack_safe(obj):
    """
    Recursively convert any non-msgpack-serializable types to safe types,
    keeping bytes unchanged.
    """
    from datetime import datetime
    from decimal import Decimal

    # Types msgpack already supports
    if obj is None or isinstance(obj, (str, int, float, bool, bytes)):
        return obj

    # Handle sequences
    if isinstance(obj, (list, tuple, set)):
        return [make_msgpack_safe(v) for v in obj]

    # Handle mappings
    if isinstance(obj, dict):
        return {make_msgpack_safe(k): make_msgpack_safe(v) for k, v in obj.items()}

    # Known common conversions
    if isinstance(obj, (datetime, Decimal)):
        return str(obj)  # ISO for datetime, str for Decimal

    # Fallback: use string representation
    return str(obj)


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

    def perform_job(self, job, queue):
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
            )
        elif task.task_type == TaskType.CHUNK:
            processed_results = process_chunk_results(
                task=task,
                conv_results=conv_results,
                work_dir=workdir,
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
