import base64
import logging
import shutil
import tempfile
import threading
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Callable, ContextManager, Optional, Union

import msgpack
import redis as sync_redis
from rq import SimpleWorker, get_current_job

from docling.datamodel.base_models import DocumentStream
from docling.datamodel.service.sources import FileSource, HttpSource, S3Coordinates
from docling.datamodel.service.tasks import TaskType

from docling_jobkit.convert.chunking import process_chunkable_results
from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.convert.results import process_exportable_results
from docling_jobkit.convert.source_expansion import expand_task_sources
from docling_jobkit.datamodel.exportable_document import (
    ExportableDocument,
    source_to_public_uri,
)
from docling_jobkit.datamodel.result import DoclingTaskResult
from docling_jobkit.datamodel.task import Task
from docling_jobkit.datamodel.task_meta import TaskStatus
from docling_jobkit.orchestrators.callback_invoker import CallbackInvoker
from docling_jobkit.orchestrators.rq.orchestrator import (
    _HEARTBEAT_INTERVAL,
    _HEARTBEAT_TTL,
    RQOrchestrator,
    RQOrchestratorConfig,
    _TaskUpdate,
)
from docling_jobkit.orchestrators.serialization import make_msgpack_safe
from docling_jobkit.public_errors import build_public_task_error

_log = logging.getLogger(__name__)

_PhaseContextFactory = Callable[[str], ContextManager[Any]]
_SourcePreparedHook = Callable[[int, Any, dict[str, str], Optional[bytes]], None]
_SourcesPreparedHook = Callable[[list[dict[str, str]], int, bool], None]
_ResultStoredHook = Callable[[str, int], None]
_FailureHook = Callable[[Task, Exception, list[dict[str, str]]], None]


def _null_phase_cm(_: str) -> ContextManager[Any]:
    return nullcontext()


def _prepare_convert_sources(
    task: Task,
    *,
    max_file_size: int | None = None,
    on_source_prepared: Optional[_SourcePreparedHook] = None,
) -> tuple[
    list[Union[str, DocumentStream]],
    Optional[dict[str, Any]],
    list[dict[str, str]],
]:
    convert_sources, headers = expand_task_sources(
        task,
        max_file_size=max_file_size,
    )
    source_info: list[dict[str, str]] = []

    for idx, source in enumerate(task.sources):
        raw_bytes: Optional[bytes] = None
        if isinstance(source, DocumentStream):
            info = {"type": "DocumentStream", "name": source.name}
        elif isinstance(source, FileSource):
            raw_bytes = base64.b64decode(source.base64_string)
            info = {"type": "FileSource", "filename": source.filename}
        elif isinstance(source, HttpSource):
            info = {"type": "HttpSource", "url": str(source.url)}
        elif isinstance(source, S3Coordinates):
            info = {
                "type": "S3Coordinates",
                "bucket": source.bucket,
                "key_prefix": source.key_prefix,
            }
        else:
            raise RuntimeError(f"Unsupported runtime task source: {type(source)!r}")

        source_info.append(info)
        if on_source_prepared:
            on_source_prepared(idx, source, info, raw_bytes)

    return convert_sources, headers, source_info


def _run_docling_task(
    task: Task,
    conversion_manager: DoclingConverterManager,
    orchestrator_config: RQOrchestratorConfig,
    scratch_dir: Path,
    *,
    phase_cm: Optional[_PhaseContextFactory] = None,
    on_source_prepared: Optional[_SourcePreparedHook] = None,
    on_sources_prepared: Optional[_SourcesPreparedHook] = None,
    on_result_stored: Optional[_ResultStoredHook] = None,
    on_failure: Optional[_FailureHook] = None,
) -> str:
    job = get_current_job()
    assert job is not None

    conn = job.connection
    task_id = task.task_id
    workdir = scratch_dir / task_id
    phase_cm = phase_cm or _null_phase_cm
    source_info: list[dict[str, str]] = []
    result_key: Optional[str] = None

    callback_invoker = CallbackInvoker() if task.callbacks else None

    try:
        with phase_cm("notify.task_started"):
            conn.publish(
                orchestrator_config.sub_channel,
                _TaskUpdate(
                    task_id=task_id,
                    task_status=TaskStatus.STARTED,
                ).model_dump_json(),
            )

        if not conversion_manager:
            raise RuntimeError("No converter")

        with phase_cm("prepare_sources"):
            convert_sources, headers, source_info = _prepare_convert_sources(
                task,
                max_file_size=conversion_manager.config.max_file_size,
                on_source_prepared=on_source_prepared,
            )
            if on_sources_prepared:
                on_sources_prepared(
                    source_info,
                    len(convert_sources),
                    headers is not None,
                )

        if not task.convert_options:
            raise RuntimeError("No conversion options")

        with phase_cm("convert_documents"):
            conv_results = conversion_manager.convert_documents(
                sources=convert_sources,
                options=task.convert_options,
                headers=headers,
            )

        exportable_documents = (
            ExportableDocument.from_conversion_result(
                conv_res,
                source_index=idx,
                source_uri=(
                    source_to_public_uri(task.sources[idx])
                    if idx < len(task.sources)
                    else str(conv_res.input.file)
                ),
            )
            for idx, conv_res in enumerate(conv_results)
        )

        processed_results: DoclingTaskResult
        with phase_cm("process_results"):
            if task.task_type == TaskType.CONVERT:
                with phase_cm("process_export_results"):
                    processed_results = process_exportable_results(
                        task=task,
                        exportable_documents=exportable_documents,
                        work_dir=workdir,
                        s3_presigned_config=orchestrator_config.s3_presigned_config,
                        callback_invoker=callback_invoker,
                        debug_error_details=orchestrator_config.debug_error_details,
                    )
            elif task.task_type == TaskType.CHUNK:
                with phase_cm("process_chunk_results"):
                    processed_results = process_chunkable_results(
                        task=task,
                        exportable_documents=exportable_documents,
                        work_dir=workdir,
                        callback_invoker=callback_invoker,
                        debug_error_details=orchestrator_config.debug_error_details,
                    )
            else:
                raise RuntimeError(f"Unsupported task type: {task.task_type}")

        with phase_cm("serialize_and_store"):
            safe_data = make_msgpack_safe(processed_results.model_dump())
            packed = msgpack.packb(safe_data, use_bin_type=True)
            result_key = f"{orchestrator_config.results_prefix}:{task_id}"
            conn.setex(result_key, orchestrator_config.results_ttl, packed)
            if on_result_stored:
                on_result_stored(result_key, len(packed))

        with phase_cm("notify.task_success"):
            conn.publish(
                orchestrator_config.sub_channel,
                _TaskUpdate(
                    task_id=task_id,
                    task_status=TaskStatus.SUCCESS,
                    result_key=result_key,
                ).model_dump_json(),
            )

        return result_key
    except Exception as e:
        if on_failure:
            on_failure(task, e, source_info)
        else:
            _log.error(f"Conversion task {task_id} failed: {e}")
        # Only publish FAILURE if the result was never committed to Redis; if
        # serialize_and_store succeeded but notify.task_success failed, the
        # result is retrievable and a spurious FAILURE would mislead subscribers.
        if result_key is None:
            conn.publish(
                orchestrator_config.sub_channel,
                _TaskUpdate(
                    task_id=task_id,
                    task_status=TaskStatus.FAILURE,
                    error_message=build_public_task_error(e),
                ).model_dump_json(),
            )
        raise
    finally:
        with phase_cm("cleanup"):
            if workdir.exists():
                shutil.rmtree(workdir)


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
    _log.debug(f"task_id inside task is: {task.task_id}")
    result_key = _run_docling_task(
        task,
        conversion_manager,
        orchestrator_config,
        scratch_dir,
    )
    _log.debug("ended task")
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
