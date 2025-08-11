import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any, Optional, Union

import msgpack
from rq import SimpleWorker, get_current_job

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.convert.results import process_results
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
from docling_jobkit.datamodel.result import ConvertDocumentResult
from docling_jobkit.datamodel.task import Task
from docling_jobkit.orchestrators.rq.orchestrator import (
    RQOrchestrator,
    RQOrchestratorConfig,
)

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
        ## Approach to init conversion manager in each worker using arg config instead of conversion_manager
        # cm=DoclingConverterManager(config=conversion_manager_config)
        # pdf_format_option = cm.get_pdf_pipeline_opts(ConvertDocumentsOptions())
        # converter = cm.get_converter(pdf_format_option)
        # converter.initialize_pipeline(InputFormat.PDF)

        self.orchestrator_config = orchestrator_config
        self.conversion_manager = DoclingConverterManager(cm_config)
        self.scratch_dir = scratch_dir

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
            self.logger.error(f"Job {job.id} failed: {e}")
            raise


def conversion_task(
    task_data: dict,
    conversion_manager: DoclingConverterManager,
    orchestrator_config: RQOrchestratorConfig,
    scratch_dir: Path,
):
    _log.debug("started task")
    task = Task.model_validate(task_data)
    task_id = task.task_id

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
        if not task.options:
            raise RuntimeError("No conversion options")
        conv_results = conversion_manager.convert_documents(
            sources=convert_sources,
            options=task.options,
            headers=headers,
        )

        processed_results: ConvertDocumentResult = process_results(
            conversion_options=task.options,
            target=task.target,
            conv_results=conv_results,
            work_dir=workdir,
        )
        packed = msgpack.packb(processed_results.model_dump(), use_bin_type=True)
        result_key = f"{orchestrator_config.results_prefix}:{task_id}"
        job = get_current_job()
        assert job is not None
        job.connection.setex(result_key, orchestrator_config.results_ttl, packed)

        _log.debug("ended task")
    finally:
        if workdir.exists():
            shutil.rmtree(workdir)

    return result_key


def run_worker():
    # create a new connection in thread, Redis and ConversionManager are not pickle
    config = RQOrchestratorConfig()
    scratch_dir = config.scratch_dir or Path(tempfile.mkdtemp(prefix="docling_"))
    redis_conn, rq_queue = RQOrchestrator.make_rq_queue(config)
    cm_config = DoclingConverterManagerConfig()
    worker = CustomRQWorker(
        [rq_queue],
        connection=redis_conn,
        orchestrator_config=config,
        cm_config=cm_config,
        scratch_dir=scratch_dir,
    )
    worker.work()


if __name__ == "__main__":
    run_worker()
