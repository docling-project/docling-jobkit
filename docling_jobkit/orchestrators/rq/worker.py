import logging
from typing import Any, Optional, Union

from rq import Worker

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.convert.manager import DoclingConverterManager
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource
from docling_jobkit.datamodel.task import Task

_log = logging.getLogger(__name__)


class CustomRQWorker(Worker):
    def __init__(
        self, queues, connection=None, conversion_manager=None, *args, **kwargs
    ):
        ## Approach to init conversion manager in each worker using arg config instead of conversion_manager
        # cm=DoclingConverterManager(config=conversion_manager_config)
        # pdf_format_option = cm.get_pdf_pipeline_opts(ConvertDocumentsOptions())
        # converter = cm.get_converter(pdf_format_option)
        # converter.initialize_pipeline(InputFormat.PDF)

        self.conversion_manager = conversion_manager

        # Call parent class constructor
        super().__init__(queues, connection=connection, *args, **kwargs)

    def perform_job(self, job, queue):
        try:
            # Add to job's kwargs conversion manager
            if hasattr(job, "kwargs"):
                job.kwargs["conversion_manager"] = self.conversion_manager

            return super().perform_job(job, queue)
        except Exception as e:
            # Custom error handling for individual jobs
            self.logger.error(f"Job {job.id} failed: {e}")
            raise


def conversion_task(
    task_data: dict, conversion_manager: DoclingConverterManager | None = None
):
    _log.debug("started task")
    task = Task.model_validate(task_data)
    task_id = task.task_id

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
    results = conversion_manager.convert_documents(
        sources=convert_sources,
        options=task.options,
        headers=headers,
    )

    _log.debug("ended task")
    return list(results)
