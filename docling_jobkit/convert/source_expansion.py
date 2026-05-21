from typing import Any, Optional, Union

from docling.datamodel.base_models import DocumentStream
from docling.datamodel.service.sources import FileSource, HttpSource, S3Coordinates

from docling_jobkit.connectors.source_processor_factory import get_source_processor
from docling_jobkit.datamodel.task import Task


def expand_task_sources(
    task: Task,
) -> tuple[list[Union[str, DocumentStream]], Optional[dict[str, Any]]]:
    """Expand task sources into converter inputs and optional HTTP headers."""
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
        elif isinstance(source, S3Coordinates):
            with get_source_processor(source) as source_processor:
                for document_stream in source_processor.iterate_documents():
                    convert_sources.append(document_stream)
        else:
            raise RuntimeError(f"Unsupported runtime task source: {type(source)!r}")

    return convert_sources, headers
