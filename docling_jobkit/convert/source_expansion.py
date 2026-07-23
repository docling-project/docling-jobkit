from typing import Any, Optional

from pydantic import BaseModel

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.source_processor import ConverterSource
from docling_jobkit.connectors.source_processor_factory import get_source_processor
from docling_jobkit.datamodel.task import Task


def expand_task_sources(
    task: Task,
    *,
    max_file_size: int | None = None,
    allow_external_plugins: bool = False,
) -> tuple[list[ConverterSource], Optional[dict[str, Any]]]:
    """Expand task sources into converter inputs and optional HTTP headers."""
    convert_sources: list[ConverterSource] = []
    headers: Optional[dict[str, Any]] = None

    for source in task.sources:
        if isinstance(source, DocumentStream):
            convert_sources.append(source)
        elif isinstance(source, BaseModel):
            with get_source_processor(
                source, allow_external_plugins=allow_external_plugins
            ) as source_processor:
                if headers is None:
                    headers = source_processor.converter_headers()
                convert_sources.extend(
                    source_processor.iterate_converter_sources(
                        max_file_size=max_file_size
                    )
                )
        else:
            raise RuntimeError(f"Unsupported runtime task source: {type(source)!r}")

    return convert_sources, headers
