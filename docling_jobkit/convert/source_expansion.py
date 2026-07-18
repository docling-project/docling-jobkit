from typing import Any, Callable, Optional, Union

from docling.datamodel.base_models import DocumentStream
from docling.datamodel.service.sources import (
    AzureBlobCoordinates,
    FileSource,
    GoogleCloudStorageCoordinates,
    GoogleDriveCoordinates,
    HttpSource,
    S3Coordinates,
)

from docling_jobkit.connectors.source_processor_factory import get_source_processor
from docling_jobkit.datamodel.task import Task

_EXPANDABLE_SOURCE_TYPES = (
    S3Coordinates,
    AzureBlobCoordinates,
    GoogleCloudStorageCoordinates,
    GoogleDriveCoordinates,
)


def expand_task_sources(
    task: Task,
    *,
    max_file_size: int | None = None,
    http_materializer: Optional[
        Callable[[HttpSource], Optional[DocumentStream]]
    ] = None,
) -> tuple[list[Union[str, DocumentStream]], Optional[dict[str, Any]], list[int]]:
    """Expand task sources into converter inputs, optional HTTP headers, and origins.

    ``source_indices[i]`` is the ``task.sources`` index that produced
    ``convert_sources[i]`` (an S3 source expands to several inputs). When
    ``http_materializer`` is given, ``HttpSource`` inputs are fetched through it
    instead of passed as URL strings; returning ``None`` skips the source.
    """
    convert_sources: list[Union[str, DocumentStream]] = []
    source_indices: list[int] = []
    headers: Optional[dict[str, Any]] = None

    def _add(converter_input: Union[str, DocumentStream], origin: int) -> None:
        convert_sources.append(converter_input)
        source_indices.append(origin)

    for idx, source in enumerate(task.sources):
        if isinstance(source, DocumentStream):
            _add(source, idx)
        elif isinstance(source, FileSource):
            _add(source.to_document_stream(), idx)
        elif isinstance(source, HttpSource):
            if http_materializer is not None:
                document_stream = http_materializer(source)
                if document_stream is not None:
                    _add(document_stream, idx)
            else:
                _add(str(source.url), idx)
                if headers is None and source.headers:
                    headers = source.headers
        elif isinstance(source, _EXPANDABLE_SOURCE_TYPES):
            # Orchestrators need converter-ready inputs for mixed runtime sources in a
            # single task. Connector chunking is a CLI batching primitive and would
            # change task/result semantics for DocumentStream/FileSource/HttpSource
            # inputs, so the runtime path expands sources directly here instead.
            with get_source_processor(source) as source_processor:
                for document_stream in source_processor.iterate_documents(
                    max_file_size=max_file_size
                ):
                    _add(document_stream, idx)
        else:
            raise RuntimeError(f"Unsupported runtime task source: {type(source)!r}")

    return convert_sources, headers, source_indices
