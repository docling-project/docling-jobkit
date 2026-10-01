from typing import Any, Optional

from pydantic import BaseModel

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.artifact_paths import hash_path_component
from docling_jobkit.connectors.source_processor import ConverterSource
from docling_jobkit.connectors.source_processor_factory import get_source_processor
from docling_jobkit.datamodel.source_identity import SourceIdentity
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


def expand_task_sources_with_identities(
    task: Task,
    *,
    max_file_size: int | None = None,
    allow_external_plugins: bool = False,
) -> tuple[list[ConverterSource], list[SourceIdentity], Optional[dict[str, Any]]]:
    """Expand sources while retaining each discovered document's public identity."""
    convert_sources: list[ConverterSource] = []
    identities: list[SourceIdentity] = []
    headers: Optional[dict[str, Any]] = None

    def append(source: ConverterSource, source_uri: str) -> None:
        convert_sources.append(source)
        identities.append(
            SourceIdentity(
                source_index=source_index,
                source_uri=source_uri,
                source_key=hash_path_component(source_uri),
            )
        )

    for source_index, source in enumerate(task.sources):
        if isinstance(source, DocumentStream):
            append(source, source.name)
        elif isinstance(source, BaseModel):
            with get_source_processor(
                source, allow_external_plugins=allow_external_plugins
            ) as source_processor:
                if headers is None:
                    headers = source_processor.converter_headers()
                try:
                    for chunk in source_processor.iterate_document_chunks(128):
                        for ref in chunk.refs:
                            append(
                                source_processor.fetch_converter_source_by_ref(
                                    ref, max_file_size=max_file_size
                                ),
                                ref.source_uri,
                            )
                except RuntimeError as exc:
                    if "does not support chunking" not in str(exc):
                        raise
                    for expanded in source_processor.iterate_converter_sources(
                        max_file_size=max_file_size
                    ):
                        append(
                            expanded,
                            expanded.name
                            if isinstance(expanded, DocumentStream)
                            else str(expanded),
                        )
        else:
            raise RuntimeError(f"Unsupported runtime task source: {type(source)!r}")

    return convert_sources, identities, headers
