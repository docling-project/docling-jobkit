"""Shared, low-memory consumption of a transport ``DocumentChunk``.

Both the CLI multiprocessing workers and the Ray converter replica receive a
fetcher-stripped :class:`DocumentChunk` (see ``DocumentChunk.for_transport``) and
need to turn its ``refs`` into converter inputs. This module centralizes that step
so the same logic runs everywhere and keeps peak memory bounded to a single
in-flight document: sources are fetched lazily, one at a time, as the converter
pulls them.
"""

from contextlib import contextmanager
from typing import Any, Iterator, Optional

from docling_jobkit.connectors.source_processor import (
    ConverterSource,
    DocumentChunk,
    SourceDocumentRef,
)
from docling_jobkit.connectors.source_processor_factory import get_source_processor


def _resolve_headers(
    source_processor: Any,
    refs: list[SourceDocumentRef] | Any,
) -> Optional[dict[str, Any]]:
    """Resolve per-request headers without fetching any document bytes.

    ``headers_for_ref`` only inspects the ref metadata, so this pre-pass is cheap.
    Headers are assumed uniform across a chunk (the converter accepts a single
    ``headers`` argument); the first non-empty value wins.
    """
    for ref in refs:
        ref_headers = source_processor.headers_for_ref(ref)
        if ref_headers:
            return ref_headers
    return None


@contextmanager
def open_chunk_sources(
    chunk: DocumentChunk,
    *,
    max_file_size: int | None = None,
    allow_external_plugins: bool = False,
) -> Iterator[tuple[Iterator[ConverterSource], Optional[dict[str, Any]]]]:
    """Open a transport chunk and yield ``(sources_iter, headers)``.

    ``sources_iter`` is a generator that materializes ONE converter source
    (``DocumentStream`` or remote URL string) per ``next()`` call, so feeding it
    to ``DoclingConverterManager.convert_documents`` keeps at most one document
    in memory at a time. The source processor stays open for the lifetime of the
    ``with`` block, which must therefore enclose the full conversion loop.
    """
    with get_source_processor(
        chunk.source, allow_external_plugins=allow_external_plugins
    ) as source_processor:
        headers = _resolve_headers(source_processor, chunk.refs)

        def _iter() -> Iterator[ConverterSource]:
            for ref in chunk.refs:
                yield source_processor.fetch_converter_source_by_ref(
                    ref,
                    max_file_size=max_file_size,
                )

        yield _iter(), headers
