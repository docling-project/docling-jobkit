from typing import Iterator, TypedDict

from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.source_processor import BaseSourceProcessor
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource


class HttpFileIdentifier(TypedDict):
    source: HttpSource | FileSource
    index: int


class HttpSourceProcessor(BaseSourceProcessor[HttpFileIdentifier]):
    def __init__(self, source: HttpSource | FileSource):
        super().__init__()
        self._source = source

    def _initialize(self):
        pass

    def _finalize(self):
        pass

    def _list_document_ids(self) -> Iterator[HttpFileIdentifier]:
        """Yield a single identifier for the HTTP/File source."""
        yield HttpFileIdentifier(source=self._source, index=0)

    def _fetch_document_by_id(self, identifier: HttpFileIdentifier) -> DocumentStream:
        """Fetch document from the identifier."""
        source = identifier["source"]
        if isinstance(source, FileSource):
            return source.to_document_stream()
        elif isinstance(source, HttpSource):
            # TODO: fetch, e.g. using the helpers in docling-core
            raise NotImplementedError("HttpSource fetching is not yet implemented")
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")

    def _fetch_documents(self) -> Iterator[DocumentStream]:
        if isinstance(self._source, FileSource):
            yield self._source.to_document_stream()
        elif isinstance(self._source, HttpSource):
            # TODO: fetch, e.g. using the helpers in docling-core
            raise NotImplementedError()
