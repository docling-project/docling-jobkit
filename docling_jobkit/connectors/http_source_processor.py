from typing import Iterator

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.source_processor import BaseSourceProcessor
from docling_jobkit.datamodel.http_inputs import FileSource, HttpSource


class HttpSourceProcessor(BaseSourceProcessor):
    def __init__(self, source: HttpSource | FileSource):
        super().__init__()
        self._source = source

    def _initialize(self):
        pass

    def _finalize(self):
        pass

    def _fetch_documents(self) -> Iterator[DocumentStream]:
        if isinstance(self._source, FileSource):
            yield self._source.to_document_stream()
        elif isinstance(self._source, HttpSource):
            # TODO: fetch, e.g. using the helpers in docling-core
            raise NotImplementedError()
