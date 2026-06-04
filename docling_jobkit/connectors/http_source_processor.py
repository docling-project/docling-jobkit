from typing import Iterator, TypedDict

from typing_extensions import override

from docling.datamodel.service.sources import FileSource, HttpSource
from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    ConverterSource,
    SourceDocumentRef,
)


class HttpFileIdentifier(TypedDict):
    source: HttpSource | FileSource
    index: int


class HttpSourceProcessor(
    BaseSourceProcessor[HttpSource | FileSource, HttpFileIdentifier]
):
    def __init__(self, source: HttpSource | FileSource):
        super().__init__(source)
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

    @override
    def _make_document_ref(
        self, identifier: HttpFileIdentifier, source_index: int
    ) -> SourceDocumentRef[HttpFileIdentifier]:
        source = identifier["source"]
        if isinstance(source, FileSource):
            filename = source.filename
            source_uri = source.filename
        else:
            filename = str(source.url).rsplit("/", 1)[-1] or str(source.url)
            source_uri = str(source.url)
        return SourceDocumentRef(
            id=identifier,
            source_index=source_index,
            source_uri=source_uri,
            filename=filename,
        )

    @override
    def fetch_converter_source_by_ref(
        self, ref: SourceDocumentRef[HttpFileIdentifier]
    ) -> ConverterSource:
        source = ref.id["source"]
        if isinstance(source, HttpSource):
            return str(source.url)
        return self._fetch_document_by_id(ref.id)

    @override
    def headers_for_ref(
        self, ref: SourceDocumentRef[HttpFileIdentifier]
    ) -> dict[str, object] | None:
        source = ref.id["source"]
        if isinstance(source, HttpSource) and source.headers:
            return source.headers
        return None

    def _fetch_documents(self) -> Iterator[DocumentStream]:
        if isinstance(self._source, FileSource):
            yield self._source.to_document_stream()
        elif isinstance(self._source, HttpSource):
            # TODO: fetch, e.g. using the helpers in docling-core
            raise NotImplementedError()
