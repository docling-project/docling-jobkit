from typing import Iterator

from pydantic import BaseModel, ConfigDict
from typing_extensions import override

from docling.datamodel.service.requests import (
    FileSourceRequest,
    HttpSourceRequest,
)
from docling.datamodel.service.sources import FileSource, HttpSource
from docling_core.types.io import DocumentStream

from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    ConverterSource,
    SourceDocumentRef,
)
from docling_jobkit.convert.materialization import (
    SourceLimitExceededError,
    _filename_for_http_source,
    normalize_max_file_size,
)


class HttpFileIdentifier(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    source: HttpSource | FileSource
    size: int | None = None
    etag: str | None = None


class HttpSourceProcessor(
    BaseSourceProcessor[HttpSource | FileSource, HttpFileIdentifier]
):
    def __init__(self, source: HttpSource | FileSource):
        super().__init__(source)
        self._source = source

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (FileSourceRequest, HttpSourceRequest)

    def _initialize(self):
        pass

    def _finalize(self):
        pass

    def _list_document_ids(self) -> Iterator[HttpFileIdentifier]:
        """Yield a single identifier for the HTTP/File source."""
        if isinstance(self._source, HttpSource):
            size, etag = self._try_head_request(self._source)
            yield HttpFileIdentifier(source=self._source, size=size, etag=etag)
            return
        yield HttpFileIdentifier(source=self._source)

    def _try_head_request(self, source: HttpSource) -> tuple[int | None, str | None]:
        try:
            import httpx

            with httpx.Client(follow_redirects=True) as client:
                response = client.head(
                    str(source.url),
                    headers=source.headers,
                )
                if not response.is_success:
                    return None, None
                content_length = response.headers.get("content-length")
                size = int(content_length) if content_length is not None else None
                return size, response.headers.get("etag")
        except (ValueError, httpx.HTTPError):
            return None, None

    def _fetch_document_by_id(
        self,
        identifier: HttpFileIdentifier,
        *,
        max_file_size: int | None = None,
    ) -> DocumentStream:
        """Fetch document from the identifier."""
        del max_file_size
        source = identifier.source
        if isinstance(source, FileSource):
            return source.to_document_stream()
        elif isinstance(source, HttpSource):
            # HttpSource is never materialized by this processor: service runtimes
            # pass the raw URL through to the converter (see
            # fetch_converter_source_by_ref / expand_task_sources). Direct byte
            # retrieval here is intentionally unsupported.
            raise NotImplementedError("HttpSource fetching is not supported")
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")

    @override
    def _make_document_ref(
        self, identifier: HttpFileIdentifier, source_index: int
    ) -> SourceDocumentRef[HttpFileIdentifier]:
        source = identifier.source
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
        self,
        ref: SourceDocumentRef[HttpFileIdentifier],
        *,
        max_file_size: int | None = None,
    ) -> ConverterSource:
        source = ref.id.source
        if isinstance(source, HttpSource):
            # Enforce max_file_size from HEAD metadata only; never materialize in
            # this path. Either reject on the advertised Content-Length (already
            # probed into ref.id.size by _list_document_ids), or hand the raw URL
            # to the converter so it performs the fetch -- identical to the
            # no-limit behavior. Setting a limit must not introduce a fetch that
            # would not otherwise happen.
            limit = normalize_max_file_size(max_file_size)
            if limit is not None and ref.id.size is not None and ref.id.size > limit:
                raise SourceLimitExceededError(
                    f"Source '{_filename_for_http_source(source)}' "
                    f"exceeds max_file_size={limit} bytes"
                )
            return str(source.url)
        return self._fetch_document_by_id(
            ref.id,
            max_file_size=max_file_size,
        )

    @override
    def headers_for_ref(
        self, ref: SourceDocumentRef[HttpFileIdentifier]
    ) -> dict[str, object] | None:
        source = ref.id.source
        if isinstance(source, HttpSource) and source.headers:
            return source.headers
        return None

    def _fetch_documents(
        self, *, max_file_size: int | None = None
    ) -> Iterator[DocumentStream]:
        del max_file_size
        if isinstance(self._source, FileSource):
            yield self._source.to_document_stream()
        elif isinstance(self._source, HttpSource):
            # See _fetch_document_by_id: HttpSource is passthrough-only.
            raise NotImplementedError("HttpSource fetching is not supported")
