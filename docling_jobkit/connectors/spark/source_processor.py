from __future__ import annotations

import logging
from io import BytesIO
from typing import Iterator, NamedTuple

from pydantic import BaseModel
from typing_extensions import override

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.errors import (
    SourceConnectorConfigError,
    SourceConnectorPolicyError,
    map_connector_authentication_errors,
)
from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    ConverterSource,
    SourceDocumentRef,
)
from docling_jobkit.connectors.spark.backend_factory import get_backend
from docling_jobkit.connectors.spark.helper import (
    download_document_from_url,
    is_spark_authentication_error,
    is_spark_unavailable_error,
)
from docling_jobkit.connectors.spark.models import TaskSparkSource
from docling_jobkit.convert.materialization import (
    SourceLimitExceededError,
    normalize_max_file_size,
)

_log = logging.getLogger(__name__)


# Distributed identifier
class SparkRowID(NamedTuple):
    partition_value: object
    row_key: str
    filename: str | None = None


_map_spark_source_errors = map_connector_authentication_errors(
    "Spark",
    is_spark_authentication_error,
    source=True,
    source_kind="spark",
    is_unavailable_error=is_spark_unavailable_error,
)


class SparkSourceProcessor(BaseSourceProcessor[TaskSparkSource, SparkRowID]):
    def __init__(self, coords: TaskSparkSource) -> None:
        super().__init__(coords)
        self._coords = coords

    @property
    def _partition_col(self) -> str | None:
        """The partition column for ORDER BY, or None."""
        return self._coords.partition_column

    @property
    def _is_query(self) -> bool:
        return self._coords.query is not None

    @classmethod
    def check_dependencies(cls) -> None:
        import pyspark  # noqa: F401

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (TaskSparkSource,)

    @classmethod
    def is_expandable(cls, config: BaseModel) -> bool:
        """Expandable when id_column enables cheap chunking, or when url_column
        avoids the memory problem. Not expandable for unbounded content_column
        reads without an id column (every chunk would be a full table scan)."""
        if getattr(config, "id_column", None):
            return True
        if getattr(config, "url_column", None):
            return True
        # Content mode without id_column: only expandable if bounded
        max_num = getattr(config, "max_num_elements", None)
        return max_num is not None

    @_map_spark_source_errors
    def _initialize(self) -> None:
        self._backend = get_backend(self._coords)

        # Validator ensures exactly one of table/query and content_column/url_column is set
        if self._coords.table and not self._backend.table_exists(self._coords.table):
            raise SourceConnectorConfigError(
                f"Spark source table {self._coords.table!r} not found, verify the "
                f"catalog.schema.table name and that it exists on the cluster."
            )

        _log.info("Spark source resolved %s", self._coords.table or "query")

    def _finalize(self) -> None:
        if self._backend is not None:
            self._backend.close()

    @_map_spark_source_errors
    def _count_documents(self) -> int:
        # Validator ensures exactly one of table/query and content_column/url_column is set
        count = self._backend.count_documents(
            self._coords.table or self._coords.query,  # type: ignore[arg-type]
            self._is_query,
            self._coords.content_column or self._coords.url_column,  # type: ignore[arg-type]
            self._coords.max_num_elements,
        )
        _log.debug("Spark source counted %d document(s)", count)

        return count

    @_map_spark_source_errors
    def _list_document_ids(self) -> Iterator[SparkRowID]:
        """Enumerate row identifiers ordered by partition.

        Unlike a typical source (one opaque id per document), each id is a
        (partition_value, row_key, filename) triple, and rows arrive
        grouped by partition so iterate_document_chunks can emit one chunk per
        partition without re-sorting.

        In content_column mode, row_key is the id_column value when id_column
        is set, otherwise sha2(content, 256). In url_column mode, row_key
        holds the URL instead.
        """
        # Validator ensures exactly one of table/query and content_column/url_column is set
        if self._coords.url_column:
            # URL mode: enumerate (id, url, filename)
            for id_val, url, filename in self._backend.enumerate_urls(
                self._coords.table or self._coords.query,  # type: ignore[arg-type]
                self._is_query,
                self._coords.url_column,  # type: ignore[arg-type]
                self._coords.id_column,  # type: ignore[arg-type]
                self._coords.filename_column,
                self._coords.max_num_elements,
            ):
                # In url mode: partition_value=None, row_key=URL
                yield SparkRowID(None, url, filename)
        else:
            # Content mode: enumerate (partition, sha2, filename)
            for partition_value, row_key, filename in self._backend.enumerate_row_keys(
                self._coords.table or self._coords.query,  # type: ignore[arg-type]
                self._is_query,
                self._coords.content_column,  # type: ignore[arg-type]
                self._partition_col,
                self._coords.filename_column,
                self._coords.max_num_elements,
                self._coords.id_column,
            ):
                yield SparkRowID(partition_value, row_key, filename)

    @override
    def _make_document_ref(
        self, identifier: SparkRowID, source_index: int
    ) -> SourceDocumentRef:
        partition_value, row_key, filename = identifier

        if self._coords.url_column:
            # URL mode: row_key is the URL, use it as source_uri
            from pathlib import Path

            return SourceDocumentRef(
                id=identifier,
                source_index=source_index,
                source_uri=row_key,  # URL itself
                filename=(filename or Path(row_key).name),
            )
        else:
            # Content mode: existing logic
            return SourceDocumentRef(
                id=identifier,
                source_index=source_index,
                source_uri=f"{self._coords.table}#{partition_value}",
                filename=(filename or f"{row_key}.bin"),
            )

    @override
    def iterate_converter_sources(
        self, *, max_file_size: int | None = None
    ) -> Iterator[ConverterSource]:
        """In URL mode, yield URL strings. In content mode, yield DocumentStreams."""
        if self._coords.url_column:
            # URL mode: yield URLs directly from _list_document_ids
            for row_id in self._list_document_ids():
                yield row_id.row_key  # row_key holds the URL
        else:
            # Content mode: use base implementation (yields DocumentStreams)
            yield from self.iterate_documents(max_file_size=max_file_size)

    @override
    def fetch_converter_source_by_ref(
        self, ref: SourceDocumentRef[SparkRowID], *, max_file_size: int | None = None
    ) -> ConverterSource:
        """Return URL string in url_column mode, DocumentStream in content_column mode."""
        if self._coords.url_column:
            # Reference mode: return the URL string
            url = ref.id.row_key  # In url mode, row_key holds the URL
            return url
        else:
            # Content mode: existing logic
            return self._fetch_document_by_id(ref.id, max_file_size=max_file_size)

    @override
    def converter_headers(self) -> dict[str, object] | None:
        """Provide Bearer token for Databricks Files API in url_column mode."""
        if not self._coords.url_column:
            return None

        # Databricks Files API auth
        if self._coords.auth and hasattr(self._coords.auth, "token"):
            return {
                "Authorization": f"Bearer {self._coords.auth.token.get_secret_value()}"
            }
        return None

    @override
    def headers_for_ref(
        self, ref: SourceDocumentRef[SparkRowID]
    ) -> dict[str, object] | None:
        """Provide Bearer token for Databricks Files API in url_column mode."""
        return self.converter_headers()

    @_map_spark_source_errors
    def _fetch_document_by_id(
        self, identifier: SparkRowID, *, max_file_size: int | None = None
    ) -> DocumentStream:
        """Fetch a single document by its identifier.

        In url_column mode: Downloads from URL using Databricks Files API.
        In content_column mode: Fetches single row by id_column or sha2 hash.
        """
        _partition_value, row_key, filename = identifier

        # URL mode: download from Databricks Files API
        if self._coords.url_column:
            url = row_key  # row_key holds the URL in url_column mode
            auth_token = self._coords.auth.token.get_secret_value()  # type: ignore[union-attr]

            _log.info("Downloading document from URL: %s", url)
            buffer = download_document_from_url(url, auth_token)

            # Use filename from identifier or extract from URL
            name = filename or url.split("/")[-1]
            return DocumentStream(name=name, stream=buffer)

        # Content mode: fetch single row (no partition cache)
        table_or_query = self._coords.table or self._coords.query
        assert table_or_query is not None, "Either table or query must be set"

        if self._coords.id_column:
            # Fetch by id_column value (row_key is the id)
            results = list(
                self._backend.fetch_by_id(
                    table_or_query,
                    self._is_query,
                    self._coords.id_column,  # type: ignore[arg-type]
                    row_key,
                    self._coords.content_column,  # type: ignore[arg-type]
                    self._coords.filename_column,
                )
            )
        else:
            # Fetch by sha2 hash (row_key is sha2(content, 256))
            results = list(
                self._backend.fetch_by_content_hash(
                    table_or_query,
                    self._is_query,
                    self._coords.content_column,  # type: ignore[arg-type]
                    row_key,
                    self._coords.filename_column,
                )
            )

        if not results:
            raise SourceConnectorPolicyError(
                f"Document not found: {row_key}", source_kind="spark"
            )

        data, name = results[0]
        limit = normalize_max_file_size(max_file_size)
        if limit is not None and len(data) > limit:
            raise SourceLimitExceededError(
                f"Source {name!r} exceeds max_file_size={limit} bytes"
            )

        return DocumentStream(name=name, stream=BytesIO(data))

    @_map_spark_source_errors
    def _fetch_documents(
        self, *, max_file_size: int | None = None
    ) -> Iterator[DocumentStream]:
        # URL mode: fetch documents by downloading from URLs
        if self._coords.url_column:
            for row_id in self._list_document_ids():
                yield self._fetch_document_by_id(row_id, max_file_size=max_file_size)
            return

        # Content mode: stream bytes from backend
        if self._coords.partition_column is None:
            _log.info(
                "Spark source reading single-node on the driver (no partition_column). "
                "Under a distributed orchestrator set partition_column for per-partition reads."
            )
        limit = normalize_max_file_size(max_file_size)

        yielded = 0
        skipped_null = 0

        documents = self._backend.stream_documents(
            self._coords.table or self._coords.query,  # type: ignore[arg-type]
            self._is_query,
            self._coords.content_column,  # type: ignore[arg-type]
            self._coords.filename_column,
            self._coords.max_num_elements,
        )
        for index, (data, fname) in enumerate(documents):
            # Skip None or empty byte rows
            if not data:
                skipped_null += 1
                _log.warning("Skipping row %d: content is null/empty", index)
                continue

            if limit is not None and len(data) > limit:
                name = fname if fname else f"row-{index}"

                raise SourceLimitExceededError(
                    f"Source {name!r} exceeds max_file_size={limit} bytes"
                )

            name = fname or f"row-{index}.bin"
            yielded += 1

            yield DocumentStream(name=name, stream=BytesIO(data))

        _log.info(
            "Spark source yielded %d document(s), skipped %d null/empty row(s)",
            yielded,
            skipped_null,
        )
