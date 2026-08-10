from __future__ import annotations

import logging
from io import BytesIO
from typing import TYPE_CHECKING, Iterator, NamedTuple, Optional

from pydantic import BaseModel
from typing_extensions import override

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.errors import (
    SourceConnectorConfigError,
    map_connector_authentication_errors,
)
from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    DocumentChunk,
    SourceDocumentRef,
)
from docling_jobkit.connectors.spark import (
    is_spark_authentication_error,
    is_spark_unavailable_error,
)
from docling_jobkit.connectors.spark.backend_factory import get_backend
from docling_jobkit.convert.materialization import (
    SourceLimitExceededError,
    normalize_max_file_size,
)
from docling_jobkit.datamodel.spark_coords import TaskSparkSource

if TYPE_CHECKING:
    pass

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
        # Distributed per-worker partition cache: one partition in memory at a time.
        self._cached_partition: Optional[object] = None
        self._partition_cache: dict[str, tuple[bytes, str]] = {}

    @property
    def _partition_col(self) -> str:
        """The partition column, required in distributed mode."""
        partition_col = self._coords.partition_column
        if partition_col is None:
            raise RuntimeError("partition_column is required for distributed reads")

        return partition_col

    @classmethod
    def check_dependencies(cls) -> None:
        import pyspark  # noqa: F401

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (TaskSparkSource,)

    @classmethod
    def is_expandable(cls, config: BaseModel) -> bool:
        return bool(getattr(config, "partition_column", None))

    @_map_spark_source_errors
    def _initialize(self) -> None:
        self._backend = get_backend(self._coords)
        if not self._backend.table_exists(self._coords.table):
            raise SourceConnectorConfigError(
                f"Spark source table {self._coords.table!r} not found, verify the "
                f"catalog.schema.table name and that it exists on the cluster."
            )

        _log.info("Spark source resolved table %s", self._coords.table)
        self._cached_partition = None
        self._partition_cache = {}

    def _finalize(self) -> None:
        if self._backend is not None:
            self._backend.close()

        self._cached_partition = None
        self._partition_cache = {}

    @_map_spark_source_errors
    def _count_documents(self) -> int:
        count = self._backend.count_documents(
            self._coords.table,
            self._coords.content_column,
            self._coords.max_num_elements,
        )
        _log.debug("Spark source counted %d document(s)", count)

        return count

    @_map_spark_source_errors
    def _list_document_ids(self) -> Iterator[SparkRowID]:
        """Enumerate row identifiers ordered by partition.

        Unlike a typical source (one opaque id per document), each id is a
        (partition_value, sha2 row_key, filename) triple, and rows arrive
        grouped by partition so iterate_document_chunks can emit one chunk per
        partition without re-sorting.
        """
        for partition_value, row_key, filename in self._backend.enumerate_row_keys(
            self._coords.table,
            self._coords.content_column,
            self._partition_col,
            self._coords.filename_column,
            self._coords.max_num_elements,
        ):
            yield SparkRowID(partition_value, row_key, filename)

    @override
    def _make_document_ref(
        self, identifier: SparkRowID, source_index: int
    ) -> SourceDocumentRef:
        partition_value, row_key, filename = identifier

        return SourceDocumentRef(
            id=identifier,
            source_index=source_index,
            source_uri=f"{self._coords.table}#{partition_value}",
            filename=(filename or f"{row_key}.bin"),
        )

    def iterate_document_chunks(
        self, chunk_size: int
    ) -> Iterator[DocumentChunk[TaskSparkSource, SparkRowID]]:
        if not self._coords.partition_column:
            raise RuntimeError(
                "Spark distributed processing requires 'partition_column' "
                "set it, or run the local orchestrator for a single-driver read"
            )

        ids_gen = self._list_document_ids()

        chunk_index = 0
        source_index = 0
        current_partition: object = object()
        current_ids: list[SparkRowID] = []

        def _emit(ids: list, idx: int, start: int) -> DocumentChunk:
            refs = [
                self._make_document_ref(identifier, start + offset)
                for offset, identifier in enumerate(ids)
            ]

            return DocumentChunk(source=self.source, refs=refs, chunk_index=idx)

        for identifier in ids_gen:
            partition_value = identifier[0]
            if current_ids and partition_value != current_partition:
                yield _emit(current_ids, chunk_index, source_index)

                chunk_index += 1
                source_index += len(current_ids)
                current_ids = []

            current_partition = partition_value
            current_ids.append(identifier)

        if current_ids:
            yield _emit(current_ids, chunk_index, source_index)

    @_map_spark_source_errors
    def _fetch_document_by_id(
        self, identifier: SparkRowID, *, max_file_size: int | None = None
    ) -> DocumentStream:
        # Rows arrive grouped by partition: on the first access to a partition,
        # cache the whole partition keyed by row_key, then serve siblings from
        # the cache, one backend read per partition instead of per document.
        partition_value, row_key, _ = identifier
        if self._cached_partition != partition_value:
            self._partition_cache = {
                rk: (data, name or f"{rk}.bin")
                for rk, data, name in self._backend.read_partition(
                    self._coords.table,
                    self._coords.content_column,
                    self._partition_col,
                    partition_value,
                    self._coords.filename_column,
                )
            }
            self._cached_partition = partition_value

        data, name = self._partition_cache[row_key]
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
        if self._coords.partition_column is None:
            _log.info(
                "Spark source reading single-node on the driver (no partition_column). "
                "Under a distributed orchestrator set partition_column for per-partition reads."
            )
        limit = normalize_max_file_size(max_file_size)

        yielded = 0
        skipped_null = 0
        documents = self._backend.stream_documents(
            self._coords.table,
            self._coords.content_column,
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
