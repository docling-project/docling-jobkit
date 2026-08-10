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
    get_spark_session,
    is_spark_authentication_error,
    is_spark_unavailable_error,
)
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
        from pyspark.sql.functions import col

        self._spark = get_spark_session(self._coords)
        if not self._spark.catalog.tableExists(self._coords.table):
            raise SourceConnectorConfigError(
                f"Spark source table {self._coords.table!r} not found, verify the "
                f"catalog.schema.table name and that it exists on the cluster."
            )

        coords_df = self._spark.table(self._coords.table)
        _log.info("Spark source resolved table %s", self._coords.table)

        non_null_limited_df = coords_df.filter(
            col(self._coords.content_column).isNotNull()
        )

        if self._coords.max_num_elements is not None:
            non_null_limited_df = non_null_limited_df.limit(
                self._coords.max_num_elements
            )

        self._df = non_null_limited_df
        self._cached_partition = None
        self._partition_cache = {}

    def _finalize(self) -> None:
        self._cached_partition = None
        self._partition_cache = {}

    @_map_spark_source_errors
    def _count_documents(self) -> int:
        count = int(self._df.count())
        _log.debug("Spark source counted %d document(s)", count)

        return count

    @_map_spark_source_errors
    def _list_document_ids(self) -> Iterator[SparkRowID]:
        """TODO: add docstring emphasizing the difference between this and typical"""
        from pyspark.sql.functions import col, sha2

        partition_col = self._partition_col
        content_col = self._coords.content_column
        name_col = self._coords.filename_column

        selects = [col(partition_col), sha2(col(content_col), 256).alias("row_key")]
        if name_col:
            selects.append(col(name_col))

        spark_df = self._df.select(*selects).orderBy(col(partition_col))
        for row in spark_df.toLocalIterator():
            filename = row[name_col] if name_col else None

            yield SparkRowID(row[partition_col], row["row_key"], filename)

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
        current_ids: list = []  # TODO: add list typing

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
        from pyspark.sql.functions import col, sha2

        partition_value, row_key, _ = identifier
        if self._cached_partition != partition_value:
            partition_col = self._partition_col
            content_col = self._coords.content_column
            name_col = self._coords.filename_column

            selects = [
                sha2(col(content_col), 256).alias("row_key"),
                col(content_col).alias("_content"),
            ]
            if name_col:
                selects.append(col(name_col).alias("_name"))

            partition_df = (
                self._spark.table(self._coords.table)
                .filter(col(partition_col) == partition_value)  # type: ignore[arg-type]
                .filter(col(content_col).isNotNull())
                .select(*selects)
            )
            cache: dict = {}
            for row in partition_df.toLocalIterator():
                name = (row["_name"] if name_col else None) or f"{row['row_key']}.bin"
                cache[row["row_key"]] = (row["_content"], name)

            self._partition_cache = cache
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
        content_col = self._coords.content_column
        name_col = self._coords.filename_column

        yielded = 0
        skipped_null = 0
        for index, row in enumerate(self._df.toLocalIterator()):
            data = row[content_col]
            # Skip None or empty byte rows
            if not data:
                skipped_null += 1
                _log.warning(
                    "Skipping row %d: %s is null/empty", index, content_col
                )  # NOTE: this might spam logs?
                continue

            if limit is not None and len(data) > limit:
                name = row[name_col] if name_col else f"row-{index}"

                raise SourceLimitExceededError(
                    f"Source {name!r} exceeds max_file_size={limit} bytes"
                )

            name = (row[name_col] if name_col else None) or f"row-{index}.bin"
            yielded += 1

            yield DocumentStream(name=name, stream=BytesIO(data))

        _log.info(
            "Spark source yielded %d document(s), skipped %d null/empty row(s)",
            yielded,
            skipped_null,
        )
