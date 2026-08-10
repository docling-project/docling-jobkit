"""This backend is for the Spark Connect Path, used for local spark and databricks classic"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator, Sequence

from docling_jobkit.connectors.spark.helper import (
    build_row_schema,
    get_spark_session,
    merge_with_retry,
)
from docling_jobkit.datamodel.spark_coords import SparkConnection

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class SparkConnectBackend:
    """Spark Connect transport (local OSS, token, Databricks classic).

    Holds a remote SparkSession and performs all reads/writes via the DataFrame
    API.
    """

    def __init__(self, conn: SparkConnection) -> None:
        self._spark: SparkSession = get_spark_session(conn)

    def close(self) -> None:
        """TODO: 1 line docstring"""

    def _non_null_limted(
        self, table: str, content_column: str, max_num_elements: int | None
    ) -> DataFrame:
        """Table rows with non-null content, optionally capped at max_num_elements."""
        from pyspark.sql.functions import col

        non_null_content_df = self._spark.table(table).filter(
            col(content_column).isNotNull()
        )
        if max_num_elements is not None:
            non_null_content_df = non_null_content_df.limit(max_num_elements)

        return non_null_content_df

    def table_exists(self, table: str) -> bool:
        return bool(self._spark.catalog.tableExists(table))

    def count_documents(
        self, table: str, content_column: str, max_num_elements: int | None
    ) -> int:
        """Count rows with non-null content (respecting max_num_elements)."""
        return int(
            self._non_null_limted(table, content_column, max_num_elements).count()
        )

    def enumerate_row_keys(
        self,
        table: str,
        content_column: str,
        partition_column: str,
        filename_column: str | None,
        max_num_elements: int | None,
    ) -> Iterator[tuple[object, str, str | None]]:
        """Yield (partition_value, sha2 row_key, filename) ordered by partition."""
        from pyspark.sql.functions import col, sha2

        non_null_content_df = self._non_null_limted(
            table, content_column, max_num_elements
        )
        selects = [
            col(partition_column),
            sha2(col(content_column), 256).alias("row_key"),
        ]
        if filename_column:
            selects.append(col(filename_column).alias("fname"))

        ordered_df = non_null_content_df.select(*selects).orderBy(col(partition_column))
        for row in ordered_df.toLocalIterator():
            yield (
                row[partition_column],
                row["row_key"],
                (row["fname"] if filename_column else None),
            )

    def read_partition(
        self,
        table: str,
        content_column: str,
        partition_column: str,
        partition_value: object,
        filename_column: str | None,
    ) -> Iterator[tuple[str, bytes, str | None]]:
        """Yield (sha2 row_key, content bytes, filename) for one partition's rows."""
        from pyspark.sql.functions import col, sha2

        selects = [
            sha2(col(content_column), 256).alias("row_key"),
            col(content_column).alias("c"),
        ]
        if filename_column:
            selects.append(col(filename_column).alias("fname"))

        partition_df = (
            self._spark.table(table)
            .filter(col(partition_column) == partition_value)  # type: ignore[arg-type]
            .filter(col(content_column).isNotNull())
            .select(*selects)
        )
        for row in partition_df.toLocalIterator():
            yield (
                row["row_key"],
                row["c"],
                (row["fname"] if filename_column else None),
            )

    def stream_documents(
        self,
        table: str,
        content_column: str,
        filename_column: str | None,
        max_num_elements: int | None,
    ) -> Iterator[tuple[bytes, str | None]]:
        """Yield (content bytes, filename) for every non-null row."""
        from pyspark.sql.functions import col

        non_null_content_df = self._non_null_limted(
            table, content_column, max_num_elements
        )
        selects = [col(content_column).alias("c")]
        if filename_column:
            selects.append(col(filename_column).alias("fname"))

        projected_df = non_null_content_df.select(*selects)
        for row in projected_df.toLocalIterator():
            yield (row["c"], (row["fname"] if filename_column else None))

    def write_rows(
        self,
        table: str,
        columns: Sequence[str],
        int_columns: set[str],
        rows: list[dict[str, Any]],
        *,
        key: str,
        table_format: str,
    ) -> None:
        """Append rows, or MERGE them into an existing delta table (idempotent upsert)."""
        rows_df = self._spark.createDataFrame(
            rows, schema=build_row_schema(columns, int_columns)
        )
        if table_format == "delta" and self._spark.catalog.tableExists(table):
            view = f"_docling_spark_merge_src{id(self)}"
            merge_with_retry(self._spark, rows_df, table, view, key)
        else:
            rows_df.write.format(table_format).mode("append").saveAsTable(table)
