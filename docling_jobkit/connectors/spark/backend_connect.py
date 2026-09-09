"""This backend is for the Spark Connect Path, used for local spark and databricks classic"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator, Sequence

from docling_jobkit.connectors.spark.helper import (
    build_row_schema,
    get_spark_session,
    merge_with_retry,
)
from docling_jobkit.connectors.spark.models import SparkConnection

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class SparkConnectBackend:
    """Spark Connect transport (local OSS, token, Databricks classic).

    Holds a dedicated remote SparkSession (never shared with another backend
    instance, even one with an identical connection) and performs all
    reads/writes via the DataFrame API.
    """

    def __init__(self, conn: SparkConnection) -> None:
        self._spark: SparkSession = get_spark_session(conn)

    def close(self) -> None:
        """Stop this backend's own session."""
        self._spark.stop()

    def _non_null_df(
        self, table_or_query: str, is_query: bool, content_column: str
    ) -> DataFrame:
        """Table/query rows with non-null content (no limit applied)."""
        from pyspark.sql.functions import col

        if is_query:
            non_null_content_df = self._spark.sql(table_or_query)
        else:
            non_null_content_df = self._spark.table(table_or_query)

        return non_null_content_df.filter(col(content_column).isNotNull())

    def _non_null_limited(
        self,
        table_or_query: str,
        is_query: bool,
        content_column: str,
        max_num_elements: int | None,
    ) -> DataFrame:
        """Table/query rows with non-null content, optionally capped at max_num_elements.

        Only safe for callers that don't order the result afterward — LIMIT
        must be applied after ORDER BY for deterministic results, see
        `enumerate_row_keys`.
        """
        non_null_content_df = self._non_null_df(
            table_or_query, is_query, content_column
        )
        if max_num_elements is not None:
            non_null_content_df = non_null_content_df.limit(max_num_elements)

        return non_null_content_df

    def table_exists(self, table: str) -> bool:
        return bool(self._spark.catalog.tableExists(table))

    def count_documents(
        self,
        table_or_query: str,
        is_query: bool,
        content_column: str,
        max_num_elements: int | None,
    ) -> int:
        """Count rows with non-null content (respecting max_num_elements)."""
        return int(
            self._non_null_limited(
                table_or_query, is_query, content_column, max_num_elements
            ).count()
        )

    def enumerate_row_keys(
        self,
        table_or_query: str,
        is_query: bool,
        content_column: str,
        partition_column: str | None,
        filename_column: str | None,
        max_num_elements: int | None,
        id_column: str | None = None,
    ) -> Iterator[tuple[object, str, str | None]]:
        """Yield (partition_value, row_key, filename) optionally ordered by partition.

        row_key is the id_column value when id_column is set (fetch_by_id then
        looks the row up the same way, with no hashing involved), otherwise
        sha2(content, 256), which fetch_by_content_hash recomputes to match.
        Deriving row_key from id_column avoids hashing content_column here only
        to read and hash it again on fetch.
        """
        from pyspark.sql.functions import col, sha2

        row_key_expr = (
            col(id_column).cast("string")
            if id_column
            else sha2(col(content_column), 256)
        ).alias("row_key")

        if partition_column:
            non_null_content_df = self._non_null_df(
                table_or_query, is_query, content_column
            )
            selects = [
                col(partition_column),
                row_key_expr,
            ]
            if filename_column:
                selects.append(col(filename_column).alias("fname"))

            ordered_df = non_null_content_df.select(*selects).orderBy(
                col(partition_column)
            )
            if max_num_elements is not None:
                ordered_df = ordered_df.limit(max_num_elements)
            for row in ordered_df.toLocalIterator():
                yield (
                    row[partition_column],
                    row["row_key"],
                    (row["fname"] if filename_column else None),
                )
        else:
            # No partition column: partition_value is always None, so ordering
            # doesn't matter and the limit can be applied up front.
            non_null_content_df = self._non_null_limited(
                table_or_query, is_query, content_column, max_num_elements
            )
            selects = [row_key_expr]
            if filename_column:
                selects.append(col(filename_column).alias("fname"))

            df_ = non_null_content_df.select(*selects)
            for row in df_.toLocalIterator():
                yield (
                    None,
                    row["row_key"],
                    (row["fname"] if filename_column else None),
                )

    def enumerate_urls(
        self,
        table_or_query: str,
        is_query: bool,
        url_column: str,
        id_column: str,
        filename_column: str | None,
        max_num_elements: int | None,
    ) -> Iterator[tuple[str, str, str | None]]:
        """Yield (id_value, url, filename) for url-column mode."""
        from pyspark.sql.functions import col

        if is_query:
            # Use sql() for queries
            df_ = self._spark.sql(table_or_query)
        else:
            # Use table() for table names
            df_ = self._spark.table(table_or_query)

        df_ = df_.filter(col(url_column).isNotNull())

        selects = [col(id_column).alias("id"), col(url_column).alias("url")]
        if filename_column:
            selects.append(col(filename_column).alias("fname"))

        if max_num_elements is not None:
            df_ = df_.limit(max_num_elements)

        projected_df = df_.select(*selects)
        for row in projected_df.toLocalIterator():
            yield (
                str(row["id"]),
                str(row["url"]),
                (str(row["fname"]) if filename_column else None),
            )

    def fetch_by_id(
        self,
        table_or_query: str,
        is_query: bool,
        id_column: str,
        id_value: str,
        content_column: str,
        filename_column: str | None,
    ) -> Iterator[tuple[bytes, str]]:
        """Fetch one row by id_column value. Yields (content_bytes, filename)."""
        from pyspark.sql.functions import col

        if is_query:
            df_ = self._spark.sql(table_or_query)
        else:
            df_ = self._spark.table(table_or_query)

        cols = [content_column]
        if filename_column:
            cols.append(filename_column)

        df_ = df_.where(col(id_column) == id_value).select(*cols).limit(1)

        for row in df_.toLocalIterator():
            fname = row[1] if filename_column else f"{id_value}.bin"
            yield (bytes(row[0]), fname)

    def fetch_by_content_hash(
        self,
        table_or_query: str,
        is_query: bool,
        content_column: str,
        sha2_hash: str,
        filename_column: str | None,
    ) -> Iterator[tuple[bytes, str]]:
        """Fetch one row by sha2(content,256). Yields (content_bytes, filename)."""
        from pyspark.sql.functions import col, sha2

        if is_query:
            df_ = self._spark.sql(table_or_query)
        else:
            df_ = self._spark.table(table_or_query)

        cols = [content_column]
        if filename_column:
            cols.append(filename_column)

        df_ = (
            df_.where(sha2(col(content_column), 256) == sha2_hash)
            .select(*cols)
            .limit(1)
        )

        for row in df_.toLocalIterator():
            fname = row[1] if filename_column else f"{sha2_hash}.bin"
            yield (bytes(row[0]), fname)

    def stream_documents(
        self,
        table_or_query: str,
        is_query: bool,
        content_column: str,
        filename_column: str | None,
        max_num_elements: int | None,
    ) -> Iterator[tuple[bytes, str | None]]:
        """Yield (content bytes, filename) for every non-null row."""
        from pyspark.sql.functions import col

        non_null_content_df = self._non_null_limited(
            table_or_query, is_query, content_column, max_num_elements
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
