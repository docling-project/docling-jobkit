"""This backend is for the Spark SQL Path, used for databricks serverless mode"""

from __future__ import annotations

import uuid
from typing import Any, Iterator, Sequence

from docling_jobkit.connectors.spark.helper import (
    _IDENT,
    create_table_sql,
    insert_sql,
    merge_sql,
    quote_identifier,
    retry_on_merge_conflict,
    staging_name,
)
from docling_jobkit.datamodel.spark_coords import SparkConnection

_FETCH = 500


class DatabricksSqlBackend:
    """Serverless transport: SQL over a databricks-sql-connector cursor."""

    def __init__(self, conn: SparkConnection) -> None:
        from databricks import sql

        auth = conn.auth
        self._conn = sql.connect(
            server_hostname=conn.host,
            http_path=auth.http_path,  # type: ignore[union-attr]
            access_token=auth.token.get_secret_value(),  # type: ignore[union-attr]
        )

        # Unique per backend instance so concurrent workers / co-resident doc+chunk
        # processors never share a staging table (uuid, not id(self), is not reused
        # across processes).
        self._staging_token = uuid.uuid4().hex

    def close(self) -> None:
        """Close the underlying serverless SQL session (a dedicated, non-shared connection)."""
        self._conn.close()

    def _query(self, sql_text: str, params: list[Any] | None = None) -> Iterator[tuple]:
        """Execute `sql_text` and yield rows in _FETCH-sized batches (bounds driver memory)."""
        with self._conn.cursor() as cursor:
            cursor.execute(sql_text, params)

            while True:
                batch = cursor.fetchmany(_FETCH)
                if not batch:
                    return

                yield from batch

    def table_exists(self, table: str) -> bool:
        """True if `table` exists, via SHOW TABLES ... LIKE (no exception-string scraping)."""
        parts = table.split(".")
        name = parts[-1]
        if not name or not _IDENT.match(name):
            raise ValueError(f"invalid SQL identifier: {table!r}")

        schema = ".".join(parts[:-1])
        in_clause = f" IN {quote_identifier(schema)}" if schema else ""

        with self._conn.cursor() as cursor:
            cursor.execute(f"SHOW TABLES{in_clause} LIKE '{name}'")

            return cursor.fetchone() is not None

    def count_documents(
        self, table: str, content_column: str, max_num_elements: int | None
    ) -> int:
        """Count rows with non-null content, capped at max_num_elements when set."""
        table_ref = quote_identifier(table)
        content_ref = quote_identifier(content_column)
        if max_num_elements is not None:
            inner = (
                f"SELECT 1 FROM {table_ref} WHERE {content_ref} IS NOT NULL "
                f"LIMIT {int(max_num_elements)}"
            )
            sql_text = f"SELECT COUNT(*) FROM ({inner})"
        else:
            sql_text = (
                f"SELECT COUNT(*) FROM {table_ref} WHERE {content_ref} IS NOT NULL"
            )

        return int(next(self._query(sql_text))[0])

    def enumerate_row_keys(
        self,
        table: str,
        content_column: str,
        partition_column: str,
        filename_column: str | None,
        max_num_elements: int | None,
    ) -> Iterator[tuple[object, str, str | None]]:
        """Yield (partition_value, sha2 row_key, filename) ordered by partition."""
        table_ref = quote_identifier(table)
        content_ref = quote_identifier(content_column)
        partition_ref = quote_identifier(partition_column)
        select_expr = f"{partition_ref}, sha2({content_ref}, 256) AS row_key"
        if filename_column:
            select_expr += f", {quote_identifier(filename_column)} AS fname"

        sql_text = (
            f"SELECT {select_expr} FROM {table_ref} "
            f"WHERE {content_ref} IS NOT NULL ORDER BY {partition_ref}"
        )
        if max_num_elements is not None:
            sql_text += f" LIMIT {int(max_num_elements)}"
        for row in self._query(sql_text):
            yield (row[0], row[1], (row[2] if filename_column else None))

    def read_partition(
        self,
        table: str,
        content_column: str,
        partition_column: str,
        partition_value: object,
        filename_column: str | None,
    ) -> Iterator[tuple[str, bytes, str | None]]:
        """Yield (sha2 row_key, content bytes, filename) for one partition's non-null rows."""
        table_ref = quote_identifier(table)
        content_ref = quote_identifier(content_column)
        partition_ref = quote_identifier(partition_column)
        select_expr = f"sha2({content_ref}, 256) AS row_key, {content_ref} AS c"
        if filename_column:
            select_expr += f", {quote_identifier(filename_column)} AS fname"

        sql_text = (
            f"SELECT {select_expr} FROM {table_ref} "
            f"WHERE {partition_ref} = ? AND {content_ref} IS NOT NULL"
        )
        for row in self._query(sql_text, [partition_value]):
            yield (row[0], bytes(row[1]), (row[2] if filename_column else None))

    def stream_documents(
        self,
        table: str,
        content_column: str,
        filename_column: str | None,
        max_num_elements: int | None,
    ) -> Iterator[tuple[bytes, str | None]]:
        """Yield (content bytes, filename) for every non-null row (single-driver read)."""
        table_ref = quote_identifier(table)
        content_ref = quote_identifier(content_column)
        select_expr = f"{content_ref} AS c"
        if filename_column:
            select_expr += f", {quote_identifier(filename_column)} AS fname"

        sql_text = (
            f"SELECT {select_expr} FROM {table_ref} WHERE {content_ref} IS NOT NULL"
        )
        if max_num_elements is not None:
            sql_text += f" LIMIT {int(max_num_elements)}"
        for row in self._query(sql_text):
            yield (bytes(row[0]), (row[1] if filename_column else None))

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
        payload = [[row.get(col) for col in columns] for row in rows]
        with self._conn.cursor() as cursor:
            cursor.execute(create_table_sql(table, columns, int_columns, table_format))
            if table_format != "delta":
                cursor.executemany(insert_sql(table, columns), payload)
                return

            staging = staging_name(table, self._staging_token)
            try:
                cursor.execute(create_table_sql(staging, columns, int_columns, "delta"))
                cursor.execute(f"TRUNCATE TABLE {quote_identifier(staging)}")
                cursor.executemany(insert_sql(staging, columns), payload)
                retry_on_merge_conflict(
                    lambda: cursor.execute(merge_sql(table, staging, key))
                )
            finally:
                # Never leave staging tables littering the user's schema.
                cursor.execute(f"DROP TABLE IF EXISTS {quote_identifier(staging)}")
