from __future__ import annotations

import json
import logging
import re
import time
from io import BytesIO
from typing import TYPE_CHECKING, Any, Callable, Iterable, Sequence

import requests

from docling_jobkit.connectors.errors import (
    SourceConnectorPolicyError,
    SourceConnectorUnavailableError,
)
from docling_jobkit.connectors.spark.models import (
    DatabricksClassicAuth,
    SparkConnection,
)

_log = logging.getLogger(__name__)

_MAX_RETRIES = 3
_BACKOFF_BASE_S = 0.5
_RETRYABLE_4XX_STATUS = {429}


def _with_exponential_retry(fn: Callable[[], Any], operation: str) -> Any:
    """Helper for exponential retries on transient errors."""
    for attempt in range(_MAX_RETRIES + 1):
        try:
            result = fn()
            if isinstance(result, requests.Response):
                result.raise_for_status()
            return result
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt == _MAX_RETRIES:
                raise SourceConnectorUnavailableError(
                    "Source document could not be reached.",
                    source_kind="spark",
                ) from exc
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if (
                status is not None
                and status < 500
                and status not in _RETRYABLE_4XX_STATUS
            ):
                error_type = (
                    SourceConnectorPolicyError
                    if status in {401, 403, 404, 413, 415, 422}
                    else SourceConnectorUnavailableError
                )
                raise error_type(
                    str(exc),
                    source_kind="spark",
                    **(
                        {"retryable": False}
                        if error_type is SourceConnectorUnavailableError
                        else {}
                    ),
                ) from exc
            if attempt == _MAX_RETRIES:
                raise SourceConnectorUnavailableError(
                    str(exc),
                    source_kind="spark",
                ) from exc

        wait = _BACKOFF_BASE_S * (2**attempt)
        _log.warning(
            "Spark: %s transient error, retry %d/%d in %.1fs",
            operation,
            attempt + 1,
            _MAX_RETRIES,
            wait,
        )
        time.sleep(wait)

    raise AssertionError("unreachable")


def download_document_from_url(url: str, auth_token: str) -> BytesIO:
    """Download document from Databricks Files API URL with Bearer token auth."""
    response = _with_exponential_retry(
        lambda: requests.get(
            url,
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=30,
        ),
        "download document",
    )
    return BytesIO(response.content)


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

# Strict allowlist for table/column identifiers: validate, then backtick-quote.
# Anything outside this charset is rejected rather than escaped.
_IDENT = re.compile(r"^[A-Za-z0-9_.]+$")


def is_spark_authentication_error(exc: BaseException) -> bool:
    AUTH_MARKERS = ("UNAUTHENTICATED", "PERMISSION_DENIED", "UNAUTHORIZED")
    text = str(exc).upper()

    return any(m.upper() in text for m in AUTH_MARKERS)


def is_spark_unavailable_error(exc: BaseException) -> bool:
    UNAVAILABLE_MARKERS = ("UNAVAILABLE", "DEADLINE_EXCEEDED", "CONNECTION REFUSED")
    text = str(exc).upper()

    return any(m.upper() in text for m in UNAVAILABLE_MARKERS)


def is_merge_conflict(exc: BaseException) -> bool:
    MERGE_CONFLICT_MARKERS = ("ConcurrentAppend", "ConcurrentModification", "conflict")
    text = str(exc).upper()

    return any(m.upper() in text for m in MERGE_CONFLICT_MARKERS)


def build_remote_url(conn: SparkConnection) -> str:
    """Assemble Spark Connect `sc://` connection string from config"""
    base = f"sc://{conn.host}:{conn.port}"
    params: list[str] = []

    if conn.user_id:
        params.append(f"user_id={conn.user_id}")
    if conn.auth is not None:
        params.append(f"token={conn.auth.token.get_secret_value()}")
        params.append("use_ssl=true")
        if isinstance(conn.auth, DatabricksClassicAuth):
            params.append(f"x-databricks-cluster-id={conn.auth.cluster_id}")
    if not params:
        return base

    return base + "/;" + ";".join(params)


def get_spark_session(conn: SparkConnection) -> "SparkSession":
    """Create a remote Spark Connect session from conn config"""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote(build_remote_url(conn)).getOrCreate()

    return spark


def normalize_row(row: dict[str, Any], columns: Sequence[str]) -> dict[str, Any]:
    """Project `row` to `columns`, JSON-encoding dict/list values so every
    non-int column is a plain string (keeps the Spark schema trivial + Connect-safe)."""
    out: dict[str, Any] = {}
    for col in columns:
        val = row.get(col)
        if isinstance(val, (dict, list)):
            val = json.dumps(val, ensure_ascii=False)
        out[col] = val

    return out


def build_row_schema(columns: Sequence[str], int_columns: Iterable[str]) -> StructType:
    """All-string StructType for `columns`, typing `int_columns` as LongType.

    Explicit (not inferred) so createDataFrame doesn't choke on always-null columns."""
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    ints = set(int_columns)

    return StructType(
        [
            StructField(c, LongType() if c in ints else StringType(), True)
            for c in columns
        ]
    )


def retry_on_merge_conflict(fn: Callable[[], Any], max_retries: int = 3) -> None:
    """Run `fn` (a MERGE), retrying with backoff on Delta concurrency conflicts."""
    for attempt in range(max_retries):
        try:
            fn()
            return
        except Exception as exc:
            if is_merge_conflict(exc) and attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
                continue
            raise


def merge_with_retry(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    view: str,
    key: str,
    max_retries: int = 3,
) -> None:
    """Idempotent Delta MERGE upsert of `df` into `table` keyed on `key`, retrying
    on Delta concurrency conflicts (mirrors AstraDB's upsert_record_with_retry)."""
    df.createOrReplaceTempView(view)

    sql = (
        f"MERGE INTO {table} t USING {view} s "
        f"ON t.{key} = s.{key} "
        f"WHEN MATCHED THEN UPDATE SET * "
        f"WHEN NOT MATCHED THEN INSERT *"
    )

    retry_on_merge_conflict(lambda: spark.sql(sql), max_retries)


# helper SQL builer methods for databricks sql serverless path


def quote_identifier(name: str) -> str:
    """Validate a table/column identifier and backtick-quote each dotted part."""
    if not name or not _IDENT.match(name):
        raise ValueError(f"invalid SQL identifier: {name!r}")

    return ".".join(f"`{part}`" for part in name.split("."))


def staging_name(table: str, token: str) -> str:
    """A per-instance staging table in the target's schema (catalog.schema._docling_stg_<token>)."""
    parts = table.split(".")
    parts[-1] = f"_docling_stg_{token}"

    return ".".join(parts)


def _col_types(columns: Sequence[str], int_columns: set[str]) -> str:
    return ", ".join(
        f"{quote_identifier(c)} {'BIGINT' if c in int_columns else 'STRING'}"
        for c in columns
    )


def create_table_sql(
    table: str, columns: Sequence[str], int_columns: set[str], table_format: str
) -> str:
    """CREATE TABLE IF NOT EXISTS with all-string/BIGINT columns and the given format."""
    using = "DELTA" if table_format == "delta" else table_format.upper()
    return (
        f"CREATE TABLE IF NOT EXISTS {quote_identifier(table)} "
        f"({_col_types(columns, int_columns)}) USING {using}"
    )


def insert_sql(table: str, columns: Sequence[str]) -> str:
    """Parameter-bound INSERT (VALUES uses `?` placeholders, never interpolated data)."""
    cols = ", ".join(quote_identifier(c) for c in columns)
    marks = ", ".join(["?"] * len(columns))
    return f"INSERT INTO {quote_identifier(table)} ({cols}) VALUES ({marks})"


def merge_sql(table: str, staging: str, key: str) -> str:
    """Static idempotent MERGE template keyed on `key` (never varies with data)."""
    key_ref = quote_identifier(key)
    return (
        f"MERGE INTO {quote_identifier(table)} t USING {quote_identifier(staging)} s "
        f"ON t.{key_ref} = s.{key_ref} "
        f"WHEN MATCHED THEN UPDATE SET * "
        f"WHEN NOT MATCHED THEN INSERT *"
    )
