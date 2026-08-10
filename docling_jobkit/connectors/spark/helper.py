from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any, Iterable, Sequence

from docling_jobkit.datamodel.spark_coords import DatabricksClassicAuth, SparkConnection

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


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

    for attempt in range(max_retries):
        try:
            spark.sql(sql)
            return
        except Exception as exc:
            if is_merge_conflict(exc) and attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
                continue
            raise
