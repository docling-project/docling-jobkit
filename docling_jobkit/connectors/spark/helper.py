from typing import TYPE_CHECKING

from docling_jobkit.datamodel.spark_coords import DatabricksAuth, SparkConnection

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


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
        if isinstance(conn.auth, DatabricksAuth):
            params.append(f"x-databricks-cluster-id={conn.auth.cluster_id}")
    if not params:
        return base

    return base + "/;" + ";".join(params)


def get_spark_session(conn: SparkConnection) -> "SparkSession":
    """Create a remote Spark Connect session from conn config"""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote(build_remote_url(conn)).getOrCreate()

    return spark
