from __future__ import annotations

from typing import Union

from docling_jobkit.connectors.spark.backend_connect import SparkConnectBackend
from docling_jobkit.connectors.spark.backend_sql import DatabricksSqlBackend
from docling_jobkit.connectors.spark.models import (
    DatabricksServerlessAuth,
    SparkConnection,
)

# The transport union both processors hold and call through.
SparkBackend = Union[SparkConnectBackend, DatabricksSqlBackend]


def get_backend(conn: SparkConnection) -> SparkBackend:
    """Return the transport backend for this connection.

    databricks_serverless -> SQL warehouse (DatabricksSqlBackend);
    everything else (local / token / databricks_classic) -> Spark Connect.
    """
    if isinstance(conn.auth, DatabricksServerlessAuth):
        try:
            import databricks.sql  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "databricks_serverless auth requires the 'databricks-sql-connector' "
                "package; install it with the 'spark_serverless' extra "
            ) from exc

        return DatabricksSqlBackend(conn)

    try:
        import pyspark  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "This Spark connection requires the 'pyspark' package; install it "
            "with the 'spark_connect' extra "
        ) from exc

    return SparkConnectBackend(conn)
