from __future__ import annotations

from typing import Union

from docling_jobkit.connectors.spark.backend_connect import SparkConnectBackend
from docling_jobkit.connectors.spark.backend_sql import DatabricksSqlBackend
from docling_jobkit.datamodel.spark_coords import (
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
        return DatabricksSqlBackend(conn)

    return SparkConnectBackend(conn)
