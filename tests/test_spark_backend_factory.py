from unittest.mock import MagicMock

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("databricks.sql")

from docling_jobkit.connectors.spark.backend_connect import SparkConnectBackend
from docling_jobkit.connectors.spark.backend_factory import get_backend
from docling_jobkit.connectors.spark.backend_sql import DatabricksSqlBackend
from docling_jobkit.connectors.spark.models import TaskSparkSource


def _src(auth):
    return TaskSparkSource(
        host="h", port=443, table="c.d.t", content_column="content", auth=auth
    )


@pytest.mark.parametrize(
    "auth",
    [
        None,
        {"kind": "token", "token": "t"},
        {"kind": "databricks_classic", "token": "t", "cluster_id": "c1"},
    ],
    ids=["local", "token", "classic"],
)
def test_routes_to_spark_connect(monkeypatch, auth):
    monkeypatch.setattr(
        "docling_jobkit.connectors.spark.backend_connect.get_spark_session",
        lambda conn: MagicMock(),
    )

    assert isinstance(get_backend(_src(auth)), SparkConnectBackend)


def test_routes_serverless_to_sql(monkeypatch):
    created = {}
    monkeypatch.setattr(
        "databricks.sql.connect",
        lambda **kw: created.update(kw) or MagicMock(),
    )
    backend = get_backend(
        _src(
            {
                "kind": "databricks_serverless",
                "token": "t",
                "http_path": "/sql/1.0/warehouses/w1",
            }
        )
    )

    assert isinstance(backend, DatabricksSqlBackend)
    assert created["http_path"] == "/sql/1.0/warehouses/w1"
    assert created["access_token"] == "t"
