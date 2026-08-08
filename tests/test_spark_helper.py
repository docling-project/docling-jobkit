import pytest

pytest.importorskip("pyspark")

from docling_jobkit.connectors.spark import (
    build_remote_url,
    is_spark_authentication_error,
    is_spark_unavailable_error,
)
from docling_jobkit.datamodel.spark_coords import SparkConnection


def _conn(**overrides) -> SparkConnection:
    base = {"host": "myhost", "port": 15002}
    base.update(overrides)

    return SparkConnection(**base)  # type: ignore


def test_remote_url_local_no_auth():
    url = build_remote_url(_conn())

    assert url == "sc://myhost:15002"


def test_remote_url_with_token_and_ssl():
    url = build_remote_url(_conn(port=443, auth={"kind": "token", "token": "T0K"}))

    assert url.startswith("sc://myhost:443/;")
    assert "token=T0K" in url
    assert "use_ssl=true" in url


def test_remote_url_databricks_includes_cluster():
    url = build_remote_url(
        _conn(port=443, auth={"kind": "databricks", "token": "T0K", "cluster_id": "c1"})
    )

    assert "token=T0K" in url
    assert "x-databricks-cluster-id=c1" in url


def test_remote_url_includes_user_id_when_set():
    url = build_remote_url(_conn(user_id="alice", auth={"kind": "token", "token": "t"}))

    assert "user_id=alice" in url


def test_remote_url_local_no_auth_with_user_id():
    # A no-auth connection carrying only user_id still emits the param.
    url = build_remote_url(_conn(user_id="alice"))
    assert url == "sc://myhost:15002/;user_id=alice"


@pytest.mark.parametrize(
    "message, is_auth, is_unavail",
    [
        ("StatusCode.UNAUTHENTICATED: bad token", True, False),
        ("PERMISSION_DENIED for table", True, False),
        ("StatusCode.UNAVAILABLE: connection refused", False, True),
        ("AnalysisException: table not found", False, False),
    ],
    ids=["unauth", "denied", "unavailable", "other"],
)
def test_error_predicates(message, is_auth, is_unavail):
    exc = RuntimeError(message)

    assert is_spark_authentication_error(exc) is is_auth
    assert is_spark_unavailable_error(exc) is is_unavail
