import pytest

pytest.importorskip("pyspark")

from docling_jobkit.connectors.spark import (
    build_remote_url,
    create_table_sql,
    insert_sql,
    is_spark_authentication_error,
    is_spark_unavailable_error,
    merge_sql,
    quote_identifier,
)
from docling_jobkit.connectors.spark.helper import get_spark_session
from docling_jobkit.connectors.spark.models import SparkConnection


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


def test_remote_url_databricks_classic_includes_cluster():
    url = build_remote_url(
        _conn(
            port=443,
            auth={"kind": "databricks_classic", "token": "T0K", "cluster_id": "c1"},
        )
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


class _FakeSession:
    def __init__(self, url):
        self.url = url
        self.stopped = False

    def stop(self):
        self.stopped = True


class _FakeBuilder:
    def remote(self, url):
        self._url = url
        return self

    def create(self):
        return _FakeSession(self._url)


def test_get_spark_session_creates_dedicated_session_per_call(monkeypatch):
    """Each call gets its own session bound to its own conn — never shared,
    even across two calls with identical connections. `.create()` (not
    `.getOrCreate()`) guarantees this: `.getOrCreate()` would return whatever
    session is already active/default in the process regardless of the URL
    just built, silently ignoring a second connector's own host/token."""
    import pyspark.sql as pyspark_sql

    monkeypatch.setattr(pyspark_sql.SparkSession, "builder", _FakeBuilder())

    conn_a = _conn(auth={"kind": "token", "token": "A"})
    conn_b = _conn(auth={"kind": "token", "token": "B"})

    session_a1 = get_spark_session(conn_a)
    session_a2 = get_spark_session(conn_a)  # same conn, still a distinct session
    session_b = get_spark_session(conn_b)

    assert session_a1 is not session_a2
    assert session_a1 is not session_b
    assert session_a1.url == session_a2.url
    assert session_a1.url != session_b.url


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


@pytest.mark.parametrize(
    "name, expected",
    [
        ("cat.db.docs", "`cat`.`db`.`docs`"),
        ("content", "`content`"),
    ],
    ids=["dotted", "single"],
)
def test_quote_identifier_backticks_each_part(name, expected):
    assert quote_identifier(name) == expected


@pytest.mark.parametrize(
    "bad",
    ["", "a;drop", "a b", "a`b", "a-b"],
    ids=["empty", "semicolon", "space", "backtick", "dash"],
)
def test_quote_identifier_rejects_injection(bad):
    with pytest.raises(ValueError):
        quote_identifier(bad)


def test_insert_sql_uses_placeholders_not_values():
    sql = insert_sql("cat.db.out", ["doc_id", "text"])

    assert sql == "INSERT INTO `cat`.`db`.`out` (`doc_id`, `text`) VALUES (?, ?)"


def test_merge_sql_is_static_and_keyed():
    sql = merge_sql("cat.db.out", "cat.db._docling_stg_1", "doc_id")

    assert "MERGE INTO `cat`.`db`.`out` t USING `cat`.`db`.`_docling_stg_1` s" in sql
    assert "ON t.`doc_id` = s.`doc_id`" in sql
    assert "WHEN MATCHED THEN UPDATE SET *" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql


@pytest.mark.parametrize(
    "fmt, using",
    [("delta", "USING DELTA"), ("parquet", "USING PARQUET")],
    ids=["delta", "parquet"],
)
def test_create_table_types_and_format(fmt, using):
    sql = create_table_sql("cat.db.out", ["doc_id", "n"], {"n"}, fmt)
    assert "`doc_id` STRING" in sql
    assert "`n` BIGINT" in sql
    assert using in sql
