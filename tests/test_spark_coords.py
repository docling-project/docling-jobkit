import pytest
from pydantic import SecretStr, ValidationError

from docling_jobkit.datamodel.spark_coords import (
    DatabricksAuth,
    SparkChunkTarget,
    SparkDocTarget,
    TaskSparkSource,
    TokenAuth,
)


def _src(**overrides):
    base = {
        "host": "localhost",
        "table": "cat.db.docs",
        "content_column": "content",
        "port": 15002,
    }
    base.update(overrides)

    return TaskSparkSource(**base)  # type: ignore


def test_source_defaults_and_kind():
    src = _src()

    assert src.kind == "spark"
    assert src.port == 15002
    assert src.filename_column is None
    assert src.max_num_elements is None
    assert src.auth is None  # omitted auth → local/no-auth


def test_content_column_is_required():
    with pytest.raises(ValidationError):
        TaskSparkSource(host="h", table="t", port=15002)  # type: ignore


@pytest.mark.parametrize(
    "auth, expected_type",
    [
        ({"kind": "token", "token": "abc"}, TokenAuth),
        ({"kind": "databricks", "token": "abc", "cluster_id": "c1"}, DatabricksAuth),
    ],
    ids=["token", "databricks"],
)
def test_auth_discriminated_union(auth, expected_type):
    src = _src(auth=auth)
    assert isinstance(src.auth, expected_type)


def test_token_is_secret():
    src = _src(auth={"kind": "token", "token": "supersecret"})

    assert isinstance(src.auth.token, SecretStr)  # type: ignore
    assert "supersecret" not in str(src.auth)


def test_omitted_auth_defaults_to_local():
    assert _src().auth is None


def test_doc_target_defaults():
    t = SparkDocTarget(
        host="h", table="cat.db.out", mappings={"MARKDOWN": "text"}, port=15002
    )

    assert t.kind == "spark_doc"
    assert t.doc_id_field == "doc_id"
    assert t.flush_batch_size == 100


def test_chunk_target_defaults():
    t = SparkChunkTarget(host="h", table="cat.db.chunks", port=15002)

    assert t.kind == "spark_chunks"
    assert t.chunk_id_field == "chunk_id"
    assert t.text_field == "text"  # inherited ChunkFieldSlots default
    assert t.chunk_index_field == "chunk_index"
