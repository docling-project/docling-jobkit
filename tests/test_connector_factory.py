"""Tests for connector registration, hydration, transport, and dispatch."""

import json
from io import BytesIO
from typing import Iterator, Literal

import pytest
from pydantic import BaseModel, SecretStr, ValidationError

from docling.datamodel.base_models import DocumentStream
from docling.datamodel.service.requests import (
    AnyHttpSourceRequest,
    FileSourceRequest,
    HttpSourceRequest,
    S3SourceRequest,
)
from docling.datamodel.service.sources import S3Coordinates
from docling.datamodel.service.targets import InBodyTarget, S3Target

from docling_jobkit.connectors.connector_factory import (
    SourceConnectorFactory,
    TargetConnectorFactory,
    get_source_connector_factory,
    get_target_connector_factory,
)
from docling_jobkit.connectors.errors import SourceConnectorConfigError
from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    DocumentChunk,
    SourceDocumentRef,
)
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.task import Task, validate_task, validate_task_json
from docling_jobkit.datamodel.task_sources import TaskFileNetSource
from docling_jobkit.datamodel.task_targets import GoogleDriveTarget, LocalPathTarget
from docling_jobkit.orchestrators.serialization import dump_model_with_secrets


def test_builtin_source_connectors_registered():
    factory = get_source_connector_factory()
    assert set(factory.registered_kinds) == {
        "filenet",
        "azure_blob",
        "file",
        "google_cloud_storage",
        "http",
        "s3",
        "local_path",
        "google_drive",
    }


def test_builtin_target_connectors_registered():
    factory = get_target_connector_factory()
    assert set(factory.registered_kinds) == {
        "azure_blob",
        "google_cloud_storage",
        "s3",
        "local_path",
        "put",
        "google_drive",
    }


def test_create_instance_exact_and_subtype_match():
    factory = get_source_connector_factory()
    # Discriminated subtype (YAML path)
    proc = factory.create_instance(
        S3SourceRequest(
            endpoint="e", bucket="b", access_key="k", secret_key="s", key_prefix="p/"
        )
    )
    assert type(proc).__name__ == "S3SourceProcessor"


def test_kindless_base_config_does_not_guess_registered_subtype():
    factory = get_source_connector_factory()
    source = S3Coordinates(
        endpoint="e", bucket="b", access_key="k", secret_key="s", key_prefix="p/"
    )
    with pytest.raises(SourceConnectorConfigError, match=r"requires.*kind"):
        factory.validate_config(source)
    with pytest.raises(RuntimeError, match="No connector found"):
        factory.create_instance(source)


def test_target_factory_dispatch():
    factory = get_target_connector_factory()
    assert (
        type(factory.create_instance(LocalPathTarget(path="./out"))).__name__
        == "LocalPathTargetProcessor"
    )
    assert (
        type(
            factory.create_instance(
                S3Target(
                    endpoint="e",
                    bucket="b",
                    access_key="k",
                    secret_key="s",
                    key_prefix="p/",
                )
            )
        ).__name__
        == "S3TargetProcessor"
    )


def test_unknown_config_raises():
    factory = TargetConnectorFactory()
    factory.load_from_plugins()
    with pytest.raises(RuntimeError):
        factory.create_instance(InBodyTarget())  # service-only, never registered


# --- External plugin registration + dynamic unions ----------------------------


class _OneDriveTarget(BaseModel):
    kind: Literal["onedrive"] = "onedrive"
    drive_id: str


class _OneDriveTargetProcessor(BaseTargetProcessor):
    def __init__(self, target: _OneDriveTarget):
        super().__init__()
        self._target = target

    @classmethod
    def get_config_types(cls):
        return (_OneDriveTarget,)

    def _initialize(self):  # pragma: no cover - trivial
        ...

    def _finalize(self):  # pragma: no cover - trivial
        ...

    def upload_file(self, filename, target_filename, content_type):  # pragma: no cover
        ...

    def upload_object(self, obj, target_filename, content_type):  # pragma: no cover
        ...


def test_external_connector_registration_and_union():
    factory = TargetConnectorFactory()
    factory.load_from_plugins()  # built-ins
    factory.register(_OneDriveTargetProcessor, "test_plugin", "tests.fake")
    assert "onedrive" in factory.registered_kinds

    union_adapter = factory.build_discriminated_union()
    from pydantic import TypeAdapter

    parsed = TypeAdapter(union_adapter).validate_python(
        {"kind": "onedrive", "drive_id": "d1"}
    )
    assert isinstance(parsed, _OneDriveTarget)
    assert parsed.drive_id == "d1"

    proc = factory.create_instance(_OneDriveTarget(drive_id="d2"))
    assert isinstance(proc, _OneDriveTargetProcessor)


def test_single_member_union_returns_bare_type():
    factory = TargetConnectorFactory()
    factory.register(_OneDriveTargetProcessor, "test_plugin", "tests.fake")
    # Only one registered type -> bare type, not an Annotated discriminated union.
    assert factory.build_discriminated_union() is _OneDriveTarget


# --- Transport chunk -----------------------------------------------------------


class _FakeSource(BaseModel):
    kind: Literal["fake"] = "fake"
    label: str = "ok"
    token: SecretStr = SecretStr("secret")


class _FakeSourceProcessor(BaseSourceProcessor):
    def __init__(self, source: _FakeSource):
        super().__init__(source)

    @classmethod
    def get_config_types(cls):
        return (_FakeSource,)

    def _initialize(self): ...

    def _finalize(self): ...

    def _fetch_documents(self, *, max_file_size=None) -> Iterator[DocumentStream]:
        del max_file_size
        yield DocumentStream(name="fake.pdf", stream=BytesIO(b"pdf"))


def test_document_chunk_is_plain_serializable():
    """A chunk carries only source + refs (no fetcher), so it is transport-safe."""
    refs = [
        SourceDocumentRef(id="a", source_index=0, source_uri="a", filename="a"),
    ]
    chunk = DocumentChunk(source=_FakeSource(), refs=refs, chunk_index=3)
    assert chunk.chunk_index == 3
    assert list(chunk.refs) == refs
    # Round-trips through pickle (the CLI mp.Pool path) without special handling.
    import pickle

    restored = pickle.loads(pickle.dumps(chunk))
    assert restored.chunk_index == 3
    assert restored.ids == ["a"]


def _source_factory_with_fake() -> SourceConnectorFactory:
    factory = SourceConnectorFactory()
    factory.load_from_plugins()
    factory.register(_FakeSourceProcessor, "fake_plugin", "fake_plugin.sources")
    return factory


def test_duplicate_kind_registration_and_plugin_processing_fail():
    class DuplicateFakeSource(BaseModel):
        kind: Literal["fake"] = "fake"

    class DuplicateFakeProcessor(_FakeSourceProcessor):
        @classmethod
        def get_config_types(cls):
            return (DuplicateFakeSource,)

    factory = SourceConnectorFactory()
    factory.register(_FakeSourceProcessor, "first", "first.sources")

    with pytest.raises(ValueError, match=r"kind 'fake'.*already registered"):
        factory.register(DuplicateFakeProcessor, "second", "second.sources")
    with pytest.raises(ValueError, match=r"kind 'fake'.*already registered"):
        factory.process_plugin(
            {"source_connectors": [DuplicateFakeProcessor]},
            "second",
            "second.sources",
        )


@pytest.mark.parametrize(
    "config_type",
    [
        type("MissingKind", (BaseModel,), {"__annotations__": {"value": str}}),
        type(
            "NonLiteralKind",
            (BaseModel,),
            {"__annotations__": {"kind": str}, "kind": "bad"},
        ),
        type(
            "MultiLiteralKind",
            (BaseModel,),
            {
                "__annotations__": {"kind": Literal["one", "two"]},
                "kind": "one",
            },
        ),
        type(
            "MismatchedKind",
            (BaseModel,),
            {"__annotations__": {"kind": Literal["expected"]}, "kind": "wrong"},
        ),
    ],
)
def test_registration_rejects_malformed_kind(config_type):
    class BadProcessor(_FakeSourceProcessor):
        @classmethod
        def get_config_types(cls):
            return (config_type,)

    with pytest.raises(ValueError, match="kind"):
        SourceConnectorFactory().register(BadProcessor, "bad", "bad.sources")


def test_registry_validates_filenet_and_http_canonical_models():
    factory = get_source_connector_factory()
    filenet = factory.validate_config(
        {
            "kind": "filenet",
            "base_url": "https://filenet.example.com/content-services-graphql",
            "username": "user",
            "api_key": "secret",
            "repository_id": "OS1",
        }
    )
    http = factory.validate_config(
        {"kind": "http", "url": "https://example.com/archive.zip"}
    )

    assert type(filenet) is TaskFileNetSource
    assert type(http) is AnyHttpSourceRequest
    with pytest.raises(ValidationError):
        HttpSourceRequest(url="https://example.com/archive.zip")


def test_builtin_capabilities_come_from_registered_processors():
    factory = get_source_connector_factory()

    assert factory.supports("filenet") is True
    assert factory.supports("missing") is False
    assert (
        factory.is_expandable(FileSourceRequest(filename="doc.pdf", base64_string=""))
        is False
    )
    assert (
        factory.is_expandable(AnyHttpSourceRequest(url="https://example.com/doc.pdf"))
        is False
    )
    assert (
        factory.is_expandable(
            S3SourceRequest(
                endpoint="e",
                bucket="b",
                access_key="k",
                secret_key="s",
                key_prefix="p/",
            )
        )
        is True
    )


def test_registry_errors_are_safe_and_identify_local_plugin():
    factory = _source_factory_with_fake()
    with pytest.raises(SourceConnectorConfigError) as exc_info:
        factory.validate_config(
            {"kind": "fake", "label": 1, "token": "credential-value"}
        )

    message = str(exc_info.value)
    assert "fake_plugin" in message
    assert "fake_plugin.sources" in message
    assert "credential-value" not in message
    assert "version" not in message.lower()


def test_task_structurally_hydrates_builtin_and_rejects_missing_kind():
    payload = {
        "task_id": "task-1",
        "sources": [
            {
                "kind": "s3",
                "endpoint": "e",
                "bucket": "b",
                "access_key": "k",
                "secret_key": "s",
                "key_prefix": "p/",
            }
        ],
    }
    rebuilt = Task.model_validate_json(Task.model_validate(payload).model_dump_json())

    assert type(rebuilt.sources[0]) is S3SourceRequest
    assert rebuilt.sources[0].bucket == "b"
    with pytest.raises(ValidationError, match=r"requires.*kind"):
        Task.model_validate({"task_id": "task-2", "sources": [{"bucket": "b"}]})


def test_filenet_survives_trusted_task_json_roundtrip():
    task = Task.model_validate(
        {
            "task_id": "task-filenet",
            "sources": [
                {
                    "kind": "filenet",
                    "base_url": "https://filenet.example.com/graphql",
                    "username": "user",
                    "api_key": "filenet-secret",
                    "repository_id": "OS1",
                }
            ],
        }
    )
    task_json = json.dumps(dump_model_with_secrets(task, serialize_as_any=True))
    rebuilt = Task.model_validate_json(task_json)

    assert type(rebuilt.sources[0]) is TaskFileNetSource
    assert rebuilt.sources[0].api_key.get_secret_value() == "filenet-secret"


def test_external_task_hydration_requires_context_and_roundtrips(monkeypatch):
    import docling_jobkit.connectors.connector_factory as connector_factory_module

    builtin_factory = get_source_connector_factory(False)
    external_factory = _source_factory_with_fake()
    monkeypatch.setattr(
        connector_factory_module,
        "get_source_connector_factory",
        lambda allow_external_plugins=False: (
            external_factory if allow_external_plugins else builtin_factory
        ),
    )
    payload = {
        "task_id": "task-1",
        "sources": [{"kind": "fake", "label": "preserved", "token": "value"}],
    }

    with pytest.raises(ValidationError, match="not registered"):
        Task.model_validate(payload)
    task = validate_task(payload, allow_external_plugins=True)
    task_json = json.dumps(dump_model_with_secrets(task, serialize_as_any=True))
    rebuilt = validate_task_json(task_json, allow_external_plugins=True)

    assert type(rebuilt.sources[0]) is _FakeSource
    assert rebuilt.sources[0].label == "preserved"
    assert external_factory.create_instance(rebuilt.sources[0]).source.label == (
        "preserved"
    )

    invalid = {
        "task_id": "task-invalid",
        "sources": [{"kind": "fake", "label": 1, "token": "credential-value"}],
    }
    with pytest.raises(ValidationError) as exc_info:
        validate_task(invalid, allow_external_plugins=True)
    message = str(exc_info.value)
    assert "fake_plugin" in message
    assert "credential-value" not in message


def test_rq_worker_uses_explicit_external_plugin_policy(monkeypatch, tmp_path):
    import docling_jobkit.connectors.connector_factory as connector_factory_module
    import docling_jobkit.orchestrators.rq.worker as rq_worker
    from docling_jobkit.orchestrators.rq.orchestrator import RQOrchestratorConfig

    factory = _source_factory_with_fake()
    monkeypatch.setattr(
        connector_factory_module,
        "get_source_connector_factory",
        lambda allow_external_plugins=False: factory,
    )
    captured = {}
    monkeypatch.setattr(
        rq_worker,
        "_run_docling_task",
        lambda task, *args: captured.setdefault("source", task.sources[0]),
    )

    result = rq_worker.docling_task(
        {"task_id": "task-rq", "sources": [{"kind": "fake"}]},
        object(),
        RQOrchestratorConfig(allow_external_plugins=True),
        tmp_path,
    )

    assert result is captured["source"]
    assert type(result) is _FakeSource


@pytest.mark.asyncio
async def test_ray_redis_hydration_uses_external_plugin_policy(monkeypatch):
    import docling_jobkit.connectors.connector_factory as connector_factory_module
    from docling_jobkit.orchestrators.ray.redis_helper import RedisStateManager

    factory = _source_factory_with_fake()
    monkeypatch.setattr(
        connector_factory_module,
        "get_source_connector_factory",
        lambda allow_external_plugins=False: factory,
    )

    class FakeRedis:
        async def lpop(self, key):
            del key
            return b'{"task_id":"task-ray","sources":[{"kind":"fake"}]}'

    manager = RedisStateManager("redis://localhost:6379/", allow_external_plugins=True)
    manager.redis = FakeRedis()

    async def ignore_limits(*args, **kwargs):
        return None

    manager.update_tenant_limits = ignore_limits
    task = await manager.dequeue_task("tenant")

    assert task is not None
    assert type(task.sources[0]) is _FakeSource


def test_factory_expandability_can_depend_on_config_type():
    class SingleSource(BaseModel):
        kind: Literal["single"] = "single"

    class MixedProcessor(_FakeSourceProcessor):
        @classmethod
        def get_config_types(cls):
            return (_FakeSource, SingleSource)

        @classmethod
        def is_expandable(cls, config):
            return isinstance(config, _FakeSource)

    factory = SourceConnectorFactory()
    factory.register(MixedProcessor, "mixed", "mixed.sources")

    assert factory.is_expandable(_FakeSource()) is True
    assert factory.is_expandable(SingleSource()) is False


def test_source_expansion_dispatches_fake_registered_processor(monkeypatch):
    import docling_jobkit.connectors.connector_factory as connector_factory_module
    import docling_jobkit.connectors.source_processor_factory as processor_factory_module

    factory = _source_factory_with_fake()
    monkeypatch.setattr(
        connector_factory_module,
        "get_source_connector_factory",
        lambda allow_external_plugins=False: factory,
    )
    monkeypatch.setattr(
        processor_factory_module,
        "get_source_connector_factory",
        lambda allow_external_plugins=False: factory,
    )
    task = validate_task(
        {"task_id": "task-1", "sources": [{"kind": "fake"}]},
        allow_external_plugins=True,
    )

    from docling_jobkit.convert.source_expansion import expand_task_sources

    sources, headers = expand_task_sources(task, allow_external_plugins=True)
    assert headers is None
    assert [source.name for source in sources] == ["fake.pdf"]


# --- Orchestrator allowlist ----------------------------------------------------


def test_validate_target_allowlist():
    from docling_jobkit.orchestrators.base_orchestrator import (
        BaseOrchestrator,
        TargetNotAllowedError,
    )

    class _Orch(BaseOrchestrator):
        async def enqueue(self, *a, **k): ...  # pragma: no cover
        async def queue_size(self): ...  # pragma: no cover
        async def get_queue_position(self, task_id): ...  # pragma: no cover
        async def process_queue(self): ...  # pragma: no cover
        async def warm_up_caches(self): ...  # pragma: no cover
        async def clear_converters(self): ...  # pragma: no cover
        async def check_connection(self): ...  # pragma: no cover
        async def task_result(self, task_id): ...  # pragma: no cover

    orch = _Orch()
    orch.allowed_target_kinds = {"s3"}

    # Allowed kind passes
    orch._validate_target(
        S3Target(
            endpoint="e", bucket="b", access_key="k", secret_key="s", key_prefix="p/"
        )
    )
    # Disallowed kind raises
    with pytest.raises(TargetNotAllowedError):
        orch._validate_target(LocalPathTarget(path="./out"))

    # None allowlist allows everything
    orch.allowed_target_kinds = None
    orch._validate_target(
        GoogleDriveTarget(
            path_id="x",
            refresh_token="refresh-token",
            credentials_path="/tmp/client-secret.json",
        )
    )


def test_factory_caches_are_isolated_by_flag():
    a = get_source_connector_factory(False)
    b = get_source_connector_factory(False)
    assert a is b
    assert isinstance(a, SourceConnectorFactory)


# --- Lazy / low-memory chunk consumption --------------------------------------


def test_open_chunk_sources_fetches_one_at_a_time(monkeypatch):
    """open_chunk_sources must fetch documents lazily, one per next(), so peak
    memory stays bounded to a single in-flight document regardless of batch size."""
    from io import BytesIO

    from docling_jobkit.convert import chunk_execution

    fetched: list[int] = []

    class _LazyProc(_FakeSourceProcessor):
        def fetch_converter_source_by_ref(self, ref, *, max_file_size=None):
            fetched.append(ref.id)
            return DocumentStream(name=str(ref.id), stream=BytesIO(b"x"))

    monkeypatch.setattr(
        chunk_execution,
        "get_source_processor",
        lambda source, **kwargs: _LazyProc(source),
    )

    refs = [
        SourceDocumentRef(id=i, source_index=i, source_uri=str(i), filename=str(i))
        for i in range(5)
    ]
    chunk = DocumentChunk(source=_FakeSource(), refs=refs, chunk_index=0)

    with chunk_execution.open_chunk_sources(chunk) as (sources, _headers):
        assert fetched == []  # nothing fetched until the converter pulls
        first = next(sources)
        assert first.name == "0"
        assert fetched == [0]  # exactly one in flight
        list(sources)
        assert fetched == [0, 1, 2, 3, 4]


def test_dynamic_job_config_parses_builtins_and_rejects_presigned():
    from docling_jobkit.datamodel.dynamic_unions import build_job_config_model

    model = build_job_config_model(allow_external_plugins=False)
    cfg = model(
        sources=[{"kind": "local_path", "path": "."}],
        target={"kind": "local_path", "path": "./out"},
    )
    assert cfg.target.kind == "local_path"
    assert cfg.sources[0].kind == "local_path"

    with pytest.raises(Exception):
        # presigned_url is a service-only target, never a storage connector
        model(sources=[], target={"kind": "presigned_url"})
