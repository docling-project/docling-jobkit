"""Tests for the pluggy connector factory, dynamic unions, transport chunk,
and the orchestrator target allowlist."""

from typing import Iterator, Literal

import pytest
from pydantic import BaseModel

from docling.datamodel.base_models import DocumentStream
from docling.datamodel.service.requests import S3SourceRequest
from docling.datamodel.service.sources import FileSource, HttpSource, S3Coordinates
from docling.datamodel.service.targets import InBodyTarget, S3Target

from docling_jobkit.connectors.connector_factory import (
    SourceConnectorFactory,
    TargetConnectorFactory,
    get_source_connector_factory,
    get_target_connector_factory,
)
from docling_jobkit.connectors.source_processor import (
    BaseSourceProcessor,
    DocumentChunk,
    SourceDocumentRef,
)
from docling_jobkit.connectors.target_processor import BaseTargetProcessor
from docling_jobkit.datamodel.task_targets import GoogleDriveTarget, LocalPathTarget


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


def test_create_instance_bare_base_fallback():
    """A bare S3Coordinates (no kind) must resolve to the S3 processor that is
    registered under the S3SourceRequest subclass."""
    factory = get_source_connector_factory()
    proc = factory.create_instance(
        S3Coordinates(
            endpoint="e", bucket="b", access_key="k", secret_key="s", key_prefix="p/"
        )
    )
    assert type(proc).__name__ == "S3SourceProcessor"

    http = factory.create_instance(HttpSource(url="https://example.com/a.pdf"))
    assert type(http).__name__ == "HttpSourceProcessor"
    file = factory.create_instance(FileSource(base64_string="", filename="a.pdf"))
    assert type(file).__name__ == "HttpSourceProcessor"


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


class _FakeSourceProcessor(BaseSourceProcessor):
    def __init__(self, source: _FakeSource):
        super().__init__(source)

    @classmethod
    def get_config_types(cls):
        return (_FakeSource,)

    def _initialize(self): ...

    def _finalize(self): ...

    def _fetch_documents(self, *, max_file_size=None) -> Iterator[DocumentStream]:
        yield from ()


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
