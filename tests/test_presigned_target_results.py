import base64
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import ClassVar
from unittest.mock import MagicMock

import pytest

from docling.datamodel.base_models import ConversionStatus, OutputFormat
from docling.datamodel.service.callbacks import CallbackSpec, ProgressKind
from docling.datamodel.service.sources import FileSource, HttpSource
from docling.datamodel.service.targets import PresignedUrlTarget, S3Target
from docling_core.types.doc import ImageRefMode
from docling_core.types.doc.document import DoclingDocument

from docling_jobkit.config.target_config import S3PresignedConfig
from docling_jobkit.connectors.artifact_paths import build_s3_source_key
from docling_jobkit.connectors.s3_helper import check_target_has_source_converted
from docling_jobkit.convert.results import process_exportable_results
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.exportable_document import ExportableDocument
from docling_jobkit.datamodel.result import PresignedArtifactResult, RemoteTargetResult
from docling_jobkit.datamodel.s3_coords import S3Coordinates
from docling_jobkit.datamodel.task import Task


class _FakeS3Client:
    def __init__(self):
        self.uploads: list[dict[str, object]] = []

    def upload_file(self, Filename, Bucket, Key, ExtraArgs):
        self.uploads.append(
            {
                "bucket": Bucket,
                "key": Key,
                "content": Path(Filename).read_bytes(),
                "extra_args": ExtraArgs,
            }
        )

    def upload_fileobj(self, Fileobj, Bucket, Key, ExtraArgs):
        self.uploads.append(
            {
                "bucket": Bucket,
                "key": Key,
                "content": Fileobj.read(),
                "extra_args": ExtraArgs,
            }
        )

    def generate_presigned_url(self, ClientMethod, Params, ExpiresIn):
        del ClientMethod
        return f"https://example.com/{Params['Key']}?expires={ExpiresIn}"

    def close(self):
        return None


class _FakeDoc(DoclingDocument):
    markdown_image_modes: ClassVar[list[ImageRefMode]] = []

    def save_as_json(self, filename, image_mode, artifacts_dir):
        del image_mode
        Path(filename).write_text('{"ok": true}', encoding="utf-8")
        artifact_path = Path(filename).parent / artifacts_dir
        artifact_path.mkdir(parents=True, exist_ok=True)
        (artifact_path / "figure.png").write_bytes(b"png")

    def export_to_markdown(self, image_mode=ImageRefMode.PLACEHOLDER, **_kwargs):
        self.__class__.markdown_image_modes.append(image_mode)
        return "converted markdown"


def _make_exportable_document(
    *,
    filename: str = "paper.pdf",
    status: ConversionStatus = ConversionStatus.SUCCESS,
    source_index: int | None = None,
    source_uri: str | None = None,
) -> ExportableDocument:
    return ExportableDocument(
        file=Path(filename),
        status=status,
        document=_FakeDoc.model_construct(pages={}, tables=[], pictures=[]),
        source_index=source_index,
        source_uri=source_uri,
    )


def _make_task() -> Task:
    return Task(
        task_id="task-123",
        sources=[
            FileSource(
                base64_string=base64.b64encode(b"fake-pdf").decode(),
                filename="paper.pdf",
            )
        ],
        target=PresignedUrlTarget(),
        convert_options=ConvertDocumentsOptions(to_formats=[OutputFormat.JSON]),
        metadata={"tenant_id": "tenant-a", "user_id": "user-1"},
    )


def _make_s3_presigned_config() -> S3PresignedConfig:
    return S3PresignedConfig(
        s3_coords=S3Coordinates(
            endpoint="s3.example.com",
            access_key="key",
            secret_key="secret",
            bucket="converted-docs",
            key_prefix="converted/",
        ),
        url_expiration=600,
    )


def _short_hash(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()[:12]


def test_process_exportable_results_requires_presigned_target_config(tmp_path: Path):
    task = _make_task()

    with pytest.raises(
        ValueError,
        match=r"requires s3_presigned_config",
    ):
        process_exportable_results(
            task=task,
            exportable_documents=[_make_exportable_document()],
            work_dir=tmp_path,
        )


def test_process_exportable_results_returns_presigned_artifact_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    fake_client = _FakeS3Client()
    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_target_processor.get_s3_connection",
        lambda _coords: (fake_client, object()),
    )

    task_result = process_exportable_results(
        task=_make_task(),
        exportable_documents=[_make_exportable_document()],
        work_dir=tmp_path,
        s3_presigned_config=_make_s3_presigned_config(),
    )

    assert isinstance(task_result.result, PresignedArtifactResult)
    assert task_result.num_converted == 1
    assert task_result.num_succeeded == 1
    assert task_result.num_failed == 0

    document = task_result.result.documents[0]
    assert document.source_index == 0
    assert document.source_uri == "paper.pdf"
    assert document.filename == "paper.pdf"
    assert {artifact.artifact_type for artifact in document.artifacts} == {
        "json",
        "resource_bundle",
    }

    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    uploaded_keys = [item["key"] for item in fake_client.uploads]
    assert len(uploaded_keys) == 2
    assert all(
        str(key).startswith(f"converted/tenant-a/{today}/task-123/")
        for key in uploaded_keys
    )
    assert all(f"/{_short_hash('paper.pdf')}/" in str(key) for key in uploaded_keys)
    assert any(str(key).endswith("paper.json") for key in uploaded_keys)
    assert any(str(key).endswith("paper_bundle.zip") for key in uploaded_keys)

    metadata = fake_client.uploads[0]["extra_args"]["Metadata"]
    assert metadata == {"tenant_id": "tenant-a", "user_id": "user-1"}
    assert all(
        str(artifact.uri).startswith("https://example.com/converted/")
        for artifact in document.artifacts
    )
    assert all(artifact.url_expires_at is not None for artifact in document.artifacts)


def test_process_exportable_results_omits_default_tenant_id_from_object_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    fake_client = _FakeS3Client()
    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_target_processor.get_s3_connection",
        lambda _coords: (fake_client, object()),
    )
    task = _make_task().model_copy(update={"metadata": {"user_id": "user-1"}})

    process_exportable_results(
        task=task,
        exportable_documents=[_make_exportable_document()],
        work_dir=tmp_path,
        s3_presigned_config=_make_s3_presigned_config(),
    )

    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    uploaded_keys = [str(item["key"]) for item in fake_client.uploads]
    assert len(uploaded_keys) == 2
    assert all(
        key.startswith(f"converted/default/{today}/task-123/") for key in uploaded_keys
    )
    metadata = fake_client.uploads[0]["extra_args"]["Metadata"]
    assert metadata == {"user_id": "user-1"}


def test_process_exportable_results_reuses_presigned_artifact_result_for_multi_doc_presigned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    fake_client = _FakeS3Client()
    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_target_processor.get_s3_connection",
        lambda _coords: (fake_client, object()),
    )

    task_result = process_exportable_results(
        task=_make_task(),
        exportable_documents=[_make_exportable_document(), _make_exportable_document()],
        work_dir=tmp_path,
        s3_presigned_config=_make_s3_presigned_config(),
    )

    assert isinstance(task_result.result, PresignedArtifactResult)
    assert task_result.num_succeeded == 2
    assert task_result.num_failed == 0
    assert [document.source_index for document in task_result.result.documents] == [
        0,
        1,
    ]


def test_presigned_target_duplicate_source_uris_share_storage_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    fake_client = _FakeS3Client()
    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_target_processor.get_s3_connection",
        lambda _coords: (fake_client, object()),
    )

    task_result = process_exportable_results(
        task=_make_task().model_copy(
            update={
                "sources": [
                    FileSource(
                        base64_string=base64.b64encode(b"fake-pdf-1").decode(),
                        filename="paper.pdf",
                    ),
                    FileSource(
                        base64_string=base64.b64encode(b"fake-pdf-2").decode(),
                        filename="paper.pdf",
                    ),
                ]
            }
        ),
        exportable_documents=[
            _make_exportable_document(source_uri="paper.pdf", source_index=0),
            _make_exportable_document(source_uri="paper.pdf", source_index=1),
        ],
        work_dir=tmp_path,
        s3_presigned_config=_make_s3_presigned_config(),
    )

    assert isinstance(task_result.result, PresignedArtifactResult)
    assert [document.source_index for document in task_result.result.documents] == [
        0,
        1,
    ]
    json_uris = [
        next(
            str(artifact.uri)
            for artifact in document.artifacts
            if artifact.artifact_type == "json"
        )
        for document in task_result.result.documents
    ]
    assert json_uris[0] == json_uris[1]

    uploaded_keys = [
        str(item["key"])
        for item in fake_client.uploads
        if str(item["key"]).endswith(".json")
    ]
    assert len(uploaded_keys) == 2
    assert uploaded_keys[0] == uploaded_keys[1]


def test_process_exportable_results_returns_remote_target_result_for_s3_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    fake_client = _FakeS3Client()
    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_target_processor.get_s3_connection",
        lambda _coords: (fake_client, object()),
    )
    task = _make_task().model_copy(
        update={
            "target": S3Target(
                endpoint="s3.example.com",
                access_key="key",
                secret_key="secret",
                bucket="converted-docs",
                key_prefix="converted/",
            )
        }
    )

    task_result = process_exportable_results(
        task=task,
        exportable_documents=[_make_exportable_document()],
        work_dir=tmp_path,
    )

    assert isinstance(task_result.result, RemoteTargetResult)
    uploaded_keys = [item["key"] for item in fake_client.uploads]
    assert len(uploaded_keys) == 2
    assert all(str(key).startswith("converted/") for key in uploaded_keys)
    assert all("/task-123/" not in str(key) for key in uploaded_keys)
    assert all(f"{_short_hash('paper.pdf')}/" in str(key) for key in uploaded_keys)
    assert any(str(key).endswith("/paper.json") for key in uploaded_keys)
    assert any(str(key).endswith("/paper_bundle.zip") for key in uploaded_keys)


def test_presigned_callbacks_emit_document_completed_after_uploads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    events: list[tuple[str, object]] = []

    class _RecordingS3Client(_FakeS3Client):
        def upload_file(self, Filename, Bucket, Key, ExtraArgs):
            super().upload_file(Filename, Bucket, Key, ExtraArgs)
            events.append(("upload", Key))

    fake_client = _RecordingS3Client()
    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_target_processor.get_s3_connection",
        lambda _coords: (fake_client, object()),
    )

    callback_invoker = MagicMock()
    callback_invoker.invoke_callbacks_async.side_effect = lambda **kwargs: (
        events.append(("callback", kwargs["progress"]))
    )

    process_exportable_results(
        task=_make_task().model_copy(
            update={"callbacks": [CallbackSpec(url="http://callback.example")]}
        ),
        exportable_documents=[_make_exportable_document()],
        work_dir=tmp_path,
        s3_presigned_config=_make_s3_presigned_config(),
        callback_invoker=callback_invoker,
    )

    kinds = [event[1].kind for event in events if event[0] == "callback"]
    assert kinds == [
        ProgressKind.SET_NUM_DOCS,
        ProgressKind.DOCUMENT_COMPLETED,
        ProgressKind.UPDATE_PROCESSED,
    ]
    doc_completed_index = next(
        index
        for index, event in enumerate(events)
        if event[0] == "callback" and event[1].kind == ProgressKind.DOCUMENT_COMPLETED
    )
    assert [event[0] for event in events[: doc_completed_index + 1]] == [
        "callback",
        "upload",
        "upload",
        "callback",
    ]


def test_document_completed_num_characters_uses_placeholder_image_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    fake_client = _FakeS3Client()
    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_target_processor.get_s3_connection",
        lambda _coords: (fake_client, object()),
    )
    _FakeDoc.markdown_image_modes = []

    callback_invoker = MagicMock()
    process_exportable_results(
        task=_make_task().model_copy(
            update={"callbacks": [CallbackSpec(url="http://callback.example")]}
        ),
        exportable_documents=[_make_exportable_document()],
        work_dir=tmp_path,
        s3_presigned_config=_make_s3_presigned_config(),
        callback_invoker=callback_invoker,
    )

    document_completed = next(
        call.kwargs["progress"]
        for call in callback_invoker.invoke_callbacks_async.call_args_list
        if call.kwargs["progress"].kind == ProgressKind.DOCUMENT_COMPLETED
    )
    assert document_completed.document.num_characters == len("converted markdown")
    assert _FakeDoc.markdown_image_modes == [ImageRefMode.PLACEHOLDER]


def test_presigned_remote_exports_release_document_references_and_cleanup_temp_dirs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    fake_client = _FakeS3Client()
    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_target_processor.get_s3_connection",
        lambda _coords: (fake_client, object()),
    )
    exportable_documents = [
        _make_exportable_document(source_index=0),
        _make_exportable_document(source_index=1),
    ]

    process_exportable_results(
        task=_make_task(),
        exportable_documents=exportable_documents,
        work_dir=tmp_path,
        s3_presigned_config=_make_s3_presigned_config(),
    )

    assert all(document.document is None for document in exportable_documents)
    assert (tmp_path / "output").exists()
    assert list((tmp_path / "output").iterdir()) == []


def test_presigned_upload_failure_becomes_failed_document_callback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    class _FailingS3Client(_FakeS3Client):
        def upload_file(self, Filename, Bucket, Key, ExtraArgs):
            del Filename, Bucket, Key, ExtraArgs
            raise RuntimeError("upload failed")

    fake_client = _FailingS3Client()
    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_target_processor.get_s3_connection",
        lambda _coords: (fake_client, object()),
    )

    callback_invoker = MagicMock()
    task_result = process_exportable_results(
        task=_make_task().model_copy(
            update={"callbacks": [CallbackSpec(url="http://callback.example")]}
        ),
        exportable_documents=[_make_exportable_document()],
        work_dir=tmp_path,
        s3_presigned_config=_make_s3_presigned_config(),
        callback_invoker=callback_invoker,
        debug_error_details=True,
    )

    assert isinstance(task_result.result, PresignedArtifactResult)
    assert task_result.num_succeeded == 0
    assert task_result.num_failed == 1
    assert task_result.result.documents[0].status == ConversionStatus.FAILURE
    assert task_result.result.documents[0].artifacts == []

    document_completed = next(
        call.kwargs["progress"]
        for call in callback_invoker.invoke_callbacks_async.call_args_list
        if call.kwargs["progress"].kind == ProgressKind.DOCUMENT_COMPLETED
    )
    assert document_completed.document.status == ConversionStatus.FAILURE
    assert document_completed.document.error == "RuntimeError: upload failed"

    final_update = next(
        call.kwargs["progress"]
        for call in callback_invoker.invoke_callbacks_async.call_args_list
        if call.kwargs["progress"].kind == ProgressKind.UPDATE_PROCESSED
    )
    assert final_update.num_failed == 1
    assert final_update.docs[0].status == ConversionStatus.FAILURE


def test_s3_target_uses_distinct_task_scoped_keys_for_same_basename_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    fake_client = _FakeS3Client()
    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_target_processor.get_s3_connection",
        lambda _coords: (fake_client, object()),
    )
    task = Task(
        task_id="task-123",
        sources=[
            HttpSource(url="https://example.com/docs/paper.pdf"),
            HttpSource(url="https://example.org/reports/paper.pdf"),
        ],
        target=S3Target(
            endpoint="s3.example.com",
            access_key="key",
            secret_key="secret",
            bucket="converted-docs",
            key_prefix="converted/",
        ),
        convert_options=ConvertDocumentsOptions(to_formats=[OutputFormat.JSON]),
        metadata={"tenant_id": "tenant-a", "user_id": "user-1"},
    )

    process_exportable_results(
        task=task,
        exportable_documents=[
            _make_exportable_document(),
            _make_exportable_document(),
        ],
        work_dir=tmp_path,
    )

    uploaded_keys = [
        str(item["key"])
        for item in fake_client.uploads
        if str(item["key"]).endswith(".json")
    ]
    assert len(uploaded_keys) == 2
    assert all(key.startswith("converted/") for key in uploaded_keys)
    assert all("/task-123/" not in key for key in uploaded_keys)
    assert uploaded_keys[0] != uploaded_keys[1]


def test_s3_target_uses_s3_source_coordinates_hash_for_s3_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    fake_client = _FakeS3Client()
    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_target_processor.get_s3_connection",
        lambda _coords: (fake_client, object()),
    )
    task = Task(
        task_id="task-123",
        sources=[
            S3Coordinates(
                endpoint="s3.source.example.com",
                access_key="source-key",
                secret_key="source-secret",
                bucket="source-bucket",
                key_prefix="incoming/documents",
            )
        ],
        target=S3Target(
            endpoint="s3.example.com",
            access_key="key",
            secret_key="secret",
            bucket="converted-docs",
            key_prefix="converted/",
        ),
        convert_options=ConvertDocumentsOptions(to_formats=[OutputFormat.JSON]),
    )

    process_exportable_results(
        task=task,
        exportable_documents=[_make_exportable_document()],
        work_dir=tmp_path,
    )

    expected_source_hash = _short_hash("s3://source-bucket/incoming/documents")
    uploaded_keys = [str(item["key"]) for item in fake_client.uploads]
    assert len(uploaded_keys) == 2
    assert all(
        key.startswith(f"converted/{expected_source_hash}/") for key in uploaded_keys
    )


def test_check_target_has_source_converted_uses_hashed_source_root(
    monkeypatch: pytest.MonkeyPatch,
):
    target_coords = S3Coordinates(
        endpoint="s3.target.example.com",
        access_key="key",
        secret_key="secret",
        bucket="converted-docs",
        key_prefix="converted/",
    )
    source_coords = S3Coordinates(
        endpoint="s3.source.example.com",
        access_key="source-key",
        secret_key="source-secret",
        bucket="source-bucket",
        key_prefix="incoming/documents",
    )
    expected_source_hash = build_s3_source_key(source_coords)

    seen_prefixes: list[str] = []

    class _FakePaginator:
        pass

    class _FakeClient:
        def get_paginator(self, _name):
            return _FakePaginator()

    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_helper.get_s3_connection",
        lambda _coords: (_FakeClient(), object()),
    )

    def _fake_count(_paginator, _bucket, prefix):
        seen_prefixes.append(prefix)
        return 1

    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_helper.count_s3_objects",
        _fake_count,
    )
    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_helper.get_keys_s3_objects_as_set",
        lambda _resource, _bucket, prefix: {
            f"{prefix}paper.json",
        },
    )

    filtered = check_target_has_source_converted(
        target_coords,
        ["incoming/documents/paper.pdf", "incoming/documents/other.pdf"],
        source_coords,
    )

    assert seen_prefixes == [f"converted/{expected_source_hash}/json/"]
    assert filtered == ["incoming/documents/other.pdf"]


def test_process_exportable_results_tracks_partial_success_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    fake_client = _FakeS3Client()
    monkeypatch.setattr(
        "docling_jobkit.connectors.s3_target_processor.get_s3_connection",
        lambda _coords: (fake_client, object()),
    )

    task_result = process_exportable_results(
        task=_make_task(),
        exportable_documents=[
            _make_exportable_document(status=ConversionStatus.SUCCESS),
            _make_exportable_document(status=ConversionStatus.PARTIAL_SUCCESS),
        ],
        work_dir=tmp_path,
        s3_presigned_config=_make_s3_presigned_config(),
    )

    assert task_result.num_succeeded == 1
    assert task_result.num_partially_succeeded == 1
    assert task_result.num_failed == 0
