import base64
from datetime import datetime, timezone
from pathlib import Path

import pytest

from docling.datamodel.base_models import ConversionStatus, OutputFormat
from docling.datamodel.service.sources import FileSource, HttpSource
from docling.datamodel.service.targets import PresignedUrlTarget, S3Target
from docling_core.types.doc.document import DoclingDocument

from docling_jobkit.config.target_config import S3PresignedConfig
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
    def save_as_json(self, filename, image_mode, artifacts_dir):
        del image_mode
        Path(filename).write_text('{"ok": true}', encoding="utf-8")
        artifact_path = Path(filename).parent / artifacts_dir
        artifact_path.mkdir(parents=True, exist_ok=True)
        (artifact_path / "figure.png").write_bytes(b"png")


def _make_exportable_document(
    *, filename: str = "paper.pdf", status: ConversionStatus = ConversionStatus.SUCCESS
) -> ExportableDocument:
    return ExportableDocument(
        file=Path(filename),
        status=status,
        document=_FakeDoc.model_construct(),
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
        ),
        url_expiration=600,
    )


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
        str(key).startswith(f"converted/{today}/tenant-a/task-123/")
        for key in uploaded_keys
    )
    assert any(str(key).endswith("paper.json") for key in uploaded_keys)
    assert any(str(key).endswith("paper_bundle.zip") for key in uploaded_keys)

    metadata = fake_client.uploads[0]["extra_args"]["Metadata"]
    assert metadata == {"tenant_id": "tenant-a", "user_id": "user-1"}
    assert all(
        str(artifact.uri).startswith("https://example.com/converted/")
        for artifact in document.artifacts
    )
    assert all(artifact.url_expires_at is not None for artifact in document.artifacts)


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
    assert task_result.num_partial_success == 0
    assert task_result.num_failed == 0
    assert [document.source_index for document in task_result.result.documents] == [
        0,
        1,
    ]


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
    assert all(
        str(key).startswith("converted/task-123/000000-") for key in uploaded_keys
    )
    assert any(str(key).endswith("/paper.json") for key in uploaded_keys)
    assert any(str(key).endswith("/paper_bundle.zip") for key in uploaded_keys)


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
    assert all(key.startswith("converted/task-123/") for key in uploaded_keys)
    assert uploaded_keys[0] != uploaded_keys[1]


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
    assert task_result.num_partial_success == 1
    assert task_result.num_failed == 0
