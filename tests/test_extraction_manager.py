"""Extraction model-gating boundary + results-builder routing/counts.

Offline: exercises the operator-gating resolution (the access-control boundary)
and the result-envelope assembly without running any VLM inference.
"""

from io import BytesIO
from pathlib import PurePath
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from docling.datamodel.base_models import ConversionStatus, DocumentStream
from docling.datamodel.extraction import ExtractedPageData
from docling.datamodel.extraction_options import ChannelSelection, ExtractionVlmOptions
from docling.datamodel.service.callbacks import CallbackSpec, ProgressKind
from docling.datamodel.service.options import ExtractDocumentsOptions
from docling.datamodel.service.requests import S3SourceRequest
from docling.datamodel.service.responses import (
    ArtifactRef,
    DoclingTaskResult,
    DocumentArtifactItem,
    ExtractionTaskResult,
    PresignedArtifactResult,
)
from docling.datamodel.service.targets import InBodyTarget, PresignedUrlTarget, S3Target
from docling.datamodel.service.tasks import TaskType
from docling.datamodel.vlm_engine_options import ApiVlmEngineOptions
from docling.models.inference_engines.vlm.base import VlmEngineType

from docling_jobkit.connectors.artifact_paths import hash_path_component
from docling_jobkit.connectors.source_processor import DocumentChunk, SourceDocumentRef
from docling_jobkit.convert import (
    extraction_manager,
    extraction_results,
    source_expansion,
)
from docling_jobkit.convert.extraction_manager import (
    DocumentExtractionManager,
    DocumentExtractionManagerConfig,
)
from docling_jobkit.convert.extraction_results import (
    process_extraction_results,
    to_result_item,
)
from docling_jobkit.convert.source_expansion import expand_task_sources_with_identities
from docling_jobkit.datamodel.source_identity import SourceIdentity
from docling_jobkit.datamodel.task import Task

# --- model-selection gating (the security boundary) ------------------------


def test_default_preset_resolves_to_operator_model():
    ecm = DocumentExtractionManager(
        DocumentExtractionManagerConfig(default_extraction_preset="granite_vision_4_1")
    )
    vlm = ecm.resolve_extraction_model(ExtractDocumentsOptions(template="x"))
    expected = ExtractionVlmOptions.from_preset("granite_vision_4_1")
    assert vlm.model_spec.name == expected.model_spec.name


def test_preset_rejected_when_not_in_allow_list():
    ecm = DocumentExtractionManager(
        DocumentExtractionManagerConfig(
            allowed_extraction_presets=["granite_vision_4_1"]
        )
    )
    with pytest.raises(ValueError, match="not allowed"):
        ecm.resolve_extraction_model(
            ExtractDocumentsOptions(template="x", extraction_preset="nuextract_2b")
        )


def test_custom_config_rejected_unless_operator_opts_in():
    ecm = DocumentExtractionManager(DocumentExtractionManagerConfig())
    with pytest.raises(ValueError, match="not allowed"):
        ecm.resolve_extraction_model(
            ExtractDocumentsOptions(
                template="x", extraction_custom_config={"model_spec": {}}
            )
        )


def test_unknown_preset_reports_available_presets():
    ecm = DocumentExtractionManager(DocumentExtractionManagerConfig())
    with pytest.raises(ValueError, match="not found"):
        ecm.resolve_extraction_model(
            ExtractDocumentsOptions(template="x", extraction_preset="does_not_exist")
        )


def test_remote_engine_rejected_when_remote_services_are_disabled():
    custom = ExtractionVlmOptions.from_preset("nuextract_2b").model_copy(
        update={"engine_options": ApiVlmEngineOptions(engine_type=VlmEngineType.API)}
    )
    ecm = DocumentExtractionManager(
        DocumentExtractionManagerConfig(allow_custom_extraction_config=True)
    )

    with pytest.raises(ValueError, match="Remote extraction services are disabled"):
        ecm.resolve_extraction_model(
            ExtractDocumentsOptions(template="x", extraction_custom_config=custom)
        )


def test_equivalent_options_reuse_extractor(monkeypatch):
    init_count = 0
    seen = {}

    class FakeExtractor:
        def __init__(self, **kwargs):
            nonlocal init_count
            init_count += 1
            pipeline_options = next(
                iter(kwargs["extraction_format_options"].values())
            ).pipeline_options
            seen["channels"] = pipeline_options.input_channels
            seen["model"] = pipeline_options.vlm_options.model_spec.name

        def extract_all(self, *args, **kwargs):
            del args
            seen["template"] = kwargs["template"]
            return iter(())

    monkeypatch.setattr(extraction_manager, "DocumentExtractor", FakeExtractor)
    ecm = DocumentExtractionManager(DocumentExtractionManagerConfig())
    template = {"invoice": {"total": "number"}}
    options = ExtractDocumentsOptions(
        template=template,
        extraction_preset="nuextract_2b",
        input_channels=ChannelSelection.TEXT,
    )

    list(ecm.extract_documents([], options))
    list(ecm.extract_documents([], options))

    assert init_count == 1
    assert seen == {
        "channels": ChannelSelection.TEXT,
        "model": ExtractionVlmOptions.from_preset("nuextract_2b").model_spec.name,
        "template": template,
    }


# --- results bridging + envelope -------------------------------------------


def _facade_result(filename: str, status: ConversionStatus, pages):
    return SimpleNamespace(
        input=SimpleNamespace(file=PurePath(filename), format=None),
        status=status,
        errors=[],
        pages=pages,
    )


def test_to_result_item_bridges_pages_and_basename():
    page = ExtractedPageData(page_no=1, extracted_data={"k": "v"}, raw_text="t")
    item = to_result_item(
        _facade_result("/abs/doc1.pdf", ConversionStatus.SUCCESS, [page])
    )
    assert item.filename == "doc1.pdf"  # basename only, not the leaked abs path
    assert item.pages[0].extracted_data == {"k": "v"}


def test_in_body_result_envelope_counts_and_round_trips():
    results = [
        _facade_result(
            "doc1.pdf",
            ConversionStatus.SUCCESS,
            [ExtractedPageData(page_no=1, extracted_data={"a": 1})],
        ),
        _facade_result("doc2.png", ConversionStatus.FAILURE, []),
    ]
    task = Task(
        task_id="t1",
        task_type=TaskType.EXTRACT,
        sources=[],
        target=InBodyTarget(),
        callbacks=[CallbackSpec(url="https://example.com/callback")],
        extract_options=ExtractDocumentsOptions(template="x"),
    )
    identities = [
        SourceIdentity(
            i, f"https://example.com/{r.input.file.name}", hash_path_component(str(i))
        )
        for i, r in enumerate(results)
    ]
    callback_invoker = MagicMock()
    tr, processed = process_extraction_results(
        task, results, identities, callback_invoker=callback_invoker
    )
    assert isinstance(tr.result, ExtractionTaskResult)
    assert (tr.num_converted, tr.num_succeeded, tr.num_failed) == (2, 1, 1)
    assert len(processed) == 2
    progresses = [
        call.kwargs["progress"]
        for call in callback_invoker.invoke_callbacks_async.call_args_list
    ]
    assert [progress.kind for progress in progresses] == [
        ProgressKind.DOCUMENT_COMPLETED,
        ProgressKind.DOCUMENT_COMPLETED,
        ProgressKind.UPDATE_PROCESSED,
    ]
    assert progresses[-1].num_failed == 1

    # Survives the DoclingTaskResult discriminated union (the Redis hop).
    again = DoclingTaskResult.model_validate_json(tr.model_dump_json())
    assert again.result.kind == "ExtractionResult"
    assert len(again.result.documents) == 2

    task_again = Task.model_validate_json(task.model_dump_json())
    assert task_again.task_type == TaskType.EXTRACT
    assert task_again.extract_options == task.extract_options


def test_empty_extraction_is_an_error():
    task = Task(
        task_id="t1",
        task_type=TaskType.EXTRACT,
        sources=[],
        target=InBodyTarget(),
        extract_options=ExtractDocumentsOptions(template="x"),
    )
    with pytest.raises(RuntimeError, match="No documents"):
        process_extraction_results(task, [], [])


def test_storage_keys_do_not_collide_and_callbacks_report_every_document(monkeypatch):
    uploaded: list[str] = []

    class FakeFactory:
        @staticmethod
        def supports(target):
            del target
            return True

        @staticmethod
        def result_mode(target):
            del target
            return "artifacts"

    class FakeProcessor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            del args

        def upload_object(self, *, obj, target_filename, content_type):
            del obj, content_type
            uploaded.append(target_filename)
            if len(uploaded) == 2:
                raise RuntimeError("storage write failed")

    monkeypatch.setattr(
        extraction_results, "get_target_connector_factory", lambda _: FakeFactory()
    )
    monkeypatch.setattr(
        extraction_results,
        "get_target_processor",
        lambda *args, **kwargs: FakeProcessor(),
    )
    task = Task(
        task_id="t1",
        task_type=TaskType.EXTRACT,
        sources=[],
        target=S3Target(
            endpoint="s3.example.com",
            access_key="key",
            secret_key="secret",
            bucket="out",
        ),
        callbacks=[CallbackSpec(url="https://example.com/callback")],
        extract_options=ExtractDocumentsOptions(template="x"),
    )
    results = [
        _facade_result("same.pdf", ConversionStatus.SUCCESS, []),
        _facade_result("same.pdf", ConversionStatus.PARTIAL_SUCCESS, []),
    ]
    identities = [
        SourceIdentity(
            0,
            "s3://in/a/same.pdf",
            hash_path_component("s3://in/a/same.pdf"),
        ),
        SourceIdentity(
            1,
            "s3://in/b/same.pdf",
            hash_path_component("s3://in/b/same.pdf"),
        ),
    ]
    callback_invoker = MagicMock()

    task_result, processed = process_extraction_results(
        task, results, identities, callback_invoker=callback_invoker
    )

    assert uploaded == [
        f"default/t1/{identities[0].source_key}/same.json",
        f"default/t1/{identities[1].source_key}/same.json",
    ]
    assert [item.source for item in processed] == [
        "s3://in/a/same.pdf",
        "s3://in/b/same.pdf",
    ]
    progresses = [
        call.kwargs["progress"]
        for call in callback_invoker.invoke_callbacks_async.call_args_list
    ]
    assert [progress.kind for progress in progresses] == [
        ProgressKind.DOCUMENT_COMPLETED,
        ProgressKind.DOCUMENT_COMPLETED,
        ProgressKind.UPDATE_PROCESSED,
    ]
    assert task_result.num_succeeded == 1
    assert task_result.num_partially_succeeded == 0
    assert task_result.num_failed == 1


def test_presigned_result_keeps_source_identity_and_artifact_metadata(monkeypatch):
    identity = SourceIdentity(4, "s3://in/report.pdf", hash_path_component("report"))

    class FakeFactory:
        @staticmethod
        def supports(target):
            del target
            return True

        @staticmethod
        def result_mode(target):
            del target
            return "presigned"

    class FakeProcessor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            del args

        def upload_artifact_file(self, **kwargs):
            assert kwargs["source"] == identity

        def build_document_artifact_item(self, **kwargs):
            return DocumentArtifactItem(
                source_index=identity.source_index,
                source_uri=identity.source_uri,
                filename=kwargs["filename"],
                status=kwargs["status"],
                artifacts=[
                    ArtifactRef(
                        artifact_type="json",
                        mime_type="application/json",
                        uri="https://example.com/report.json",
                    )
                ],
            )

    monkeypatch.setattr(
        extraction_results, "get_target_connector_factory", lambda _: FakeFactory()
    )
    monkeypatch.setattr(
        extraction_results,
        "get_target_processor",
        lambda *args, **kwargs: FakeProcessor(),
    )
    task = Task(
        task_id="t1",
        task_type=TaskType.EXTRACT,
        sources=[],
        target=PresignedUrlTarget(),
        extract_options=ExtractDocumentsOptions(template="x"),
    )

    task_result, _ = process_extraction_results(
        task,
        [_facade_result("report.pdf", ConversionStatus.SUCCESS, [])],
        [identity],
    )

    assert isinstance(task_result.result, PresignedArtifactResult)
    document = task_result.result.documents[0]
    assert (document.source_index, document.source_uri) == (4, identity.source_uri)
    assert str(document.artifacts[0].uri) == "https://example.com/report.json"


def test_source_expansion_preserves_public_uri_and_global_index(monkeypatch):
    refs = [
        SourceDocumentRef(
            id="a", source_index=0, source_uri="s3://in/a/same.pdf", filename="same.pdf"
        ),
        SourceDocumentRef(
            id="b", source_index=1, source_uri="s3://in/b/same.pdf", filename="same.pdf"
        ),
    ]

    class FakeProcessor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            del args

        def converter_headers(self):
            return None

        def iterate_document_chunks(self, chunk_size):
            assert chunk_size == 128
            yield DocumentChunk(source={}, refs=refs, chunk_index=0)

        def fetch_converter_source_by_ref(self, ref, *, max_file_size):
            del max_file_size
            return DocumentStream(name=ref.filename, stream=BytesIO(ref.id.encode()))

    monkeypatch.setattr(
        source_expansion,
        "get_source_processor",
        lambda *args, **kwargs: FakeProcessor(),
    )
    task = Task(
        task_id="t1",
        task_type=TaskType.EXTRACT,
        sources=[
            S3SourceRequest(
                endpoint="s3.example.com",
                access_key="key",
                secret_key="secret",
                bucket="in",
            )
        ],
        extract_options=ExtractDocumentsOptions(template="x"),
    )

    sources, identities, headers = expand_task_sources_with_identities(task)

    assert len(sources) == 2
    assert headers is None
    assert [(i.source_index, i.source_uri) for i in identities] == [
        (0, "s3://in/a/same.pdf"),
        (1, "s3://in/b/same.pdf"),
    ]


@pytest.mark.asyncio
async def test_ray_extraction_emits_full_nonterminal_callback_sequence(
    monkeypatch, tmp_path
):
    from docling_jobkit.convert.manager import DoclingConverterManagerConfig
    from docling_jobkit.orchestrators.ray import serve_deployment
    from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
    from docling_jobkit.orchestrators.ray.models import ExtractPassthroughRequest

    recorded = []

    class FakeCallbackInvoker:
        def invoke_callbacks_async(self, **kwargs):
            recorded.append(kwargs["progress"])

    class FakeExtractionManager:
        def extract_documents(self, **kwargs):
            del kwargs
            return [_facade_result("doc.pdf", ConversionStatus.SUCCESS, [])]

    monkeypatch.setattr(
        serve_deployment.serve,
        "get_replica_context",
        lambda: SimpleNamespace(replica_id="converter-1"),
    )
    monkeypatch.setattr(serve_deployment, "DoclingConverterManager", MagicMock)
    monkeypatch.setattr(serve_deployment, "CallbackInvoker", FakeCallbackInvoker)
    source = DocumentStream(name="doc.pdf", stream=BytesIO(b"pdf"))
    identity = SourceIdentity(0, "s3://in/doc.pdf", hash_path_component("doc"))
    monkeypatch.setattr(
        serve_deployment,
        "expand_task_sources_with_identities",
        lambda *args, **kwargs: ([source], [identity], None),
    )
    converter_cls = serve_deployment.DoclingProcessorConverterDeployment.func_or_class
    converter = converter_cls(
        converter_manager_config=DoclingConverterManagerConfig(),
        config=RayOrchestratorConfig(
            redis_url="redis://localhost:6379/", scratch_dir=tmp_path
        ),
    )
    converter.ecm = FakeExtractionManager()
    task = Task(
        task_id="t1",
        task_type=TaskType.EXTRACT,
        sources=[source],
        target=InBodyTarget(),
        callbacks=[CallbackSpec(url="https://example.com/callback")],
        extract_options=ExtractDocumentsOptions(template="x"),
    )

    result = await converter.process_converter_request(
        ExtractPassthroughRequest(task=task)
    )

    assert "expected_doc_count" not in ExtractPassthroughRequest.model_fields
    assert result.task_result.num_succeeded == 1
    assert [progress.kind for progress in recorded] == [
        ProgressKind.SET_NUM_DOCS,
        ProgressKind.DOCUMENT_COMPLETED,
        ProgressKind.UPDATE_PROCESSED,
    ]
