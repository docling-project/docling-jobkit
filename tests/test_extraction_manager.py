"""Extraction model-gating boundary + results-builder routing/counts.

Offline: exercises the operator-gating resolution (the access-control boundary)
and the result-envelope assembly without running any VLM inference.
"""

from io import BytesIO
from pathlib import PurePath
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from docling.datamodel.base_models import (
    ConversionStatus,
    DoclingComponentType,
    DocumentStream,
    ErrorItem,
    FailureCategory,
)
from docling.datamodel.extraction import (
    DocumentScope,
    ExtractionItem,
    ExtractionTarget,
    ExtractionTemplate,
    PageScope,
    VlmInferenceMetadata,
)
from docling.datamodel.extraction_options import ChannelSelection, ExtractionVlmOptions
from docling.datamodel.service.callbacks import CallbackSpec, ProgressKind
from docling.datamodel.service.options import ExtractDocumentsOptions
from docling.datamodel.service.requests import S3SourceRequest
from docling.datamodel.service.responses import (
    ArtifactRef,
    DoclingTaskResult,
    DocumentArtifactItem,
    ExtractionDocumentResult,
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


def _item_error(message: str) -> ErrorItem:
    return ErrorItem(
        component_type=DoclingComponentType.MODEL,
        module_name="ExtractionVlmPipeline",
        error_message=message,
        category=FailureCategory.INFERENCE_FAILURE,
    )


def _target(field="amount"):
    return ExtractionTarget(
        output_schema={"type": "object", "properties": {field: {"type": "number"}}},
        template=ExtractionTemplate(format="nuextract", value={field: "number"}),
        instructions=f"Extract {field} only.",
    )


# --- model-selection gating (the security boundary) ------------------------


def test_default_preset_resolves_to_operator_model():
    ecm = DocumentExtractionManager(
        DocumentExtractionManagerConfig(default_extraction_preset="granite_vision_4_1")
    )
    vlm = ecm.resolve_extraction_model(ExtractDocumentsOptions())
    expected = ExtractionVlmOptions.from_preset("granite_vision_4_1")
    assert vlm.model_spec.name == expected.model_spec.name


@pytest.mark.parametrize(
    "preset", ["nuextract_2b", "granite_vision_4_1", "nuextract_3", "lift"]
)
def test_startup_resolves_stable_default_without_task_target(preset):
    ecm = DocumentExtractionManager(
        DocumentExtractionManagerConfig(default_extraction_preset=preset)
    )
    resolved = ecm.resolve_extraction_model()
    assert resolved.model_spec == ExtractionVlmOptions.from_preset(preset).model_spec
    assert resolved.output_mode == "prompt_only"
    assert ecm._get_extractor.cache_info().currsize == 0


def test_startup_applies_default_preset_and_engine_allow_lists():
    for config in (
        DocumentExtractionManagerConfig(allowed_extraction_presets=[]),
        DocumentExtractionManagerConfig(allowed_extraction_engines=["api"]),
    ):
        with pytest.raises(ValueError, match="not allowed"):
            DocumentExtractionManager(config).resolve_extraction_model()


def test_operator_defined_preset_can_be_default_and_override_builtin():
    custom = ExtractionVlmOptions.from_preset("nuextract_2b").model_copy(
        update={"scale": 1.25}
    )
    ecm = DocumentExtractionManager(
        DocumentExtractionManagerConfig(
            default_extraction_preset="server_model",
            allowed_extraction_presets=["server_model", "nuextract_2b"],
            custom_extraction_presets={
                "server_model": custom,
                "nuextract_2b": custom,
            },
        )
    )

    assert ecm.resolve_extraction_model().scale == 1.25
    assert (
        ecm.resolve_extraction_model(
            ExtractDocumentsOptions(extraction_preset="nuextract_2b")
        ).scale
        == 1.25
    )


def test_preset_rejected_when_not_in_allow_list():
    ecm = DocumentExtractionManager(
        DocumentExtractionManagerConfig(
            allowed_extraction_presets=["granite_vision_4_1"]
        )
    )
    with pytest.raises(ValueError, match="not allowed"):
        ecm.resolve_extraction_model(
            ExtractDocumentsOptions(extraction_preset="nuextract_2b")
        )


def test_custom_config_rejected_unless_operator_opts_in():
    ecm = DocumentExtractionManager(DocumentExtractionManagerConfig())
    with pytest.raises(ValueError, match="not allowed"):
        ecm.resolve_extraction_model(
            ExtractDocumentsOptions(extraction_custom_config={"model_spec": {}})
        )


def test_unknown_preset_reports_available_presets():
    ecm = DocumentExtractionManager(DocumentExtractionManagerConfig())
    with pytest.raises(ValueError, match="not found"):
        ecm.resolve_extraction_model(
            ExtractDocumentsOptions(extraction_preset="does_not_exist")
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
            ExtractDocumentsOptions(extraction_custom_config=custom)
        )


def test_cached_extractor_isolates_targets_and_stable_configuration(monkeypatch):
    initialized = []
    calls = []

    class FakeExtractor:
        def __init__(self, **kwargs):
            initialized.append(
                next(
                    iter(kwargs["extraction_format_options"].values())
                ).pipeline_options
            )

        def extract_all(self, sources, **kwargs):
            calls.append(kwargs)
            return iter(())

    monkeypatch.setattr(extraction_manager, "DocumentExtractor", FakeExtractor)
    ecm = DocumentExtractionManager(
        DocumentExtractionManagerConfig(
            enable_remote_services=True,
            allow_custom_extraction_config=True,
            max_num_pages=9,
            max_file_size=1000,
        )
    )
    custom = ExtractionVlmOptions.from_preset("lift").model_copy(
        update={
            "engine_options": ApiVlmEngineOptions(engine_type=VlmEngineType.API),
            "output_mode": "schema_constrained",
        }
    )
    options = ExtractDocumentsOptions(
        extraction_custom_config=custom,
        input_channels=ChannelSelection.TEXT,
        page_range=(5, 8),
    )
    target1 = _target().model_copy(
        update={
            "template": ExtractionTemplate(format="example_json", value={"amount": 10})
        }
    )
    target2 = _target("tax").model_copy(
        update={"template": ExtractionTemplate(format="example_json", value={"tax": 2})}
    )
    headers = {"Authorization": "Bearer test"}
    list(
        ecm.extract_documents(
            [], extraction_target=target1, options=options, headers=headers
        )
    )
    list(
        ecm.extract_documents(
            [], extraction_target=target2, options=options, headers=headers
        )
    )
    assert len(initialized) == 1
    assert initialized[0].vlm_options.output_mode == "prompt_only"
    assert initialized[0].input_channels == ChannelSelection.TEXT
    assert custom.output_mode == "schema_constrained"
    assert [call["target"] for call in calls] == [target1, target2]
    assert calls[0] == {
        "target": target1,
        "page_range": (5, 8),
        "max_num_pages": 9,
        "max_file_size": 1000,
        "headers": headers,
        "raises_on_error": False,
    }
    for changed in (
        {"output_mode": "schema_constrained"},
        {"input_channels": ChannelSelection.IMAGE},
    ):
        list(
            ecm.extract_documents(
                [],
                extraction_target=target1,
                options=options.model_copy(update=changed),
            )
        )
    assert len(initialized) == 3
    assert initialized[1].vlm_options.output_mode == "schema_constrained"


def test_constrained_mode_rejects_local_engine():
    ecm = DocumentExtractionManager(DocumentExtractionManagerConfig())
    with pytest.raises(ValueError, match="vLLM API"):
        ecm.resolve_extraction_model(
            ExtractDocumentsOptions(output_mode="schema_constrained")
        )


# --- results bridging + envelope -------------------------------------------


def _facade_result(filename: str, status: ConversionStatus, items):
    return SimpleNamespace(
        input=SimpleNamespace(file=PurePath(filename), format=None),
        status=status,
        errors=[],
        items=items,
    )


def test_to_result_item_bridges_items_identity_and_basename():
    page = ExtractionItem(
        scope=PageScope(page_no=5), extracted_data={"k": "v"}, raw_text="t"
    )
    item = to_result_item(
        _facade_result("/abs/doc1.pdf", ConversionStatus.SUCCESS, [page]),
        SourceIdentity(3, "s3://in/doc1.pdf", "doc1"),
    )
    assert item.filename == "doc1.pdf"  # basename only, not the leaked abs path
    assert item.items[0].extracted_data == {"k": "v"}
    assert item.items[0].scope == PageScope(page_no=5)
    assert (item.source_index, item.source_uri) == (3, "s3://in/doc1.pdf")
    assert "input" not in item.model_dump()


def test_in_body_result_envelope_counts_and_round_trips():
    results = [
        _facade_result(
            "doc1.pdf",
            ConversionStatus.SUCCESS,
            [
                ExtractionItem(
                    scope=PageScope(page_no=5),
                    extracted_data={"a": 1},
                    raw_text='{"a": 1}',
                    validation_status="passed",
                    inference_metadata=VlmInferenceMetadata(usage={"total_tokens": 12}),
                )
            ],
        ),
        _facade_result(
            "doc2.md",
            ConversionStatus.PARTIAL_SUCCESS,
            [
                ExtractionItem(
                    scope=DocumentScope(),
                    extracted_data={"a": "bad"},
                    raw_text="bad",
                    errors=[_item_error("schema failed")],
                    validation_status="failed",
                ),
                ExtractionItem(
                    scope=DocumentScope(),
                    errors=[_item_error("timeout")],
                    validation_status="not_run",
                ),
            ],
        ),
        _facade_result("doc3.png", ConversionStatus.FAILURE, []),
    ]
    task = Task(
        task_id="t1",
        task_type=TaskType.EXTRACT,
        sources=[],
        target=InBodyTarget(),
        callbacks=[CallbackSpec(url="https://example.com/callback")],
        extract_target=_target(),
        extract_options=ExtractDocumentsOptions(),
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
    assert (tr.num_converted, tr.num_succeeded, tr.num_failed) == (3, 1, 1)
    assert len(processed) == 3
    progresses = [
        call.kwargs["progress"]
        for call in callback_invoker.invoke_callbacks_async.call_args_list
    ]
    assert [progress.kind for progress in progresses] == [
        ProgressKind.DOCUMENT_COMPLETED,
        ProgressKind.DOCUMENT_COMPLETED,
        ProgressKind.DOCUMENT_COMPLETED,
        ProgressKind.UPDATE_PROCESSED,
    ]
    assert progresses[-1].num_failed == 1

    # Survives the DoclingTaskResult discriminated union (the Redis hop).
    again = DoclingTaskResult.model_validate_json(tr.model_dump_json())
    assert again.result.kind == "ExtractionResult"
    assert len(again.result.documents) == 3
    assert again.result.documents == tr.result.documents
    assert tr.num_partially_succeeded == 1

    task_again = Task.model_validate_json(task.model_dump_json())
    assert task_again.task_type == TaskType.EXTRACT
    assert task_again.extract_options == task.extract_options
    assert task_again.extract_target == task.extract_target


def test_empty_extraction_is_an_error():
    task = Task(
        task_id="t1",
        task_type=TaskType.EXTRACT,
        sources=[],
        target=InBodyTarget(),
        extract_target=_target(),
        extract_options=ExtractDocumentsOptions(),
    )
    with pytest.raises(RuntimeError, match="No documents"):
        process_extraction_results(task, [], [])


def test_storage_keys_do_not_collide_and_callbacks_report_every_document(monkeypatch):
    uploaded: list[str] = []
    payloads = []
    events = []
    processor_kwargs = []

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
            del content_type
            events.append("upload")
            payloads.append(ExtractionDocumentResult.model_validate_json(obj))
            uploaded.append(target_filename)
            if len(uploaded) == 2:
                raise RuntimeError("storage write failed")

    monkeypatch.setattr(
        extraction_results, "get_target_connector_factory", lambda _: FakeFactory()
    )

    def get_processor(*args, **kwargs):
        del args
        processor_kwargs.append(kwargs)
        return FakeProcessor()

    monkeypatch.setattr(extraction_results, "get_target_processor", get_processor)
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
        extract_target=_target(),
        extract_options=ExtractDocumentsOptions(),
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
    callback_invoker.invoke_callbacks_async.side_effect = lambda **kwargs: (
        events.append(kwargs["progress"].kind)
    )

    task_result, processed = process_extraction_results(
        task, results, identities, callback_invoker=callback_invoker
    )

    assert processor_kwargs == [{"allow_external_plugins": False}]
    assert uploaded == [
        f"default/t1/{identities[0].source_key}/same.extraction.json",
        f"default/t1/{identities[1].source_key}/same.extraction.json",
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
    assert events == [
        "upload",
        ProgressKind.DOCUMENT_COMPLETED,
        "upload",
        ProgressKind.DOCUMENT_COMPLETED,
        ProgressKind.UPDATE_PROCESSED,
    ]
    assert payloads[0] == to_result_item(results[0], identities[0])
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
        extract_target=_target(),
        extract_options=ExtractDocumentsOptions(),
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


def test_source_expansion_preserves_public_uri_and_original_index(monkeypatch):
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
            ),
            DocumentStream(name="last.md", stream=BytesIO(b"text")),
        ],
        extract_target=_target(),
        extract_options=ExtractDocumentsOptions(),
    )

    sources, identities, headers = expand_task_sources_with_identities(task)

    assert len(sources) == 3
    assert headers is None
    assert [(i.source_index, i.source_uri) for i in identities] == [
        (0, "s3://in/a/same.pdf"),
        (0, "s3://in/b/same.pdf"),
        (1, "last.md"),
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
        extract_target=_target(),
        extract_options=ExtractDocumentsOptions(),
    )

    result = await converter.process_converter_request(
        ExtractPassthroughRequest(task=task), tenant_id="default"
    )

    assert "expected_doc_count" not in ExtractPassthroughRequest.model_fields
    assert result.task_result.num_succeeded == 1
    assert [progress.kind for progress in recorded] == [
        ProgressKind.SET_NUM_DOCS,
        ProgressKind.DOCUMENT_COMPLETED,
        ProgressKind.UPDATE_PROCESSED,
    ]
