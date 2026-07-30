"""Regression test: slice finalization must resolve chunking presets.

Large (sliced) documents are finalized on the coordinator via
``_finalize_slice_results``. That path used to forward the *raw*
``convert_options.chunking_options`` — which is ``None`` whenever the caller
only supplies a chunking preset (or relies on a chunk-requiring target to
activate chunking). A ``None`` value silently disables chunk export, so the
target index / ``chunks.jsonl`` came out empty for any document over
``max_page_slice_size`` pages.

``_resolve_chunking_options`` resolves the preset to concrete options on the
coordinator, mirroring the non-sliced converter path.
"""

from unittest.mock import MagicMock

import pytest

pytest.importorskip("ray")

from docling.datamodel.service.options import ConvertDocumentsOptions

from docling_jobkit.convert.manager import DoclingConverterManagerConfig
from docling_jobkit.orchestrators.ray.config import RayOrchestratorConfig
from docling_jobkit.orchestrators.ray.serve_deployment import (
    DoclingProcessorCoordinatorDeployment,
)


def _make_coordinator(
    monkeypatch: pytest.MonkeyPatch,
) -> DoclingProcessorCoordinatorDeployment:
    monkeypatch.setattr(
        "docling_jobkit.orchestrators.ray.serve_deployment.serve.get_replica_context",
        lambda: type("ReplicaContext", (), {"replica_id": "coordinator-1"})(),
    )
    deployment_cls = getattr(
        DoclingProcessorCoordinatorDeployment, "func_or_class", None
    )
    assert deployment_cls is not None
    return deployment_cls(
        converter_manager_config=DoclingConverterManagerConfig(),
        config=RayOrchestratorConfig(),
        redis_url="redis://localhost:6379/0",
        converter_handle=MagicMock(),
    )


def test_resolve_chunking_options_fills_default_preset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator = _make_coordinator(monkeypatch)

    # The caller supplied no explicit chunking_options: the raw value the buggy
    # code forwarded is None, which disabled chunk export on the slice path.
    convert_options = ConvertDocumentsOptions()
    assert convert_options.chunking_options is None

    resolved = coordinator._resolve_chunking_options(convert_options)

    assert resolved is not None


def test_resolve_chunking_options_preserves_explicit_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator = _make_coordinator(monkeypatch)
    cm = coordinator._get_converter_manager()

    # An explicitly-provided preset must resolve to the same concrete options the
    # converter deployment would use for a non-sliced document.
    convert_options = ConvertDocumentsOptions()
    expected = cm.parse_chunking_options(ConvertDocumentsOptions())

    resolved = coordinator._resolve_chunking_options(convert_options)

    assert type(resolved) is type(expected)


def test_get_converter_manager_is_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator = _make_coordinator(monkeypatch)
    first = coordinator._get_converter_manager()
    second = coordinator._get_converter_manager()
    assert first is second
