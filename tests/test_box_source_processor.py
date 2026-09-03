from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("box_sdk_gen")

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.box.source_processor import (
    BoxFileIdentifier,
    BoxSourceProcessor,
)
from docling_jobkit.connectors.errors import (
    SourceConnectorAuthenticationError,
    SourceConnectorUnavailableError,
)
from docling_jobkit.convert.materialization import SourceLimitExceededError

_HELPER = "docling_jobkit.connectors.box.helper"


def _proc(config) -> BoxSourceProcessor:
    processor = BoxSourceProcessor(config)
    processor._client = MagicMock()
    return processor


def _meta(id: str, name: str, size: int = 10) -> dict:
    return {"id": id, "name": name, "size": size, "modified_at": None}


def test_ccg_and_jwt_auth_modes(box_ccg_source_config, box_jwt_source_config):
    assert box_ccg_source_config.auth_mode == "ccg"
    assert box_jwt_source_config.auth_mode == "jwt"


def test_partial_jwt_fields_are_rejected():
    from pydantic import SecretStr, ValidationError

    from docling_jobkit.connectors.box.models import BoxSource

    with pytest.raises(ValidationError, match="must all be provided together"):
        BoxSource(
            client_id="c",
            client_secret=SecretStr("s"),
            enterprise_id="e",
            jwt_key_id="only-this-one",
        )


def test_enterprise_and_user_id_are_mutually_exclusive():
    from pydantic import SecretStr, ValidationError

    from docling_jobkit.connectors.box.models import BoxSource

    with pytest.raises(ValidationError, match="Exactly one"):
        BoxSource(
            client_id="c",
            client_secret=SecretStr("s"),
            enterprise_id="e",
            user_id="u1",
        )


def test_list_document_ids_folder_mode_pushes_cap_into_the_walk(box_ccg_source_config):
    """The cap belongs in the listing call, not in a post-hoc truncation, so a capped
    run stops enumerating instead of pulling the whole tree."""
    config = box_ccg_source_config.model_copy(update={"max_num_elements": 2})
    processor = _proc(config)

    with (
        patch(
            f"{_HELPER}.list_folder_items", return_value=iter([_meta("1", "a.pdf")])
        ) as list_folder,
        patch(f"{_HELPER}.fetch_file_by_id") as fetch_by_id,
    ):
        ids = list(processor._list_document_ids())

    assert [i.id for i in ids] == ["1"]
    assert all(isinstance(i, BoxFileIdentifier) for i in ids)
    list_folder.assert_called_once_with(processor._client, "0", limit=2)
    fetch_by_id.assert_not_called()


def test_file_ids_mode_dispatches_to_fetch_by_id_and_still_caps(box_ccg_source_config):
    config = box_ccg_source_config.model_copy(
        update={"file_ids": ["x", "y", "z"], "max_num_elements": 2}
    )
    processor = _proc(config)
    metas = {
        "x": _meta("x", "x.pdf"),
        "y": _meta("y", "y.pdf"),
        "z": _meta("z", "z.pdf"),
    }

    with (
        patch(f"{_HELPER}.fetch_file_by_id", side_effect=lambda _c, fid: metas[fid]),
        patch(f"{_HELPER}.list_folder_items") as list_folder,
    ):
        ids = list(processor._list_document_ids())

    assert [i.id for i in ids] == ["x", "y"]
    list_folder.assert_not_called()


def test_fetch_rejects_oversized_before_download(box_ccg_source_config):
    processor = _proc(box_ccg_source_config)
    identifier = BoxFileIdentifier(id="1", name="big.pdf", size=9)

    with patch(f"{_HELPER}.download_file") as download:
        with pytest.raises(SourceLimitExceededError, match="max_file_size=8"):
            processor._fetch_document_by_id(identifier, max_file_size=8)

    download.assert_not_called()


def test_fetch_returns_stream_with_name(box_ccg_source_config):
    processor = _proc(box_ccg_source_config)
    identifier = BoxFileIdentifier(id="1", name="a.pdf", size=10)

    with patch(f"{_HELPER}.download_file", return_value=BytesIO(b"PDF")) as download:
        doc = processor._fetch_document_by_id(identifier)

    assert isinstance(doc, DocumentStream)
    assert doc.name == "a.pdf"
    assert doc.stream.read() == b"PDF"
    download.assert_called_once_with(processor._client, "1")


def test_make_document_ref_builds_box_source_uri(box_ccg_source_config):
    processor = _proc(box_ccg_source_config)
    identifier = BoxFileIdentifier(
        id="FILE-1",
        name="a.pdf",
        size=10,
        modified_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    ref = processor._make_document_ref(identifier, source_index=3)

    assert ref.source_index == 3
    assert ref.filename == "a.pdf"
    assert ref.source_uri == "box://FILE-1"


@pytest.mark.parametrize(
    "status, expected",
    [
        (401, SourceConnectorAuthenticationError),
        (503, SourceConnectorUnavailableError),
    ],
    ids=["auth", "unavailable"],
)
def test_box_api_error_maps_to_connector_error(
    box_ccg_source_config, box_api_error, status, expected
):
    processor = _proc(box_ccg_source_config)

    with patch(f"{_HELPER}.list_folder_items", side_effect=box_api_error(status)):
        with pytest.raises(expected):
            list(processor._list_document_ids())
