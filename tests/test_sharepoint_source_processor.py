from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("office365")

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.errors import (
    SourceConnectorAuthenticationError,
    SourceConnectorPolicyError,
    SourceConnectorUnavailableError,
)
from docling_jobkit.connectors.sharepoint.source_processor import (
    SharePointFileIdentifier,
    SharePointSourceProcessor,
)
from docling_jobkit.convert.materialization import (
    SourceLimitExceededError,
)

_HELPER = "docling_jobkit.connectors.sharepoint.helper"


def _proc(coords) -> SharePointSourceProcessor:
    processor = SharePointSourceProcessor(coords)
    processor._client = MagicMock()
    processor._drive = MagicMock()
    return processor


def _meta(id: str, name: str, size: int = 10) -> dict:
    return {"id": id, "name": name, "size": size, "last_modified": None}


def test_initialize_reports_unresolvable_drive_as_source_policy_error(sp_source_coords):
    """The shared helper raises a neutral error; the source must not surface it as a
    *target* config error."""
    from docling_jobkit.connectors.sharepoint.helper import SharePointDriveNotFoundError

    processor = SharePointSourceProcessor(sp_source_coords)
    with (
        patch(f"{_HELPER}.get_client"),
        patch(
            f"{_HELPER}.resolve_drive",
            side_effect=SharePointDriveNotFoundError("library 'Missing' not found"),
        ),
    ):
        with pytest.raises(SourceConnectorPolicyError, match="Missing") as exc_info:
            processor._initialize()

    assert exc_info.value.source_kind == "sharepoint"
    assert exc_info.value.retryable is False


def test_list_document_ids_folder_mode_pushes_cap_into_the_walk(sp_source_coords):
    """The cap belongs in the listing call, not in a post-hoc truncation, so a capped
    run stops enumerating instead of pulling the whole library."""
    coords = sp_source_coords.model_copy(update={"max_num_elements": 2})
    processor = _proc(coords)

    with (
        patch(
            f"{_HELPER}.list_folder_items", return_value=iter([_meta("1", "a.pdf")])
        ) as list_folder,
        patch(f"{_HELPER}.list_items_by_id") as list_by_id,
    ):
        ids = list(processor._list_document_ids())

    assert [i.id for i in ids] == ["1"]
    assert all(isinstance(i, SharePointFileIdentifier) for i in ids)
    list_folder.assert_called_once_with(processor._drive, None, limit=2)
    list_by_id.assert_not_called()


def test_file_ids_mode_dispatches_to_items_by_id_and_still_caps(sp_source_coords):
    coords = sp_source_coords.model_copy(
        update={"file_ids": ["x", "y", "z"], "max_num_elements": 2}
    )
    processor = _proc(coords)
    metas = [_meta("x", "x.pdf"), _meta("y", "y.pdf"), _meta("z", "z.pdf")]

    with (
        patch(f"{_HELPER}.list_items_by_id", return_value=iter(metas)) as list_by_id,
        patch(f"{_HELPER}.list_folder_items") as list_folder,
    ):
        ids = list(processor._list_document_ids())

    assert [i.id for i in ids] == ["x", "y"]
    list_folder.assert_not_called()
    list_by_id.assert_called_once_with(
        processor._client, processor._drive, ["x", "y", "z"]
    )


def test_fetch_rejects_oversized_before_download(sp_source_coords):
    processor = _proc(sp_source_coords)
    identifier = SharePointFileIdentifier(id="1", name="big.pdf", size=9)

    with patch(f"{_HELPER}.download_item") as download:
        with pytest.raises(SourceLimitExceededError, match="max_file_size=8"):
            processor._fetch_document_by_id(identifier, max_file_size=8)

    download.assert_not_called()


def test_fetch_returns_stream_with_name(sp_source_coords):
    processor = _proc(sp_source_coords)
    identifier = SharePointFileIdentifier(id="1", name="a.pdf", size=10)

    with patch(f"{_HELPER}.download_item", return_value=BytesIO(b"PDF")) as download:
        doc = processor._fetch_document_by_id(identifier)

    assert isinstance(doc, DocumentStream)
    assert doc.name == "a.pdf"
    assert doc.stream.read() == b"PDF"
    download.assert_called_once_with(processor._client, processor._drive, "1")


def test_make_document_ref_builds_sharepoint_source_uri(sp_source_coords):
    processor = _proc(sp_source_coords)
    identifier = SharePointFileIdentifier(id="ITEM-1", name="a.pdf", size=10)

    ref = processor._make_document_ref(identifier, source_index=3)

    assert ref.source_index == 3
    assert ref.filename == "a.pdf"
    assert ref.source_uri == "sharepoint://ITEM-1"


@pytest.mark.parametrize(
    "status, expected",
    [
        (401, SourceConnectorAuthenticationError),
        (503, SourceConnectorUnavailableError),
    ],
    ids=["auth", "unavailable"],
)
def test_graph_error_maps_to_connector_error(
    sp_source_coords, graph_error, status, expected
):
    processor = _proc(sp_source_coords)

    with patch(f"{_HELPER}.list_folder_items", side_effect=graph_error(status)):
        with pytest.raises(expected):
            list(processor._list_document_ids())
