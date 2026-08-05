from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest
from office365.runtime.client_request_exception import ClientRequestException
from pydantic import SecretStr, ValidationError
from requests import Response

from docling.datamodel.base_models import DocumentStream

from docling_jobkit.connectors.errors import (
    SourceConnectorAuthenticationError,
    SourceConnectorUnavailableError,
)
from docling_jobkit.connectors.sharepoint.source_processor import (
    SharePointFileIdentifier,
    SharePointSourceProcessor,
)
from docling_jobkit.convert.materialization import SourceLimitExceededError
from docling_jobkit.datamodel.sharepoint_coords import SharePointSourceCoordinates

_HELPER = "docling_jobkit.connectors.sharepoint.helper"


@pytest.fixture
def sp_coords() -> SharePointSourceCoordinates:
    """SharePoint target (site_url)."""
    return SharePointSourceCoordinates(
        tenant="tenant-guid",
        client_id="client-id",
        client_secret=SecretStr("secret"),
        site_url="https://contoso.sharepoint.com/sites/Marketing",
    )


def _proc(coords: SharePointSourceCoordinates) -> SharePointSourceProcessor:
    processor = SharePointSourceProcessor(coords)
    processor._client = MagicMock()
    processor._drive = MagicMock()
    return processor


def _meta(id: str, name: str, size: int = 10) -> dict:
    return {"id": id, "name": name, "size": size, "last_modified": None}


def _graph_error(status: int) -> ClientRequestException:
    """Build a ClientRequestException without its response-inspecting __init__."""
    exc = ClientRequestException.__new__(ClientRequestException)
    response = Response()
    response.status_code = status
    exc.response = response
    return exc


@pytest.mark.parametrize(
    "kwargs",
    [
        {"site_url": "u"},  # SharePoint
        {"onedrive_user": "a@x.com"},  # OneDrive
        {"site_url": "u", "document_library": "Contracts"},
    ],
    ids=["sharepoint_site", "onedrive_user", "site_with_library"],
)
def test_coords_accepts_valid_targets(kwargs):
    coords = SharePointSourceCoordinates(
        tenant="t", client_id="c", client_secret=SecretStr("s"), **kwargs
    )
    assert coords.site_url or coords.onedrive_user


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({}, "Exactly one"),  # neither
        ({"site_url": "u", "onedrive_user": "a@x.com"}, "Exactly one"),  # both
        ({"onedrive_user": "a@x.com", "document_library": "D"}, "document_library"),
    ],
    ids=["no_target", "both_targets", "library_with_onedrive"],
)
def test_coords_rejects_invalid_targets(kwargs, match):
    with pytest.raises(ValidationError, match=match):
        SharePointSourceCoordinates(
            tenant="t", client_id="c", client_secret=SecretStr("s"), **kwargs
        )


def test_list_document_ids_folder_mode(sp_coords):
    processor = _proc(sp_coords)
    metas = [_meta("1", "a.pdf"), _meta("2", "b.pdf")]

    with (
        patch(f"{_HELPER}.list_folder_items", return_value=iter(metas)) as list_folder,
        patch(f"{_HELPER}.list_items_by_id") as list_by_id,
    ):
        ids = list(processor._list_document_ids())

    assert [i.id for i in ids] == ["1", "2"]
    assert all(isinstance(i, SharePointFileIdentifier) for i in ids)
    list_folder.assert_called_once_with(processor._client, processor._drive, None)
    list_by_id.assert_not_called()


def test_list_document_ids_respects_max_num_elements(sp_coords):
    coords = sp_coords.model_copy(update={"max_num_elements": 2})
    processor = _proc(coords)
    metas = [_meta(str(i), f"{i}.pdf") for i in range(5)]

    with patch(f"{_HELPER}.list_folder_items", return_value=iter(metas)):
        ids = list(processor._list_document_ids())

    assert [i.id for i in ids] == ["0", "1"]


def test_file_ids_mode_dispatches_to_items_by_id(sp_coords):
    coords = sp_coords.model_copy(update={"file_ids": ["x", "y"]})
    processor = _proc(coords)

    with (
        patch(
            f"{_HELPER}.list_items_by_id", return_value=iter([_meta("x", "x.pdf")])
        ) as list_by_id,
        patch(f"{_HELPER}.list_folder_items") as list_folder,
    ):
        ids = list(processor._list_document_ids())

    assert [i.id for i in ids] == ["x"]
    list_folder.assert_not_called()
    list_by_id.assert_called_once_with(processor._client, processor._drive, ["x", "y"])


def test_iterate_documents_respects_max_num_elements(sp_coords):
    coords = sp_coords.model_copy(update={"max_num_elements": 2})
    processor = _proc(coords)
    processor._initialized = True
    metas = [_meta(str(i), f"{i}.pdf") for i in range(3)]

    with patch(f"{_HELPER}.list_folder_items", return_value=iter(metas)):
        processor._fetch_document_by_id = MagicMock(
            side_effect=lambda identifier, **_: DocumentStream(
                name=identifier.name, stream=BytesIO(b"x")
            )
        )
        docs = list(processor.iterate_documents())

    assert [d.name for d in docs] == ["0.pdf", "1.pdf"]
    assert processor._fetch_document_by_id.call_count == 2


def test_fetch_rejects_oversized_before_download(sp_coords):
    processor = _proc(sp_coords)
    identifier = SharePointFileIdentifier(id="1", name="big.pdf", size=9)

    with pytest.raises(SourceLimitExceededError, match="max_file_size=8"):
        processor._fetch_document_by_id(identifier, max_file_size=8)


def test_fetch_returns_stream_with_name(sp_coords):
    processor = _proc(sp_coords)
    identifier = SharePointFileIdentifier(id="1", name="a.pdf", size=10)

    with patch(f"{_HELPER}.download_item", return_value=BytesIO(b"PDF")) as download:
        doc = processor._fetch_document_by_id(identifier)

    assert isinstance(doc, DocumentStream)
    assert doc.name == "a.pdf"
    assert doc.stream.read() == b"PDF"
    download.assert_called_once_with(processor._client, processor._drive, "1")


def test_make_document_ref_builds_sharepoint_source_uri(sp_coords):
    processor = _proc(sp_coords)
    identifier = SharePointFileIdentifier(id="ITEM-1", name="a.pdf", size=10)

    ref = processor._make_document_ref(identifier, source_index=3)

    assert ref.source_index == 3
    assert ref.filename == "a.pdf"
    assert ref.source_uri == "sharepoint://ITEM-1"


@pytest.mark.parametrize(
    "status, expected",
    [
        (401, SourceConnectorAuthenticationError),
        (403, SourceConnectorAuthenticationError),
        (503, SourceConnectorUnavailableError),
    ],
    ids=["401_auth", "403_auth", "503_unavailable"],
)
def test_graph_error_maps_to_connector_error(sp_coords, status, expected):
    processor = _proc(sp_coords)

    with patch(f"{_HELPER}.list_folder_items", side_effect=_graph_error(status)):
        with pytest.raises(expected):
            list(processor._list_document_ids())
