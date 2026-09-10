from io import BytesIO
from unittest.mock import patch

import pytest

from docling_jobkit.connectors.databricks_volumes.models import (
    DatabricksVolumesCoordinates,
)
from docling_jobkit.connectors.databricks_volumes.source_processor import (
    DatabricksVolumeFileIdentifier,
    DatabricksVolumesSourceProcessor,
)
from docling_jobkit.convert.materialization import SourceLimitExceededError


@pytest.fixture
def coords() -> DatabricksVolumesCoordinates:
    return DatabricksVolumesCoordinates(
        workspace_host="dbc-xxxxxxx.cloud.databricks.com",
        token="tok",
        volume_path="/Volumes/main/default/docs",
    )


def _entry(path: str, name: str, *, is_directory: bool = False, size: int = 0) -> dict:
    entry = {"path": path, "name": name, "is_directory": is_directory}
    if not is_directory:
        entry["file_size"] = size
    return entry


def test_list_document_ids_recurses_into_subdirectories(coords):
    processor = DatabricksVolumesSourceProcessor(coords)

    root_entries = [
        _entry("/Volumes/main/default/docs/a.pdf", "a.pdf", size=10),
        _entry("/Volumes/main/default/docs/sub", "sub", is_directory=True),
    ]
    sub_entries = [
        _entry("/Volumes/main/default/docs/sub/b.pdf", "b.pdf", size=20),
    ]

    def fake_iter_directory(host, token, path):
        if path == "/Volumes/main/default/docs":
            return iter(root_entries)
        if path == "/Volumes/main/default/docs/sub":
            return iter(sub_entries)
        raise AssertionError(f"unexpected path {path}")

    with patch(
        "docling_jobkit.connectors.databricks_volumes.source_processor.iter_directory",
        side_effect=fake_iter_directory,
    ):
        ids = list(processor._list_document_ids())

    assert {i.name for i in ids} == {"a.pdf", "b.pdf"}


def test_list_document_ids_respects_max_num_elements(coords):
    capped_coords = coords.model_copy(update={"max_num_elements": 1})
    processor = DatabricksVolumesSourceProcessor(capped_coords)

    entries = [
        _entry("/Volumes/main/default/docs/a.pdf", "a.pdf", size=10),
        _entry("/Volumes/main/default/docs/b.pdf", "b.pdf", size=20),
    ]

    with patch(
        "docling_jobkit.connectors.databricks_volumes.source_processor.iter_directory",
        return_value=iter(entries),
    ):
        ids = list(processor._list_document_ids())

    assert len(ids) == 1


def test_count_documents_clips_to_max_num_elements(coords):
    capped_coords = coords.model_copy(update={"max_num_elements": 1})
    processor = DatabricksVolumesSourceProcessor(capped_coords)

    entries = [
        _entry("/Volumes/main/default/docs/a.pdf", "a.pdf", size=10),
        _entry("/Volumes/main/default/docs/b.pdf", "b.pdf", size=20),
    ]

    with patch(
        "docling_jobkit.connectors.databricks_volumes.source_processor.iter_directory",
        return_value=iter(entries),
    ):
        assert processor._count_documents() == 1


def test_make_document_ref_uses_databricks_volumes_uri(coords):
    processor = DatabricksVolumesSourceProcessor(coords)

    identifier = DatabricksVolumeFileIdentifier(
        path="/Volumes/main/default/docs/a.pdf", name="a.pdf", size=10
    )

    ref = processor._make_document_ref(identifier, source_index=0)

    assert (
        ref.source_uri == "databricks_volumes://dbc-xxxxxxx.cloud.databricks.com"
        "/Volumes/main/default/docs/a.pdf"
    )
    assert ref.filename == "a.pdf"


def test_fetch_document_by_id_rejects_oversized_before_download(coords):
    processor = DatabricksVolumesSourceProcessor(coords)

    identifier = DatabricksVolumeFileIdentifier(
        path="/Volumes/main/default/docs/large.pdf", name="large.pdf", size=10000
    )

    with pytest.raises(SourceLimitExceededError, match="max_file_size=8000"):
        processor._fetch_document_by_id(identifier, max_file_size=8000)


def test_fetch_document_by_id_passes_source_kind_and_builds_files_api_url(coords):
    processor = DatabricksVolumesSourceProcessor(coords)

    identifier = DatabricksVolumeFileIdentifier(
        path="/Volumes/main/default/docs/a.pdf", name="a.pdf", size=10
    )

    with patch(
        "docling_jobkit.connectors.databricks_volumes.source_processor.download_document_from_url",
        return_value=BytesIO(b"content"),
    ) as mock_download:
        stream = processor._fetch_document_by_id(identifier)

    assert stream.name == "a.pdf"
    mock_download.assert_called_once_with(
        "https://dbc-xxxxxxx.cloud.databricks.com"
        "/api/2.0/fs/files/Volumes/main/default/docs/a.pdf",
        "tok",
        expected_host="dbc-xxxxxxx.cloud.databricks.com",
        max_file_size=None,
        source_kind="databricks_volumes",
    )
