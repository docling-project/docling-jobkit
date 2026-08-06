import os
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("office365")

from office365.runtime.client_request_exception import (
    ClientRequestException,
)
from requests import ConnectionError as RequestsConnectionError, Timeout

from docling_jobkit.connectors.sharepoint import helper


def _drive_item(id, name, size=10, is_folder=False, children=()):
    """Stand-in for an office365 DriveItem (only the fields the helper reads)."""
    item = MagicMock()
    item.id = id
    item.name = name
    item.is_folder = is_folder
    item.last_modified_datetime = None
    item.get_property.side_effect = lambda key, default=None: (
        size if key == "size" else default
    )
    item.children.paged.return_value.__iter__.side_effect = lambda: iter(children)
    return item


# --- connection ---------------------------------------------------------------


def test_get_client_builds_confidential_client(sp_connection):
    with patch("office365.graph_client.GraphClient") as graph_client:
        client = helper.get_client(sp_connection)

    graph_client.assert_called_once_with(tenant="tenant-guid")
    # the secret must be unwrapped, not handed over as a SecretStr
    graph_client.return_value.with_client_secret.assert_called_once_with(
        "client-id", "secret"
    )
    assert client is graph_client.return_value.with_client_secret.return_value


def test_resolve_drive_uses_default_library(sp_connection):
    client = MagicMock()
    site = (
        client.sites.get_by_url.return_value.get.return_value.execute_query.return_value
    )

    drive = helper.resolve_drive(client, sp_connection)

    assert drive is site.drive
    client.sites.get_by_url.assert_called_once_with(sp_connection.site_url)


def test_resolve_drive_selects_named_library(sp_connection):
    coords = sp_connection.model_copy(update={"document_library": "Contracts"})
    client = MagicMock()
    site = (
        client.sites.get_by_url.return_value.get.return_value.execute_query.return_value
    )
    contracts = SimpleNamespace(name="Contracts")
    site.drives.get_all.return_value.execute_query.return_value = [
        SimpleNamespace(name="Documents"),
        contracts,
    ]

    assert helper.resolve_drive(client, coords) is contracts


def test_resolve_drive_named_library_not_found_raises_neutral_error(sp_connection):
    """Neutral on purpose: source and target translate it into their own family."""
    coords = sp_connection.model_copy(update={"document_library": "Missing"})
    client = MagicMock()
    site = (
        client.sites.get_by_url.return_value.get.return_value.execute_query.return_value
    )
    site.drives.get_all.return_value.execute_query.return_value = [
        SimpleNamespace(name="Documents")
    ]

    with pytest.raises(helper.SharePointDriveNotFoundError, match="Missing"):
        helper.resolve_drive(client, coords)


def test_resolve_drive_onedrive_uses_user_drive(od_connection):
    client = MagicMock()

    drive = helper.resolve_drive(client, od_connection)

    assert drive is client.users.get_by_principal_name.return_value.drive
    client.users.get_by_principal_name.assert_called_once_with("alice@contoso.com")


# --- listing ------------------------------------------------------------------


@pytest.mark.parametrize(
    "folder_path", [None, "/Reports/2026"], ids=["root", "subpath"]
)
def test_list_folder_items_yields_file_meta(folder_path):
    files = [_drive_item("1", "a.pdf", 10), _drive_item("2", "b.pdf", 20)]
    root = _drive_item("root", "root", children=files)
    drive = MagicMock()
    drive.root = root
    root.get_by_path.return_value = _drive_item("sub", "sub", children=files)

    metas = list(helper.list_folder_items(drive, folder_path))

    assert [m["id"] for m in metas] == ["1", "2"]
    assert metas[1]["size"] == 20
    if folder_path:
        root.get_by_path.assert_called_once_with(folder_path)
    else:
        root.get_by_path.assert_not_called()


def test_list_folder_items_descends_into_subfolders():
    subfolder = _drive_item(
        "sub", "sub", is_folder=True, children=[_drive_item("2", "b.pdf")]
    )
    drive = MagicMock()
    drive.root = _drive_item(
        "root", "root", children=[_drive_item("1", "a.pdf"), subfolder]
    )

    metas = list(helper.list_folder_items(drive, None))

    assert sorted(m["id"] for m in metas) == ["1", "2"]


def test_list_folder_items_stops_walking_at_limit():
    """The cap must short-circuit the walk, not truncate a fully-drained listing."""
    produced: list[str] = []

    def _lazy_children():
        for i in range(10):
            produced.append(str(i))
            yield _drive_item(str(i), f"{i}.pdf")

    root = MagicMock()
    root.children.paged.return_value.__iter__.side_effect = _lazy_children
    drive = MagicMock()
    drive.root = root

    metas = list(helper.list_folder_items(drive, None, limit=3))

    assert [m["id"] for m in metas] == ["0", "1", "2"]
    assert produced == ["0", "1", "2"]  # the remaining 7 were never pulled
    root.children.paged.assert_called_once_with(helper._DEFAULT_PAGE_SIZE)


def test_list_items_by_id_skips_folders():
    client = MagicMock()
    drive = MagicMock()
    drive.items.__getitem__.side_effect = lambda key: {
        "1": _drive_item("1", "a.pdf"),
        "2": _drive_item("2", "subfolder", is_folder=True),
    }[key]

    metas = list(helper.list_items_by_id(client, drive, ["1", "2"]))

    assert [m["id"] for m in metas] == ["1"]


# --- error predicates ---------------------------------------------------------


@pytest.mark.parametrize(
    "status, is_auth, is_unavailable",
    [(401, True, False), (403, True, False), (404, False, False), (503, False, True)],
    ids=["401", "403", "404", "503"],
)
def test_error_predicates_classify_graph_status(
    graph_error, status, is_auth, is_unavailable
):
    exc = graph_error(status)
    assert helper.is_sharepoint_authentication_error(exc) is is_auth
    assert helper.is_sharepoint_unavailable_error(exc) is is_unavailable


@pytest.mark.parametrize(
    "exc, is_unavailable",
    [(RequestsConnectionError(), True), (Timeout(), True), (ValueError(), False)],
    ids=["connection_error", "timeout", "unrelated"],
)
def test_error_predicates_on_non_graph_exceptions(exc, is_unavailable):
    assert helper.is_sharepoint_authentication_error(exc) is False
    assert helper.is_sharepoint_unavailable_error(exc) is is_unavailable


# --- retry --------------------------------------------------------------------


def test_on_retry_failure_reraises_only_on_final_attempt(graph_error):
    exc = graph_error(503)
    # non-final attempt only logs, so the retry loop keeps going
    helper._on_retry_failure(1, exc)
    # ...and the last one re-raises: execute_query_retry swallows on exhaustion, which
    # would otherwise report a persistent failure as a successful upload
    with pytest.raises(ClientRequestException):
        helper._on_retry_failure(helper._UPLOAD_MAX_RETRY, exc)


def test_on_retry_failure_fails_fast_on_terminal_status(graph_error):
    """A 401 cannot become a 200; retrying it just burns delay on every artifact."""
    with pytest.raises(ClientRequestException):
        helper._on_retry_failure(1, graph_error(401))


# --- destination resolution ---------------------------------------------------


@pytest.mark.parametrize(
    "base_folder, target_filename, expected",
    [
        (None, "doc.json", (None, "doc.json")),
        ("out", "doc.json", ("out", "doc.json")),
        ("out", "json/doc.json", ("out/json", "doc.json")),
        ("/a/", "p/q/f.png", ("a/p/q", "f.png")),
        (None, "pages/1/img.png", ("pages/1", "img.png")),
    ],
)
def test_resolve_destination(base_folder, target_filename, expected):
    assert helper.resolve_destination(base_folder, target_filename) == expected


def test_get_or_create_folder_returns_root_for_empty_path():
    drive = MagicMock()
    assert helper.get_or_create_folder(drive, None) is drive.root
    drive.root.get_by_path.assert_not_called()


def test_get_or_create_folder_returns_existing():
    drive = MagicMock()
    existing = MagicMock(name="existing")
    drive.root.get_by_path.return_value.get.return_value.execute_query.return_value = (
        existing
    )

    result = helper.get_or_create_folder(drive, "out")

    assert result is existing
    drive.root.get_by_path.assert_called_once_with("out")
    drive.root.create_folder.assert_not_called()


def test_get_or_create_folder_looks_every_segment_up_from_the_root():
    """Nested segments must be addressed as a full path from the drive root.

    Chaining ``get_by_path`` onto an already-resolved item builds
    ``/items/{id}:/json:/``, which Graph rejects with a 400 — but only once the parent
    exists, so the first run silently "works" (the malformed request 404s and the
    folder gets created) and every later run fails.
    """
    drive = MagicMock()
    resolved = drive.root.get_by_path.return_value.get.return_value.execute_query

    helper.get_or_create_folder(drive, "out/json")

    assert [c.args[0] for c in drive.root.get_by_path.call_args_list] == [
        "out",
        "out/json",
    ]
    # never chained off the item resolved for the previous segment
    resolved.return_value.get_by_path.assert_not_called()


def test_get_or_create_folder_creates_under_the_resolved_parent(graph_error):
    """Creation, unlike lookup, must target the parent's id-addressed children."""
    drive = MagicMock()
    lookup = drive.root.get_by_path.return_value
    parent = MagicMock(name="parent")
    created = MagicMock(name="created")
    lookup.get.return_value.execute_query.side_effect = [parent, graph_error(404)]
    parent.create_folder.return_value = created

    result = helper.get_or_create_folder(drive, "out/json")

    parent.create_folder.assert_called_once()
    assert parent.create_folder.call_args.args[0] == "json"
    drive.root.create_folder.assert_not_called()
    assert result is created


def test_get_or_create_folder_creates_missing_and_returns_created(graph_error):
    drive = MagicMock()
    child = drive.root.get_by_path.return_value
    child.get.return_value.execute_query.side_effect = graph_error(404)
    created = MagicMock(name="created")
    drive.root.create_folder.return_value = created

    result = helper.get_or_create_folder(drive, "out")

    drive.root.create_folder.assert_called_once()
    created.execute_query_retry.assert_called_once()
    # the created folder — not its parent — must be returned
    assert result is created


def test_get_or_create_folder_adopts_folder_created_concurrently(graph_error):
    """Every parallel worker races to create the same tree; the loser must not fail."""
    drive = MagicMock()
    child = drive.root.get_by_path.return_value
    adopted = MagicMock(name="adopted")
    child.get.return_value.execute_query.side_effect = [graph_error(404), adopted]
    created = MagicMock(name="created")
    created.execute_query_retry.side_effect = graph_error(409)
    drive.root.create_folder.return_value = created

    assert helper.get_or_create_folder(drive, "out") is adopted


def test_get_or_create_folder_reraises_non_404(graph_error):
    drive = MagicMock()
    drive.root.get_by_path.return_value.get.return_value.execute_query.side_effect = (
        graph_error(403)
    )
    with pytest.raises(ClientRequestException):
        helper.get_or_create_folder(drive, "out")


# --- uploads ------------------------------------------------------------------


def test_upload_file_simple_uploads_bytes(tmp_path):
    folder = MagicMock()
    src = tmp_path / "src.json"
    src.write_bytes(b"hello")

    helper.upload_file(folder, src, "doc.json")

    folder.upload.assert_called_once_with("doc.json", b"hello")
    folder.upload.return_value.execute_query_retry.assert_called_once()
    folder.resumable_upload.assert_not_called()


def test_upload_file_large_same_name_streams_from_source(tmp_path, monkeypatch):
    monkeypatch.setattr(helper, "_SIMPLE_UPLOAD_MAX_BYTES", 2)
    folder = MagicMock()
    src = tmp_path / "doc.json"
    src.write_bytes(b"1234567")

    helper.upload_file(folder, src, "doc.json")

    folder.upload.assert_not_called()
    (path_arg,), _ = folder.resumable_upload.call_args
    assert path_arg == os.fspath(src)  # uploaded in place, nothing staged


def test_upload_file_large_differing_name_stages_under_target_leaf(
    tmp_path, monkeypatch
):
    """resumable_upload names the item after the file it reads, hence the staged copy."""
    monkeypatch.setattr(helper, "_SIMPLE_UPLOAD_MAX_BYTES", 2)
    folder = MagicMock()
    src = tmp_path / "tmp_random.json"  # basename != target leaf
    src.write_bytes(b"1234567")
    captured = {}

    def _capture(path, chunk_size):
        captured["basename"] = os.path.basename(path)
        captured["content"] = open(path, "rb").read()
        return MagicMock()

    folder.resumable_upload.side_effect = _capture

    helper.upload_file(folder, src, "doc.json")

    assert captured["basename"] == "doc.json"
    assert captured["content"] == b"1234567"


@pytest.mark.parametrize(
    "obj, expected",
    [("hi", b"hi"), (b"hi", b"hi"), (bytearray(b"hi"), b"hi"), (BytesIO(b"hi"), b"hi")],
    ids=["str", "bytes", "bytearray", "file_like"],
)
def test_upload_object_normalizes_to_bytes(obj, expected):
    folder = MagicMock()

    helper.upload_object(folder, obj, "doc.txt")

    folder.upload.assert_called_once_with("doc.txt", expected)


@pytest.mark.parametrize(
    "obj", [b"1234567", BytesIO(b"1234567")], ids=["bytes", "file_like"]
)
def test_upload_object_large_stages_to_disk(obj, monkeypatch):
    monkeypatch.setattr(helper, "_SIMPLE_UPLOAD_MAX_BYTES", 2)
    monkeypatch.setattr(helper, "_RESUMABLE_CHUNK_BYTES", 2)
    folder = MagicMock()
    captured = {}

    def _capture(path, chunk_size):
        captured["basename"] = os.path.basename(path)
        captured["content"] = open(path, "rb").read()
        return MagicMock()

    folder.resumable_upload.side_effect = _capture

    helper.upload_object(folder, obj, "big.json")

    folder.upload.assert_not_called()
    assert captured["basename"] == "big.json"
    assert captured["content"] == b"1234567"


def test_upload_object_reads_large_stream_in_bounded_steps(monkeypatch):
    """A file-like over the cap must never be read into memory in one go."""
    monkeypatch.setattr(helper, "_SIMPLE_UPLOAD_MAX_BYTES", 4)
    monkeypatch.setattr(helper, "_RESUMABLE_CHUNK_BYTES", 4)
    folder = MagicMock()
    reads: list[int | None] = []

    class _RecordingStream(BytesIO):
        def read(self, size=-1):
            reads.append(size)
            return super().read(size)

    helper.upload_object(folder, _RecordingStream(b"x" * 20), "big.bin")

    assert all(size is not None and size > 0 for size in reads)
    assert max(reads) <= helper._SIMPLE_UPLOAD_MAX_BYTES + 1
