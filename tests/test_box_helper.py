import base64
import hashlib
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

pytest.importorskip("box_sdk_gen")

from box_sdk_gen import BoxSDKError, FileFull, FolderMini, RequestException, WebLink

from docling_jobkit.connectors.box import helper
from docling_jobkit.connectors.errors import SourceConnectorPolicyError


class _FakeItems:
    def __init__(self, entries, next_marker=None):
        self.entries = entries
        self.next_marker = next_marker


def _client_with_layout(layout: dict[str, list], *, page_size: int = 100) -> MagicMock:
    """A client whose folders serve *layout*, paged the way Box pages it.

    Marker-based: each response carries a ``next_marker`` for the following page and
    ``None`` on the last one, so a helper that stops on a short page instead of on
    the absent marker will visibly under-read.
    """
    client = MagicMock()

    def _get_folder_items(folder_id, *, fields, usemarker, marker, limit):
        del fields, limit
        assert usemarker is True, "offset paging is unreliable for large folders"
        entries = layout.get(folder_id, [])
        start = int(marker) if marker else 0
        window = entries[start : start + page_size]
        nxt = start + page_size
        return _FakeItems(window, next_marker=str(nxt) if nxt < len(entries) else None)

    client.folders.get_folder_items.side_effect = _get_folder_items
    return client


def test_list_folder_items_descends_into_an_immediate_subfolder():
    layout = {
        "0": [FileFull(id="1", name="a.pdf", size=10), FolderMini(id="sub")],
        "sub": [FileFull(id="2", name="b.pdf", size=20)],
    }
    client = _client_with_layout(layout)

    metas = list(helper.list_folder_items(client, "0"))

    assert sorted(m["id"] for m in metas) == ["1", "2"]


def test_list_folder_items_recurses_arbitrarily_deep():
    layout = {
        "0": [FolderMini(id="a")],
        "a": [FolderMini(id="b")],
        "b": [FolderMini(id="c")],
        "c": [FileFull(id="deep-file", name="deep.pdf", size=5)],
    }
    client = _client_with_layout(layout)

    metas = list(helper.list_folder_items(client, "0"))

    assert [m["id"] for m in metas] == ["deep-file"]


def test_list_folder_items_skips_web_links():
    layout = {
        "0": [
            WebLink(id="link", name="not-a-file"),
            FileFull(id="1", name="a.pdf", size=10),
        ],
    }
    client = _client_with_layout(layout)

    metas = list(helper.list_folder_items(client, "0"))

    assert [m["id"] for m in metas] == ["1"]


def test_list_folder_items_stops_walking_at_limit():
    layout = {
        "0": [
            FileFull(id="1", name="1.pdf", size=1),
            FileFull(id="2", name="2.pdf", size=1),
            FolderMini(id="never-queried"),
        ],
    }
    client = _client_with_layout(layout)

    metas = list(helper.list_folder_items(client, "0", limit=1))

    assert [m["id"] for m in metas] == ["1"]
    queried = [c.args[0] for c in client.folders.get_folder_items.call_args_list]
    assert queried == ["0"]


def test_list_folder_items_follows_the_marker_across_pages():
    """A full page must not end the listing: only an absent next_marker does.

    Box returns exactly `limit` entries per full page, so inferring the end from a
    short page silently truncates every folder that is an exact multiple of the
    page size — and offset paging is documented as unreliable at high offsets.
    """
    files = [FileFull(id=str(n), name=f"{n}.pdf", size=1) for n in range(6)]
    client = _client_with_layout({"0": files}, page_size=2)

    metas = list(helper.list_folder_items(client, "0"))

    assert [m["id"] for m in metas] == ["0", "1", "2", "3", "4", "5"]
    markers = [
        c.kwargs["marker"] for c in client.folders.get_folder_items.call_args_list
    ]
    assert markers == [None, "2", "4"]


def test_list_folder_items_stops_paging_at_limit_mid_folder():
    files = [FileFull(id=str(n), name=f"{n}.pdf", size=1) for n in range(6)]
    client = _client_with_layout({"0": files}, page_size=2)

    metas = list(helper.list_folder_items(client, "0", limit=3))

    assert [m["id"] for m in metas] == ["0", "1", "2"]
    # Third page never requested: the page generator is abandoned at the cap.
    assert client.folders.get_folder_items.call_count == 2


def test_list_folder_items_rejects_a_file_without_a_size():
    """`size` is Optional in the SDK schema; without it max_file_size cannot be
    enforced before the download, so the item must fail as a classified connector
    error rather than a raw ValidationError out of BoxFileIdentifier."""
    client = _client_with_layout({"0": [FileFull(id="1", name="a.pdf")]})

    with pytest.raises(SourceConnectorPolicyError, match="without a 'size'"):
        list(helper.list_folder_items(client, "0"))


def test_transport_failure_is_classified_retryable():
    """BoxNetworkClient catches requests' exceptions around the request call and
    re-raises a bare BoxSDKError, so matching only RequestException misses the
    commonest transient failure and reports it as permanent."""
    wrapped = BoxSDKError(message="boom", error=RequestException("connection refused"))

    assert helper.is_box_unavailable_error(wrapped) is True
    assert helper.is_box_policy_error(wrapped) is False
    assert helper.is_box_authentication_error(wrapped) is False


def test_non_transport_sdk_errors_stay_non_retryable():
    assert helper.is_box_unavailable_error(
        BoxSDKError(message="no location header")
    ) is (False)


@pytest.mark.parametrize(
    "status, predicate",
    [
        (401, "is_box_authentication_error"),
        (403, "is_box_authentication_error"),
        (404, "is_box_policy_error"),
        (400, "is_box_policy_error"),
        (429, "is_box_unavailable_error"),
        (503, "is_box_unavailable_error"),
    ],
)
def test_status_codes_land_in_exactly_one_family(box_api_error, status, predicate):
    """The three predicates must partition the statuses: an exception matching none
    of them escapes unclassified and is reported as a non-retryable internal error."""
    exc = box_api_error(status)
    matched = {
        name
        for name in (
            "is_box_authentication_error",
            "is_box_policy_error",
            "is_box_unavailable_error",
        )
        if getattr(helper, name)(exc)
    }
    assert matched == {predicate}


@pytest.mark.parametrize(
    "error_code, description",
    [
        ("invalid_grant", "Grant credentials are invalid"),
        ("invalid_grant", "App is not yet authorized for use"),
        ("invalid_client", "The client credentials are invalid"),
    ],
)
def test_oauth_token_failures_are_auth_not_policy(error_code, description):
    """POST /oauth2/token reports credential problems as HTTP 400, not 401.

    Both strings are real Box responses. Classifying them by status alone lands
    them in _POLICY_STATUS, so an operator with a wrong subject id or an
    unauthorized app is told to check 'folder_id' instead.
    """
    from box_sdk_gen import BoxAPIError
    from box_sdk_gen.box.errors import RequestInfo, ResponseInfo

    exc = BoxAPIError(
        request_info=RequestInfo(
            method="POST",
            url="https://api.box.com/oauth2/token",
            query_params={},
            headers={},
        ),
        response_info=ResponseInfo(
            status_code=400,
            headers={},
            body={"error": error_code, "error_description": description},
        ),
        message="400",
    )

    assert helper.is_box_authentication_error(exc) is True
    assert helper.is_box_policy_error(exc) is False


def test_plain_bad_request_stays_policy(box_api_error):
    """A 400 without an OAuth error code is still a bad request, not a credential
    failure — the two must not collapse into one another."""
    exc = box_api_error(400)

    assert helper.is_box_policy_error(exc) is True
    assert helper.is_box_authentication_error(exc) is False


# Large-file chunked upload


def _sha1_digest(data: bytes) -> str:
    return f"sha={base64.b64encode(hashlib.sha1(data).digest()).decode()}"


def _chunked_client(part_size: int, *, committed_id: str = "committed-id") -> MagicMock:
    """A client whose ``chunked_uploads`` methods behave like Box's real ones
    just enough to drive ``_upload_via_chunked_session`` end to end.

    Both session-creation calls return the same session shape (an id and a
    part_size), each part upload echoes back a `.part` descriptor carrying the
    bytes it received, and commit returns the finished file as `.entries[0]`.
    """
    client = MagicMock()
    session = SimpleNamespace(id="sess-1", part_size=part_size)
    client.chunked_uploads.create_file_upload_session.return_value = session
    client.chunked_uploads.create_file_upload_session_for_existing_file.return_value = (
        session
    )

    def _upload_part(session_id, body, *, digest, content_range):
        return SimpleNamespace(
            part=SimpleNamespace(
                data=body.read(), digest=digest, content_range=content_range
            )
        )

    client.chunked_uploads.upload_file_part.side_effect = _upload_part
    client.chunked_uploads.create_file_upload_session_commit.return_value = (
        SimpleNamespace(
            entries=[FileFull(id=committed_id, name="whatever.pdf", size=1)]
        )
    )
    return client


def test_chunked_session_splits_streams_hashes_and_commits_parts_in_order():
    client = _chunked_client(part_size=4, committed_id="new-file-id")
    data = b"0123456789"  # 10 bytes over part_size=4 -> chunks of 4, 4, 2

    result = helper._upload_via_chunked_session(
        client,
        lambda: client.chunked_uploads.create_file_upload_session(
            "0", len(data), "f.pdf"
        ),
        BytesIO(data),
        len(data),
    )

    calls = client.chunked_uploads.upload_file_part.call_args_list
    assert all(hasattr(c.args[1], "read") for c in calls)
    assert [c.kwargs["content_range"] for c in calls] == [
        "bytes 0-3/10",
        "bytes 4-7/10",
        "bytes 8-9/10",
    ]
    assert [c.kwargs["digest"] for c in calls] == [
        _sha1_digest(b"0123"),
        _sha1_digest(b"4567"),
        _sha1_digest(b"89"),
    ]

    args, kwargs = client.chunked_uploads.create_file_upload_session_commit.call_args
    session_id, parts = args
    assert session_id == "sess-1"
    assert [p.data for p in parts] == [b"0123", b"4567", b"89"]
    assert kwargs["digest"] == _sha1_digest(data)
    assert result.id == "new-file-id"


def test_new_vs_overwrite_large_file_use_the_correct_session_call():
    client = _chunked_client(part_size=100)
    data = b"x" * 10

    helper._upload_new_large_file(
        client, "folder-1", "big.pdf", BytesIO(data), len(data)
    )
    client.chunked_uploads.create_file_upload_session.assert_called_once_with(
        "folder-1", len(data), "big.pdf"
    )
    client.chunked_uploads.create_file_upload_session_for_existing_file.assert_not_called()

    helper._overwrite_large_file(client, "file-99", "big.pdf", BytesIO(data), len(data))
    client.chunked_uploads.create_file_upload_session_for_existing_file.assert_called_once_with(
        "file-99", len(data), file_name="big.pdf"
    )


@pytest.mark.parametrize(
    ("size", "expects_chunked"),
    [
        (10, True),  # == threshold: `size < _CHUNKED_THRESHOLD`, so already "large"
        (9, False),  # just under: still the simple path
    ],
)
def test_upload_document_routes_on_the_threshold_boundary(
    monkeypatch, size, expects_chunked
):
    monkeypatch.setattr(helper, "_CHUNKED_THRESHOLD", 10)
    client = _chunked_client(part_size=4)
    data = b"x" * size

    helper.upload_document(client, "folder-1", "f.pdf", BytesIO(data), size)

    assert client.chunked_uploads.create_file_upload_session.called is expects_chunked
    assert client.uploads.upload_file.called is not expects_chunked


def test_upload_document_large_file_conflict_falls_back_to_a_version_session(
    monkeypatch, box_api_error
):
    """A 409 on the initial large-file session creation must resolve the
    existing file and retry as create_file_upload_session_for_existing_file,
    re-reading the whole stream from the start."""
    monkeypatch.setattr(helper, "_CHUNKED_THRESHOLD", 10)
    client = _chunked_client(part_size=4)
    existing = FileFull(id="existing-id", name="big.pdf", size=10)
    client.folders.get_folder_items.return_value = _FakeItems([existing])
    client.chunked_uploads.create_file_upload_session.side_effect = box_api_error(409)
    data = b"0123456789"

    helper.upload_document(client, "folder-1", "big.pdf", BytesIO(data), len(data))

    client.chunked_uploads.create_file_upload_session_for_existing_file.assert_called_once_with(
        "existing-id", len(data), file_name="big.pdf"
    )
    # The stream must have been rewound and re-read in full on retry, not
    # left partially consumed from the failed first attempt.
    args, _ = client.chunked_uploads.create_file_upload_session_commit.call_args
    _, parts = args
    assert b"".join(p.data for p in parts) == data
