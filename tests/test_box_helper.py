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
