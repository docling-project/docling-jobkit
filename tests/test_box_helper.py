from unittest.mock import MagicMock

import pytest

pytest.importorskip("box_sdk_gen")

from box_sdk_gen import FileFull, FolderMini, WebLink

from docling_jobkit.connectors.box import helper


class _FakeItems:
    def __init__(self, entries):
        self.entries = entries


def _client_with_layout(layout: dict[str, list]) -> MagicMock:
    client = MagicMock()

    def _get_folder_items(folder_id, *, fields, offset, limit):
        del fields, limit
        return _FakeItems(layout.get(folder_id, []) if offset == 0 else [])

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
