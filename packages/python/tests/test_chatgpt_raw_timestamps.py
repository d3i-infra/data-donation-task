"""Tests for chatgpt.conversations_to_df — raw ``create_time`` timestamps (ADR-0042).

The donated ``time`` cell is the export's own ``create_time`` value: a raw
number, ``""`` for a JSON null, or (in a malformed export) whatever string
was there — never dropped and never re-typed. The sort key that orders rows
by time must tolerate all three shapes without raising, since a raised
exception there is caught by the extractor's broad ``except`` and would
silently drop every message in the conversation, not just the bad one.
"""

import io
import json
import zipfile
from collections import Counter

from port.helpers.extraction_helpers import ZipArchiveReader
from port.platforms import chatgpt


def _archive(conversations: list) -> io.BytesIO:
    """Build an in-memory zip containing a single conversations.json member."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("conversations.json", json.dumps(conversations))
    buf.seek(0)
    return buf


def _node(node_id: str, parent: str | None, role: str, create_time, text: str) -> dict:
    return {
        "id": node_id,
        "parent": parent,
        "children": [],
        "message": {
            "id": f"msg-{node_id}",
            "author": {"role": role},
            "create_time": create_time,
            "content": {"content_type": "text", "parts": [text]},
        },
    }


def test_numeric_null_and_malformed_create_time_all_survive_in_order():
    conversation = {
        "title": "mixed timestamps",
        "current_node": "n3",
        "mapping": {
            "n1": _node("n1", None, "user", 1700000000, "first"),
            "n2": _node("n2", "n1", "assistant", None, "second"),
            "n3": _node("n3", "n2", "assistant", "not-a-number", "third"),
        },
    }
    errors = Counter()
    reader = ZipArchiveReader(_archive([conversation]), ["conversations.json"], errors)
    df = chatgpt.conversations_to_df(reader, errors)

    assert not errors
    assert len(df) == 3

    times = dict(zip(df["message id"], df["time"]))
    assert times["n1"] == 1700000000
    assert times["n2"] == ""
    assert times["n3"] == "not-a-number"
    assert isinstance(times["n1"], int)  # not a float64 upcast (final review, Minor 11)

    # The one real timestamp sorts first; empty/malformed values sort after it
    # (order between the two ties is whatever `sorted` leaves them in, both
    # having the same (1, 0.0) key — but neither disappears).
    assert df["time"].iloc[0] == 1700000000
    assert isinstance(list(df["time"])[0], int)
    assert set(df["time"].iloc[1:]) == {"", "not-a-number"}


def test_time_sort_key_never_raises():
    for value in (1700000000, 1700000000.5, None, "", "garbage", "n/a", object()):
        key = chatgpt._time_sort_key(value)
        assert isinstance(key, tuple) and len(key) == 2
