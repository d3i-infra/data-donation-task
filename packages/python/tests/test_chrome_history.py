"""Tests for chrome.browser_history_to_df — raw microsecond timestamps (ADR-0042).

The donated ``Date`` cell is the export's own ``time_usec`` value: a raw integer,
never converted to an ISO string. Sorting (newest first, capped at 10 000 rows) is
presentation over that raw value, not a rewrite of it.
"""

import io
import json
import zipfile
from collections import Counter

from port.helpers.extraction_helpers import ZipArchiveReader
from port.platforms import chrome


def _archive(data: dict) -> io.BytesIO:
    """Build an in-memory zip containing a single History.json member."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("History.json", json.dumps(data))
    buf.seek(0)
    return buf


def test_chrome_keeps_raw_microseconds_and_the_newest_rows():
    usec = [1781548241000000 + i * 1_000_000 for i in range(10_005)]
    archive = _archive({"Browser History": [{"title": f"t{i}", "url": "https://x", "time_usec": u} for i, u in enumerate(usec)]})
    errors = Counter()
    df = chrome.browser_history_to_df(ZipArchiveReader(archive, ["History.json"], errors), errors)
    assert len(df) == 10_000
    assert df["Date"].iloc[0] == usec[-1]            # newest first, numeric order
    assert df["Date"].iloc[-1] == usec[5]            # the five oldest were cut
    assert all(isinstance(v, int) for v in df["Date"])   # the raw numbers, not strings
    assert not errors


def test_chrome_sorts_numerically_not_lexically():
    """Final review, Minor 10: the existing cap test uses monotonic, equal-length
    ints, so a plain (non-numeric) ``sort_values("Date")`` would coincidentally
    pass too. Mix in a value whose *string* order disagrees with its *numeric*
    order — a 3-digit ``999`` among 16-digit microsecond values sorts last
    lexically ('9' > '1') but must still sort just above the empty/missing
    values numerically — plus a row with no ``time_usec`` key at all, which
    makes the raw column ``object``-dtype (int and str mixed) rather than a
    clean ``int64`` column that would sort correctly by accident.
    """
    large = [1781548241000000 + i * 1_000_000 for i in range(5)]
    items = [{"title": f"t{i}", "url": "https://x", "time_usec": u} for i, u in enumerate(large)]
    items.append({"title": "small", "url": "https://x", "time_usec": 999})
    items.append({"title": "missing", "url": "https://x"})  # no time_usec key -> ""
    archive = _archive({"Browser History": items})
    errors = Counter()
    df = chrome.browser_history_to_df(ZipArchiveReader(archive, ["History.json"], errors), errors)

    dates = list(df["Date"])
    assert dates[0] == large[-1]     # newest (numerically largest) first
    assert dates[-2] == 999          # tiny value sorts just above the unplaceable one
    assert dates[-1] == ""           # missing/unparseable sorts last
    assert not errors


def test_chrome_missing_time_usec_is_empty():
    archive = _archive({"Browser History": [{"title": "t", "url": "https://x"}]})
    errors = Counter()
    df = chrome.browser_history_to_df(ZipArchiveReader(archive, ["History.json"], errors), errors)
    assert list(df["Date"]) == [""]
    assert not errors


def test_chrome_explicit_null_time_usec_is_empty_not_none():
    """An explicit JSON ``null`` must normalise to ``""``, never bare ``None``
    (final review, Minor 1): mixing ``None`` with real ints in the same column
    upcasts it to float64, and the null then donates as the text ``"null"``.
    """
    archive = _archive({"Browser History": [
        {"title": "t0", "url": "https://x", "time_usec": None},
        {"title": "t1", "url": "https://x", "time_usec": 1781548241000000},
    ]})
    errors = Counter()
    df = chrome.browser_history_to_df(ZipArchiveReader(archive, ["History.json"], errors), errors)
    assert list(df["Date"]) == [1781548241000000, ""]
    assert isinstance(df["Date"].iloc[0], int)
    assert not errors
