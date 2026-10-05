"""Tests for YouTube's raw timestamps and ``date_locale`` stamp (ADR-0042/0043).

The json export's ``time`` field is already the export's own ISO string and is
never converted; the html export's ``time`` field is the display text Takeout
rendered, and must be kept as-is rather than parsed into ISO 8601 — donate the
export's own value, never our reading of it (ADR-0042). ``extraction()``
stamps ``date_locale`` on every table it actually returns, from the validated
DDP language (ADR-0043).
"""
import io
import zipfile
from collections import Counter

from port.helpers.extraction_helpers import ZipArchiveReader
from port.helpers.validate import Language, validate_zip
from port.platforms import youtube

import pytest
from test_date_columns_declared import _load_config


@pytest.fixture(autouse=True)
def _config_from_docstrings(monkeypatch):
    """CI has no generated ``port/configs/*_config.json`` (gitignored); build the
    config in memory from the docstrings, as the generator does."""
    monkeypatch.setattr(youtube, "load_port_config", lambda registry, platform: _load_config(youtube, platform))


def _archive(members: dict) -> io.BytesIO:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)
    buf.seek(0)
    return buf


WATCH_JSON = (
    '[{"title": "Watched A video", '
    '"titleUrl": "https://www.youtube.com/watch?v=abc", '
    '"time": "2026-06-15T20:30:41Z"}]'
)

WATCH_HTML = (
    '<div><a href="https://www.youtube.com/watch?v=abc">A video</a><br>'
    '15 jun 2026, 20:30:41 CEST<br></div>'
)

SEARCH_HTML = (
    '<div><a href="https://www.youtube.com/results?search_query=cats">cats</a><br>'
    '15 jun 2026, 20:30:41 CEST<br></div>'
)

WATCH_HTML_ONE_DIGIT_HOUR = (
    '<div><a href="https://www.youtube.com/watch?v=abc">A video</a><br>'
    '<a href="https://www.youtube.com/channel/UCx">A channel</a><br>'
    'Aug 26, 2026, 3:50:40 PM BST<br></div>'
)

WATCH_HTML_NO_TIMESTAMP = (
    '<div><a href="https://www.youtube.com/watch?v=abc">A video</a></div>'
)

SEARCH_HTML_NO_TIMESTAMP = (
    '<div><a href="https://www.youtube.com/results?search_query=cats">cats</a></div>'
)


def test_watch_history_json_keeps_the_export_s_own_string():
    archive = _archive({"watch-history.json": WATCH_JSON})
    validation = validate_zip(youtube.DDP_CATEGORIES, archive)
    reader = ZipArchiveReader(archive, validation.archive_members, Counter())

    df = youtube.watch_history_to_df(reader, Counter(), validation)

    assert df.iloc[0]["Timestamp"] == "2026-06-15T20:30:41Z"


def test_watch_history_html_keeps_the_rendered_text_unconverted():
    archive = _archive({"watch-history.html": WATCH_HTML})
    validation = validate_zip(youtube.DDP_CATEGORIES, archive)
    reader = ZipArchiveReader(archive, validation.archive_members, Counter())

    df = youtube.watch_history_to_df(reader, Counter(), validation)

    assert df.iloc[0]["Timestamp"] == "15 jun 2026, 20:30:41 CEST"


def test_search_history_html_keeps_the_rendered_text_unconverted():
    archive = _archive({"search-history.html": SEARCH_HTML})
    validation = validate_zip(youtube.DDP_CATEGORIES, archive)
    reader = ZipArchiveReader(archive, validation.archive_members, Counter())

    df = youtube.search_history_to_df(reader, Counter(), validation)

    assert df.iloc[0]["Timestamp"] == "15 jun 2026, 20:30:41 CEST"


def test_watch_history_html_with_no_timestamp_is_empty_not_none():
    """A watch-history html row with no matching timestamp must donate ``""``,
    never bare ``None`` (final review, Minor 2): an explicit ``None`` cell
    shows blank in the table but donates the text ``"null"``.
    """
    archive = _archive({"watch-history.html": WATCH_HTML_NO_TIMESTAMP})
    validation = validate_zip(youtube.DDP_CATEGORIES, archive)
    reader = ZipArchiveReader(archive, validation.archive_members, Counter())

    df = youtube.watch_history_to_df(reader, Counter(), validation)

    assert df.iloc[0]["Timestamp"] == ""
    assert df.iloc[0]["Timestamp"] is not None


def test_search_history_html_with_no_timestamp_is_empty_not_none():
    archive = _archive({"search-history.html": SEARCH_HTML_NO_TIMESTAMP})
    validation = validate_zip(youtube.DDP_CATEGORIES, archive)
    reader = ZipArchiveReader(archive, validation.archive_members, Counter())

    df = youtube.search_history_to_df(reader, Counter(), validation)

    assert df.iloc[0]["Timestamp"] == ""
    assert df.iloc[0]["Timestamp"] is not None


def test_extraction_stamps_date_locale_en_for_the_english_ddp():
    archive = _archive({"watch-history.json": WATCH_JSON})
    validation = validate_zip(youtube.DDP_CATEGORIES, archive)

    result = youtube.extraction(archive, validation)

    assert validation.current_ddp_category.language == Language.EN
    assert result.tables  # at least the watch-history table survived
    assert all(t.date_locale == "en" for t in result.tables)


def test_extraction_stamps_date_locale_nl_for_the_dutch_ddp():
    archive = _archive({"kijkgeschiedenis.json": WATCH_JSON})
    validation = validate_zip(youtube.DDP_CATEGORIES, archive)

    result = youtube.extraction(archive, validation)

    assert validation.current_ddp_category.language == Language.NL
    assert result.tables
    assert all(t.date_locale == "nl" for t in result.tables)


def test_watch_history_html_keeps_a_one_digit_hour():
    """English renders drop the leading zero ("3:50:40 PM"); the timestamp
    pattern must not require two hour digits, or most rows lose their time."""
    archive = _archive({"watch-history.html": WATCH_HTML_ONE_DIGIT_HOUR})
    validation = validate_zip(youtube.DDP_CATEGORIES, archive)
    reader = ZipArchiveReader(archive, validation.archive_members, Counter())

    df = youtube.watch_history_to_df(reader, Counter(), validation)

    assert df.iloc[0]["Timestamp"] == "Aug 26, 2026, 3:50:40 PM BST"


COMMENTS_CSV_TITLE_CASE = (
    "Comment ID,Channel ID,Comment Create Timestamp,Price,Video ID,Comment Text\n"
    'c1,ch1,2026-08-26T14:51:06.211488+00:00,0,v1,"{""text"":""hello""}"\n'
)


def test_comments_csv_title_case_header_keeps_timestamp_and_text():
    """Current exports write the header in title case ("Comment Create Timestamp",
    "Comment Text"); the rename map must catch both spellings or the declared
    Timestamp column vanishes from the table."""
    archive = _archive({"comments.csv": COMMENTS_CSV_TITLE_CASE})
    validation = validate_zip(youtube.DDP_CATEGORIES, archive)
    reader = ZipArchiveReader(archive, validation.archive_members, Counter())

    df = youtube.comments_to_df(reader, Counter())

    assert "Timestamp" in df.columns and "Comment text" in df.columns
    assert df.iloc[0]["Timestamp"] == "2026-08-26T14:51:06.211488+00:00"
