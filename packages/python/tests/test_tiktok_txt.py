"""The TikTok TXT parser treats a sentinel-only file as absent data.

Mirrors packages/mobile-tiktok/src/txt.test.ts (ADR-0041 parity)."""
import io
import zipfile
from collections import Counter

import pytest

from port.helpers.extraction_helpers import ZipArchiveReader
from port.helpers.validate import validate_zip
from port.platforms.tiktok import (
    DDP_CATEGORIES,
    _is_empty_sentinel,
    _item_get,
    _parse_tiktok_txt,
    comments_to_df,
)


@pytest.mark.parametrize("line", [
    "You have no data in this section",
    "  YOU HAVE NO DATA IN THIS SECTION ",
    "Er staan geen gegevens in dit gedeelte",
    "Dit gedeelte bevat geen gegevens",
    "Je hebt geen informatie over platforms van derden",
])
def test_sentinels_match_case_insensitively(line):
    assert _is_empty_sentinel(line)


def test_other_text_is_not_a_sentinel():
    assert not _is_empty_sentinel("Date: 2026-01-01 00:00:00")


def test_sentinel_only_file_parses_to_none():
    assert _parse_tiktok_txt(io.BytesIO(b"You have no data in this section\n")) is None


DUTCH_COMMENT = {
    "Datum": "2026-05-02 10:09:50 UTC",
    "Reactie": "hoi",
    "Sticker": "N.v.t.",
    "Link naar origineel bericht": "https://www.tiktok.com/@x/video/1",
}


def test_dutch_comment_post_link_is_read():
    # Real Dutch TXT exports (2026-09) write the post link under this key.
    url = _item_get(DUTCH_COMMENT, "Url", "Link", "originalPostUrl", "Original Post Link",
                    "Link naar origineel bericht", "Originele link naar bericht")
    assert url == "https://www.tiktok.com/@x/video/1"


def test_dutch_comment_record_parses_with_the_link_key():
    text = "Datum: 2026-05-02 10:09:50 UTC\nReactie: hoi\nSticker: N.v.t.\nLink naar origineel bericht: https://www.tiktok.com/@x/video/1\n"
    parsed = _parse_tiktok_txt(io.BytesIO(text.encode()))
    assert parsed["Link naar origineel bericht"] == "https://www.tiktok.com/@x/video/1"


def test_comments_to_df_fills_url_for_dutch_txt():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("TikTok/Reacties/Reacties.txt",
                   "Datum: 2026-05-02 10:09:50 UTC\nReactie: hoi\nSticker: N.v.t.\nLink naar origineel bericht: https://www.tiktok.com/@x/video/1\n")
    buf.seek(0)
    validation = validate_zip(DDP_CATEGORIES, buf)
    buf.seek(0)
    errors = Counter()
    df = comments_to_df(ZipArchiveReader(buf, validation.archive_members, errors), errors, validation)
    assert list(df["Url"]) == ["https://www.tiktok.com/@x/video/1"]
