"""The TikTok TXT parser treats a sentinel-only file as absent data.

Mirrors packages/mobile-tiktok/src/txt.test.ts (ADR-0041 parity)."""
import io

import pytest

from port.platforms.tiktok import _is_empty_sentinel, _parse_tiktok_txt


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
