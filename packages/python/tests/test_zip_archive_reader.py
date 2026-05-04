"""Tests for ZipArchiveReader — member resolution, extraction, result types.

Per extraction/AD0007, ZipArchiveReader accepts a seekable binary file-like
(SeekableBinaryReader Protocol) — never a path string. Test fixtures use
io.BytesIO; production callers pass an AsyncFileAdapter directly.
"""
import sys
import io
import json
import zipfile
from collections import Counter
from unittest.mock import MagicMock

sys.modules["js"] = MagicMock()

import pytest
import pandas as pd
from port.helpers.extraction_helpers import (
    ZipArchiveReader,
    JsonExtractionResult,
    CsvExtractionResult,
    RawExtractionResult,
)


def _build_archive(*entries: tuple[str, str | bytes]) -> io.BytesIO:
    """Build an in-memory zip with the given (name, content) entries."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in entries:
            zf.writestr(name, content)
    buf.seek(0)
    return buf


@pytest.fixture
def sample_zip():
    """Build a fresh in-memory archive with known structure for testing.

    Returns (archive, members). The archive is a BytesIO satisfying
    SeekableBinaryReader. Each test gets its own fresh BytesIO so seek
    positions don't leak between tests.
    """
    archive = _build_archive(
        ("data/following.json", json.dumps({"relationships_following": []})),
        ("data/nested/following.json", json.dumps({"other": "data"})),
        ("data/foo_following.json", json.dumps({"wrong": "file"})),
        ("ratings.csv", "Title,Rating\nMovie A,5\nMovie B,3\n"),
        ("Bookmarks.html", "<html><body><a href='http://example.com'>Example</a></body></html>"),
        ("post_comments_1.json", json.dumps([{"comment": "one"}])),
        ("post_comments_2.json", json.dumps([{"comment": "two"}])),
    )
    members = [
        "data/following.json", "data/nested/following.json",
        "data/foo_following.json", "ratings.csv", "Bookmarks.html",
        "post_comments_1.json", "post_comments_2.json",
    ]
    return archive, members


class TestResolveMember:
    def test_exact_match(self, sample_zip):
        archive, members = sample_zip
        reader = ZipArchiveReader(archive, members, Counter())
        assert reader.resolve_member("data/following.json") == "data/following.json"

    def test_suffix_match_unique(self, sample_zip):
        archive, members = sample_zip
        reader = ZipArchiveReader(archive, members, Counter())
        assert reader.resolve_member("ratings.csv") == "ratings.csv"

    def test_suffix_match_path_boundary(self, sample_zip):
        """foo_following.json must NOT match following.json."""
        archive, members = sample_zip
        filtered = [m for m in members if m != "data/nested/following.json"]
        reader = ZipArchiveReader(archive, filtered, Counter())
        result = reader.resolve_member("following.json")
        assert result == "data/following.json"

    def test_no_match_returns_none(self, sample_zip):
        archive, members = sample_zip
        reader = ZipArchiveReader(archive, members, Counter())
        assert reader.resolve_member("nonexistent.json") is None

    def test_ambiguous_match_returns_none_and_counts_error(self, sample_zip):
        """Multiple path-boundary matches → None + AmbiguousMemberMatch."""
        archive, members = sample_zip
        errors = Counter()
        reader = ZipArchiveReader(archive, members, errors)
        result = reader.resolve_member("following.json")
        assert result is None
        assert errors["AmbiguousMemberMatch"] == 1

    def test_exact_match_wins_over_suffix(self):
        """When a file exists at top level AND nested, exact match wins."""
        archive = _build_archive(
            ("ratings.csv", "a,b\n1,2\n"),
            ("data/ratings.csv", "c,d\n3,4\n"),
        )
        members = ["ratings.csv", "data/ratings.csv"]
        reader = ZipArchiveReader(archive, members, Counter())
        assert reader.resolve_member("ratings.csv") == "ratings.csv"


class TestJsonExtraction:
    def test_found(self, sample_zip):
        archive, members = sample_zip
        reader = ZipArchiveReader(archive, members, Counter())
        result = reader.json("data/following.json")
        assert result.found is True
        assert result.data == {"relationships_following": []}
        assert result.member_path == "data/following.json"

    def test_not_found(self, sample_zip):
        archive, members = sample_zip
        errors = Counter()
        reader = ZipArchiveReader(archive, members, errors)
        result = reader.json("nonexistent.json")
        assert result.found is False
        assert result.data == {}
        assert result.member_path is None
        assert errors.get("FileNotFoundInZipError", 0) == 0

    def test_malformed_json(self):
        archive = _build_archive(("bad.json", "not valid json {{{"))
        errors = Counter()
        reader = ZipArchiveReader(archive, ["bad.json"], errors)
        result = reader.json("bad.json")
        assert result.found is True
        assert result.data == {}
        assert errors["JSONDecodeError"] > 0


class TestCsvExtraction:
    def test_found(self, sample_zip):
        archive, members = sample_zip
        reader = ZipArchiveReader(archive, members, Counter())
        result = reader.csv("ratings.csv")
        assert result.found is True
        assert isinstance(result.data, pd.DataFrame)
        assert len(result.data) == 2

    def test_not_found(self, sample_zip):
        archive, members = sample_zip
        reader = ZipArchiveReader(archive, members, Counter())
        result = reader.csv("nonexistent.csv")
        assert result.found is False
        assert result.data.empty


class TestRawExtraction:
    def test_found(self, sample_zip):
        archive, members = sample_zip
        reader = ZipArchiveReader(archive, members, Counter())
        result = reader.raw("Bookmarks.html")
        assert result.found is True
        assert b"Example" in result.data.getvalue()

    def test_not_found(self, sample_zip):
        archive, members = sample_zip
        reader = ZipArchiveReader(archive, members, Counter())
        result = reader.raw("nonexistent.html")
        assert result.found is False
        assert result.data.getvalue() == b""


class TestJsonAll:
    def test_matches_multiple(self, sample_zip):
        archive, members = sample_zip
        reader = ZipArchiveReader(archive, members, Counter())
        results = reader.json_all(r"post_comments_\d+\.json$")
        assert len(results) == 2
        assert all(r.found for r in results)

    def test_sorted_lexicographically(self, sample_zip):
        archive, members = sample_zip
        reader = ZipArchiveReader(archive, members, Counter())
        results = reader.json_all(r"post_comments_\d+\.json$")
        paths = [r.member_path for r in results]
        assert paths == sorted(paths)

    def test_no_matches(self, sample_zip):
        archive, members = sample_zip
        reader = ZipArchiveReader(archive, members, Counter())
        results = reader.json_all(r"nonexistent_\d+\.json$")
        assert results == []


class TestMultipleReads:
    """Successive ZipFile contexts on the same archive object work
    (mirrors AsyncFileAdapter reuse across member accesses).
    """

    def test_multiple_reads_from_same_archive(self, sample_zip):
        archive, members = sample_zip
        reader = ZipArchiveReader(archive, members, Counter())
        r1 = reader.json("data/following.json")
        r2 = reader.csv("ratings.csv")
        assert r1.found and r2.found
