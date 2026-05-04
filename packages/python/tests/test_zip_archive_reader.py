"""Tests for ZipArchiveReader — member resolution, extraction, result types."""
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


@pytest.fixture
def sample_zip(tmp_path):
    """Create a zip with known structure for testing."""
    zip_path = tmp_path / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("data/following.json", json.dumps({"relationships_following": []}))
        zf.writestr("data/nested/following.json", json.dumps({"other": "data"}))
        zf.writestr("data/foo_following.json", json.dumps({"wrong": "file"}))
        zf.writestr("ratings.csv", "Title,Rating\nMovie A,5\nMovie B,3\n")
        zf.writestr("Bookmarks.html", "<html><body><a href='http://example.com'>Example</a></body></html>")
        zf.writestr("post_comments_1.json", json.dumps([{"comment": "one"}]))
        zf.writestr("post_comments_2.json", json.dumps([{"comment": "two"}]))
    members = [
        "data/following.json", "data/nested/following.json",
        "data/foo_following.json", "ratings.csv", "Bookmarks.html",
        "post_comments_1.json", "post_comments_2.json",
    ]
    return str(zip_path), members


class TestResolveMember:
    def test_exact_match(self, sample_zip):
        path, members = sample_zip
        reader = ZipArchiveReader(path, members, Counter())
        assert reader.resolve_member("data/following.json") == "data/following.json"

    def test_suffix_match_unique(self, sample_zip):
        path, members = sample_zip
        reader = ZipArchiveReader(path, members, Counter())
        assert reader.resolve_member("ratings.csv") == "ratings.csv"

    def test_suffix_match_path_boundary(self, sample_zip):
        """foo_following.json must NOT match following.json."""
        path, members = sample_zip
        filtered = [m for m in members if m != "data/nested/following.json"]
        reader = ZipArchiveReader(path, filtered, Counter())
        result = reader.resolve_member("following.json")
        assert result == "data/following.json"

    def test_no_match_returns_none(self, sample_zip):
        path, members = sample_zip
        reader = ZipArchiveReader(path, members, Counter())
        assert reader.resolve_member("nonexistent.json") is None

    def test_ambiguous_match_returns_none_and_counts_error(self, sample_zip):
        """Multiple path-boundary matches → None + AmbiguousMemberMatch."""
        path, members = sample_zip
        errors = Counter()
        reader = ZipArchiveReader(path, members, errors)
        result = reader.resolve_member("following.json")
        assert result is None
        assert errors["AmbiguousMemberMatch"] == 1

    def test_exact_match_wins_over_suffix(self, tmp_path):
        """When a file exists at top level AND nested, exact match wins."""
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("ratings.csv", "a,b\n1,2\n")
            zf.writestr("data/ratings.csv", "c,d\n3,4\n")
        members = ["ratings.csv", "data/ratings.csv"]
        reader = ZipArchiveReader(str(zip_path), members, Counter())
        assert reader.resolve_member("ratings.csv") == "ratings.csv"


class TestJsonExtraction:
    def test_found(self, sample_zip):
        path, members = sample_zip
        reader = ZipArchiveReader(path, members, Counter())
        result = reader.json("data/following.json")
        assert result.found is True
        assert result.data == {"relationships_following": []}
        assert result.member_path == "data/following.json"

    def test_not_found(self, sample_zip):
        path, members = sample_zip
        errors = Counter()
        reader = ZipArchiveReader(path, members, errors)
        result = reader.json("nonexistent.json")
        assert result.found is False
        assert result.data == {}
        assert result.member_path is None
        assert errors.get("FileNotFoundInZipError", 0) == 0

    def test_malformed_json(self, tmp_path):
        zip_path = tmp_path / "bad.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("bad.json", "not valid json {{{")
        errors = Counter()
        reader = ZipArchiveReader(str(zip_path), ["bad.json"], errors)
        result = reader.json("bad.json")
        assert result.found is True
        assert result.data == {}
        assert errors["JSONDecodeError"] > 0


class TestCsvExtraction:
    def test_found(self, sample_zip):
        path, members = sample_zip
        reader = ZipArchiveReader(path, members, Counter())
        result = reader.csv("ratings.csv")
        assert result.found is True
        assert isinstance(result.data, pd.DataFrame)
        assert len(result.data) == 2

    def test_not_found(self, sample_zip):
        path, members = sample_zip
        reader = ZipArchiveReader(path, members, Counter())
        result = reader.csv("nonexistent.csv")
        assert result.found is False
        assert result.data.empty


class TestRawExtraction:
    def test_found(self, sample_zip):
        path, members = sample_zip
        reader = ZipArchiveReader(path, members, Counter())
        result = reader.raw("Bookmarks.html")
        assert result.found is True
        assert b"Example" in result.data.getvalue()

    def test_not_found(self, sample_zip):
        path, members = sample_zip
        reader = ZipArchiveReader(path, members, Counter())
        result = reader.raw("nonexistent.html")
        assert result.found is False
        assert result.data.getvalue() == b""


class TestJsonAll:
    def test_matches_multiple(self, sample_zip):
        path, members = sample_zip
        reader = ZipArchiveReader(path, members, Counter())
        results = reader.json_all(r"post_comments_\d+\.json$")
        assert len(results) == 2
        assert all(r.found for r in results)

    def test_sorted_lexicographically(self, sample_zip):
        path, members = sample_zip
        reader = ZipArchiveReader(path, members, Counter())
        results = reader.json_all(r"post_comments_\d+\.json$")
        paths = [r.member_path for r in results]
        assert paths == sorted(paths)

    def test_no_matches(self, sample_zip):
        path, members = sample_zip
        reader = ZipArchiveReader(path, members, Counter())
        results = reader.json_all(r"nonexistent_\d+\.json$")
        assert results == []


class TestFileLikeAcceptance:
    """ZipArchiveReader must accept a seekable binary file-like archive
    (e.g. AsyncFileAdapter), not just a path string. AD0007.
    """

    @pytest.fixture
    def sample_archive_buf(self):
        """Build the same archive as `sample_zip` but as a BytesIO buffer."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("data/following.json", json.dumps({"relationships_following": []}))
            zf.writestr("ratings.csv", "Title,Rating\nMovie A,5\nMovie B,3\n")
        members = ["data/following.json", "ratings.csv"]
        return buf, members

    def test_json_extraction_from_bytesio(self, sample_archive_buf):
        """Reader extracts JSON from a BytesIO-backed archive."""
        buf, members = sample_archive_buf
        reader = ZipArchiveReader(buf, members, Counter())
        result = reader.json("data/following.json")
        assert result.found is True
        assert result.data == {"relationships_following": []}

    def test_csv_extraction_from_bytesio(self, sample_archive_buf):
        """Reader extracts CSV from a BytesIO-backed archive."""
        buf, members = sample_archive_buf
        reader = ZipArchiveReader(buf, members, Counter())
        result = reader.csv("ratings.csv")
        assert result.found is True
        assert len(result.data) == 2

    def test_multiple_reads_from_same_bytesio(self, sample_archive_buf):
        """Successive ZipFile contexts on the same archive object work
        (mirrors AsyncFileAdapter reuse across member accesses).
        """
        buf, members = sample_archive_buf
        reader = ZipArchiveReader(buf, members, Counter())
        r1 = reader.json("data/following.json")
        r2 = reader.csv("ratings.csv")
        assert r1.found and r2.found
