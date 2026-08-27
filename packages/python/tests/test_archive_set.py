"""Tests for ArchiveSet / SingleArchiveSource — the ArchiveSource protocol
that presents N uploaded zip parts (or one) as one logical archive with a
canonical union member inventory, provenance, and a materialization-time
per-member size guard.
"""

import io
import zipfile

import pytest

from port.helpers.archive_set import ArchiveSet, SingleArchiveSource, MemberTooLargeError


def _part(name, entries):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for path, content in entries:
            zf.writestr(path, content)
    buf.seek(0)
    buf.name = name
    buf.size = len(buf.getvalue())
    return buf


class TestInventory:
    def test_union_in_canonical_order_regardless_of_selection_order(self):
        p2 = _part("takeout-x-2-001.zip", [("Takeout/YouTube/history/watch-history.html", b"w")])
        p1 = _part("takeout-x-1-001.zip", [("Takeout/Chrome/History.json", b"h")])
        s = ArchiveSet([p2, p1])  # wrong order on purpose
        assert s.members == [
            "Takeout/Chrome/History.json",
            "Takeout/YouTube/history/watch-history.html",
        ]

    def test_exact_duplicate_resolves_to_first_part_and_counts(self):
        p1 = _part("a-1.zip", [("Takeout/archive_browser.html", b"ONE")])
        p2 = _part("a-2.zip", [("Takeout/archive_browser.html", b"TWO")])
        s = ArchiveSet([p1, p2])
        assert s.members.count("Takeout/archive_browser.html") == 1
        assert s.read_member("Takeout/archive_browser.html") == b"ONE"
        assert s.duplicates["DuplicateMemberAcrossParts"] == 1

    def test_read_routes_to_owning_part(self):
        p1 = _part("a-1.zip", [("Takeout/Chrome/History.json", b"chrome")])
        p2 = _part("a-2.zip", [("Takeout/News/articles.txt", b"news")])
        s = ArchiveSet([p1, p2])
        assert s.read_member("Takeout/News/articles.txt") == b"news"
        assert s.part_index_of("Takeout/News/articles.txt") == 1

    def test_corrupt_part_raises_badzipfile(self):
        bad = io.BytesIO(b"not a zip")
        bad.name = "a-2.zip"
        with pytest.raises(zipfile.BadZipFile):
            ArchiveSet([_part("a-1.zip", [("x", b"1")]), bad])

    def test_name_tie_breaks_on_size_regardless_of_input_order(self):
        """Two parts sharing a `.name` are ordered by size (metadata-only,
        no byte reads) — never by the order they were passed in."""
        small = _part("part.zip", [("small.txt", b"s")])
        large = _part("part.zip", [("large.txt", b"l" * 500)])
        assert small.name == large.name
        assert small.size < large.size

        s_wrong_order = ArchiveSet([large, small])
        s_right_order = ArchiveSet([small, large])

        for s in (s_wrong_order, s_right_order):
            assert s.members == ["large.txt", "small.txt"]
            # Canonical order sorts by (name, size): the smaller part is index 0.
            assert s.part_index_of("small.txt") == 0
            assert s.part_index_of("large.txt") == 1


    def test_within_part_duplicate_counted_distinctly_and_resolves_last_entry(self):
        """The zip format allows a repeated path inside one part's central
        directory. That must count under a distinct key from cross-part
        duplicates, and reading it must match plain zipfile semantics
        (the *last* central-directory entry for that name)."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("dup.txt", b"FIRST")
            zf.writestr("dup.txt", b"SECOND")
        buf.seek(0)
        buf.name = "a-1.zip"
        buf.size = len(buf.getvalue())

        s = ArchiveSet([buf])
        assert s.duplicates["DuplicateMemberWithinPart"] == 1
        assert s.duplicates["DuplicateMemberAcrossParts"] == 0
        assert s.members.count("dup.txt") == 1
        assert s.read_member("dup.txt") == b"SECOND"


class TestMemberGuard:
    def test_oversized_member_refused_without_decompression(self, monkeypatch):
        import port.helpers.archive_set as mod

        monkeypatch.setattr(mod, "MAX_MEMBER_UNCOMPRESSED_BYTES", 4)
        p = _part("a-1.zip", [("big.bin", b"123456")])
        s = ArchiveSet([p])
        with pytest.raises(MemberTooLargeError):
            s.read_member("big.bin")


class TestSingleSource:
    def test_wraps_one_archive(self):
        p = _part("only.zip", [("m.json", b"{}")])
        with zipfile.ZipFile(p) as zf:
            members = zf.namelist()
        src = SingleArchiveSource(p, members)
        assert src.members == members and src.read_member("m.json") == b"{}"
