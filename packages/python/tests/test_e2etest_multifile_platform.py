"""Tests for the e2e-only multi-file demo platform.

``e2etest_multifile`` exists so the Playwright suite can exercise the
`PayloadFiles` upload path (`tests/multifile.spec.ts`) end-to-end: a
participant selects two zip parts, ``FlowBuilder`` unions them into one
``ArchiveSet`` (ADR-0040), and this platform's extractor produces a table
whose rows are provably sourced from both parts. Like ``e2etest``, it is
excluded from release discovery and the shipped wheel — see release.sh,
scripts/build_release_wheel.sh, and scripts/verify_release_wheel.py.
"""
import io
import zipfile
from collections import Counter

import pandas as pd
import pytest

from port.helpers.archive_set import ArchiveSet
from port.helpers.validate import ValidateInput


def _part(name: str, entries: list[tuple[str, bytes]]) -> io.BytesIO:
    """Build an in-memory zip part with the `name`/`size` attributes ArchiveSet needs."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for path, content in entries:
            zf.writestr(path, content)
    buf.seek(0)
    buf.name = name
    buf.size = len(buf.getvalue())
    return buf


def _two_part_archive_set() -> ArchiveSet:
    part1 = _part("takeout-test-1-001.zip", [("Takeout/one.txt", b"one")])
    part2 = _part("takeout-test-2-001.zip", [("Takeout/two.txt", b"two")])
    return ArchiveSet([part1, part2])


def test_standard_platform_interface():
    """The module satisfies the standard platform interface so script.py can
    dispatch to it like any other platform."""
    from port.platforms import e2etest_multifile

    assert callable(e2etest_multifile.extraction)
    assert callable(e2etest_multifile.process)
    assert "archive_membership_to_df" in e2etest_multifile.EXTRACTOR_REGISTRY
    assert "archive_content_preview_to_df" in e2etest_multifile.EXTRACTOR_REGISTRY


def test_expects_payload_files():
    """The flow declares the multi-file payload so FlowBuilder builds an
    ArchiveSet from the uploaded parts before validate_file/extract_data run."""
    from port.platforms.e2etest_multifile import E2eTestMultifileFlow

    flow = E2eTestMultifileFlow("session-1")
    assert flow.expected_file_payload == "PayloadFiles"
    assert flow.platform_name == "e2etest_multifile"


def test_validate_file_returns_status_zero_with_archive_members():
    """validate_file accepts any readable ArchiveSet (FlowBuilder has already
    proven it opens as a zip by constructing it) and records the union
    member inventory for the extractor."""
    from port.platforms.e2etest_multifile import E2eTestMultifileFlow

    archive_set = _two_part_archive_set()
    flow = E2eTestMultifileFlow("session-1")
    validation = flow.validate_file(archive_set)

    assert validation.get_status_code_id() == 0
    assert validation.archive_members == sorted(["Takeout/one.txt", "Takeout/two.txt"])


def _table(result, table_id):
    for table in result.tables:
        if table.id == table_id:
            return table
    raise AssertionError(f"no table with id={table_id!r} in {[t.id for t in result.tables]}")


def test_extract_data_produces_rows_from_both_parts():
    """Both tables' rows are sourced from members of both uploaded parts, not
    just the first — this is what tests/multifile.spec.ts asserts in the
    browser. Membership is metadata-only (part_index_of); content_preview
    additionally proves member BYTES were read (via ArchiveSet.read_member),
    not just central-directory metadata — the plumbing this demo platform
    exists to exercise."""
    from port.platforms.e2etest_multifile import E2eTestMultifileFlow

    archive_set = _two_part_archive_set()
    flow = E2eTestMultifileFlow("session-1")
    validation = flow.validate_file(archive_set)
    result = flow.extract_data(archive_set, validation)

    assert len(result.tables) == 2

    membership = _table(result, "e2etest_multifile_membership").data_frame
    assert set(membership["filename"]) == {"Takeout/one.txt", "Takeout/two.txt"}
    assert set(membership["part_index"]) == {0, 1}

    content = _table(result, "e2etest_multifile_content_preview").data_frame
    by_part = dict(zip(content["part_index"], content["content_preview"]))
    assert by_part[0] == "one"   # Takeout/one.txt's actual content, part 0
    assert by_part[1] == "two"   # Takeout/two.txt's actual content, part 1


def test_extraction_delegates_to_registered_extractor(monkeypatch):
    """extraction() drives every registered extractor through
    ZipArchiveReader/run_extraction like every other platform, rather than
    hand-rolling table assembly."""
    from port.platforms import e2etest_multifile

    calls = []

    def fake_extractor(reader, errors):
        calls.append((reader, errors))
        return pd.DataFrame({"filename": ["x"], "part_index": [0]})

    monkeypatch.setitem(e2etest_multifile.EXTRACTOR_REGISTRY, "archive_membership_to_df", fake_extractor)

    archive_set = _two_part_archive_set()
    v = ValidateInput([], [])
    v.archive_members = archive_set.members
    result = e2etest_multifile.extraction(archive_set, v)

    assert len(calls) == 1
    membership = _table(result, "e2etest_multifile_membership").data_frame
    assert membership["filename"].tolist() == ["x"]


def test_archive_membership_to_df_reads_through_zip_archive_reader():
    """The extractor works over an ArchiveSource-backed ZipArchiveReader —
    unlike example.py's file_stats_to_df, which returns empty for
    ArchiveSource-backed readers because it needs direct zipfile access."""
    from port.helpers.extraction_helpers import ZipArchiveReader
    from port.platforms.e2etest_multifile import archive_membership_to_df

    archive_set = _two_part_archive_set()
    errors: Counter = Counter()
    reader = ZipArchiveReader(archive_set, archive_set.members, errors)

    df = archive_membership_to_df(reader, errors)

    assert not df.empty
    assert set(df["filename"]) == {"Takeout/one.txt", "Takeout/two.txt"}


def test_archive_membership_to_df_has_exactly_one_filename_column():
    """Regression pin: an earlier revision carried both `filename` and
    `basename` with near-duplicate headers ("Filename" / "File name"),
    which rendered as two visually-identical columns in the consent table
    for these flat (no-directory) fixtures. Exactly one filename column
    (plus part_index) must remain."""
    from port.helpers.extraction_helpers import ZipArchiveReader
    from port.platforms.e2etest_multifile import archive_membership_to_df

    archive_set = _two_part_archive_set()
    errors: Counter = Counter()
    reader = ZipArchiveReader(archive_set, archive_set.members, errors)

    df = archive_membership_to_df(reader, errors)

    assert list(df.columns) == ["filename", "part_index"]


def test_archive_content_preview_to_df_reads_member_bytes_from_both_parts():
    """Pins the core plumbing archive_membership_to_df cannot prove:
    content_preview must come from an actual ArchiveSet.read_member() call
    (per-part reopen + the materialization-time size guard), not from
    filenames or central-directory metadata. If this extractor were changed
    to read only names, this test would fail — the two members' distinct
    byte content (b"one" / b"two") only shows up in the output if
    reader.raw() -> ArchiveSet.read_member() actually ran."""
    from port.helpers.extraction_helpers import ZipArchiveReader
    from port.platforms.e2etest_multifile import archive_content_preview_to_df

    archive_set = _two_part_archive_set()
    errors: Counter = Counter()
    reader = ZipArchiveReader(archive_set, archive_set.members, errors)

    df = archive_content_preview_to_df(reader, errors)

    assert not df.empty
    assert len(df) == 2
    by_filename = dict(zip(df["filename"], df["content_preview"]))
    assert by_filename["Takeout/one.txt"] == "one"
    assert by_filename["Takeout/two.txt"] == "two"
    # part_index_of confirms these came from two DIFFERENT parts, not one
    # part read twice.
    assert set(df["part_index"]) == {0, 1}


def test_archive_content_preview_to_df_enforces_member_size_guard(monkeypatch):
    """A member whose declared uncompressed size exceeds
    MAX_MEMBER_UNCOMPRESSED_BYTES raises MemberTooLargeError inside
    ArchiveSet.read_member. reader.raw()'s _read_member_bytes catches it
    (like any other read exception), increments errors[type(e).__name__],
    and returns empty bytes with found still True — so the extractor keeps
    running (one row, empty preview) instead of the whole extraction
    aborting, while the guard's own error is still visible in the
    extraction-summary log. Lowering the threshold (rather than writing a
    real oversized member) mirrors test_archive_set.py's own pattern."""
    import port.helpers.archive_set as archive_set_module
    from port.helpers.extraction_helpers import ZipArchiveReader
    from port.platforms.e2etest_multifile import archive_content_preview_to_df

    monkeypatch.setattr(archive_set_module, "MAX_MEMBER_UNCOMPRESSED_BYTES", 2)
    archive_set = _two_part_archive_set()  # members are b"one"/b"two" — 3 bytes, over the 2-byte cap

    errors: Counter = Counter()
    reader = ZipArchiveReader(archive_set, archive_set.members, errors)
    df = archive_content_preview_to_df(reader, errors)

    assert len(df) == 2  # found=True for both — the row-per-member contract holds
    assert set(df["content_preview"]) == {""}  # empty bytes on the guarded error path
    assert errors["MemberTooLargeError"] == 2
