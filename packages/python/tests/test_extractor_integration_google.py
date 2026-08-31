"""Integration canaries for the Google platform (12 extractors).

Requires local fixture SETS at ``tests/ddp/google_set_<name>/`` — directories
of Takeout zip parts, real or scrubbed. ADR-0014: fixture sets are never
committed; this whole file collects and skips cleanly against an empty
``tests/ddp/``.

Placing fixtures locally (copy OR symlink — both are equally untracked;
symlinks avoid duplicating multi-GB exports on disk)::

    DDP=packages/python/tests/ddp
    mkdir -p "$DDP/google_set_<name>"
    ln -s /path/to/export/*.zip "$DDP/google_set_<name>/"
    # or: cp /path/to/export/*.zip "$DDP/google_set_<name>/"

The sets this module was verified against (2026-08-30), each a directory of
real or scrubbed Takeout zip parts:

- ``uu-acct``, ``uu-acct-nl``, ``uu-acct-es``, ``uu-acct-ar``, ``uu-acct-tr``,
  ``uu-acct-zh``, ``uu-acct-de`` — the same UU study-shaped account, one
  export per locale (``~/data/d3i/self/google-takeout/<name>/``).
- ``gmail-acct`` — a separate, heavier personal account; its manifest is the
  Failed-Files detector's positive fixture (6 real failure entries) and it
  carries the browser-renamed duplicate download (``...-001 (1).zip``).
- ``en_vid``, ``nl_vid`` — the algosoc-2026 fork's scrubbed copies of
  gmail-acct and uu-acct-nl respectively (same account, same export
  timestamps; large media stripped). See ``EXPECT_NON_EMPTY``'s docstring
  note below for what "scrubbed" turned out to mean for the Locations
  column.

Each set directory holds only the platform's own ``*.zip`` parts —
``open_fixture_set`` globs ``*.zip``, so any non-zip file (e.g. a fixture
set's own README) is harmless but also not needed there.

``EXPECT_NON_EMPTY`` pins, per known set, which extractors must produce
rows; it was populated from the first verified run against the local sets
above and is a canary (trips on regressions), not a spec — a set absent from
this map falls back to "must recognize + at least one extractor produces
rows" (``test_set_has_at_least_one_nonempty_table_when_unpinned``), so a
fixture set added later without updating this file still gets a minimal
tripwire.

Locations content (real activity records carrying Maps captions,
``locationInfos``/the ``Locations`` column) is present in every REAL set's
``search_history`` table (a handful to ~12.8k rows depending on account) but
is entirely absent — every row blank — in both fork ``*_vid`` sets, even
though row counts otherwise match their real counterparts almost exactly.
That is the scrub, not a bug: the fork stripped location captions along with
large media. ``SETS_WITH_LOCATION_CONTENT`` / ``test_search_history_location_content``
encode this as a tripwire in both directions — a real set losing its location
content, or a *_vid set unexpectedly gaining some, both fail loudly.
"""
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pytest

import extractor_integration_helpers as eih
from extractor_integration_helpers import find_fixture_sets, open_fixture_set
from port.helpers.archive_set import ArchiveSet
from port.helpers.extraction_helpers import ZipArchiveReader
from port.platforms import google

GOOGLE_SETS = find_fixture_sets("google")
_SET_IDS = [p.name for p in GOOGLE_SETS] or ["no-fixtures"]
_SET_PARAMS: list[Path | None] = list(GOOGLE_SETS) or [None]
_NO_FIXTURES_REASON = (
    "No google_set_*/ fixture sets found in tests/ddp/ (ADR-0014: real/scrubbed "
    "exports are never committed — see this module's docstring for the local copy step)"
)

#: Expected DDP locale per known set name, checked by ``test_set_is_recognized``.
EXPECTED_LOCALE: dict[str, str] = {
    "google_set_uu-acct": "en",
    "google_set_uu-acct-nl": "nl",
    "google_set_uu-acct-es": "es",
    "google_set_uu-acct-ar": "ar",
    "google_set_uu-acct-tr": "tr",
    "google_set_uu-acct-zh": "zh",
    "google_set_uu-acct-de": "de",
    "google_set_gmail-acct": "en",
    "google_set_en_vid": "en",
    "google_set_nl_vid": "nl",
}

#: The six extractors the UU study account (Chrome-less, subscription-less,
#: no News/Discover activity) exercises, shared by every uu-acct* locale
#: export and its fork counterparts.
_UU_ACCOUNT_TABLES = {
    "youtube_watch_history_to_df",
    "youtube_search_history_to_df",
    "search_history_to_df",
    "video_search_history_to_df",
    "ads_history_to_df",
    "google_news_history_to_df",
}

#: All 12 extractors — the heavier gmail-acct/en_vid account (and only that
#: account, among the local sets) exercises every one of them.
_ALL_TABLES = set(google.EXTRACTOR_REGISTRY)

#: Populated from the verified run against the local sets (2026-08-30). A set
#: not listed here falls back to the weaker "recognized + at least one table
#: non-empty" check below — see the module docstring.
EXPECT_NON_EMPTY: dict[str, set[str]] = {
    "google_set_gmail-acct": _ALL_TABLES,
    "google_set_en_vid": _ALL_TABLES,
    "google_set_uu-acct": _UU_ACCOUNT_TABLES,
    "google_set_uu-acct-nl": _UU_ACCOUNT_TABLES,
    "google_set_uu-acct-es": _UU_ACCOUNT_TABLES,
    "google_set_uu-acct-ar": _UU_ACCOUNT_TABLES,
    "google_set_uu-acct-tr": _UU_ACCOUNT_TABLES,
    "google_set_uu-acct-zh": _UU_ACCOUNT_TABLES,
    "google_set_uu-acct-de": _UU_ACCOUNT_TABLES,
    "google_set_nl_vid": _UU_ACCOUNT_TABLES,
}

#: Sets whose manifest (archive_browser.html) reports the real 6 Failed-Files
#: entries (gmail-acct's positive fixture and its fork scrub).
FAILED_FILES_SETS = {"google_set_gmail-acct", "google_set_en_vid"}

#: Sets containing the browser-renamed duplicate download
#: (``takeout-...-001 (1).zip`` alongside ``takeout-...-001.zip``).
DUPLICATE_SETS = {"google_set_gmail-acct", "google_set_en_vid"}

#: Sets whose real search-history activity carries nonblank Locations
#: (Maps captions on an activity) — see the module docstring's Locations note.
SETS_WITH_LOCATION_CONTENT = {
    "google_set_gmail-acct",
    "google_set_uu-acct",
    "google_set_uu-acct-nl",
    "google_set_uu-acct-es",
    "google_set_uu-acct-ar",
    "google_set_uu-acct-tr",
    "google_set_uu-acct-zh",
    "google_set_uu-acct-de",
}

#: Error-counter keys a clean run over a real (or realistically messy)
#: export may legitimately carry. Anything else surfacing here is a real
#: extraction failure, not expected archive noise.
ALLOWED_ERROR_KEYS = {
    "ExportReportedFailedFiles",
    "DuplicateMemberAcrossParts",
    "DuplicateMemberWithinPart",
}


@dataclass
class _SetContext:
    archive_set: ArchiveSet
    validation: "google.GoogleValidation"


#: Module-scoped cache keyed by fixture-set directory: validating and
#: building the ArchiveSet (central-directory reads only, per ADR-0040 — no
#: member bytes touched) is shared across every (set × extractor) test case
#: instead of repeating it once per extractor.
_SET_CACHE: dict[Path, _SetContext] = {}


def _context_for(set_dir: Path) -> _SetContext:
    if set_dir not in _SET_CACHE:
        archive_set = open_fixture_set(set_dir)
        validation = google.validate_ddp(archive_set)
        _SET_CACHE[set_dir] = _SetContext(archive_set, validation)
    return _SET_CACHE[set_dir]


# ---------------------------------------------------------------------------
# Registry completeness — static, no fixtures required (ADR-0027)
# ---------------------------------------------------------------------------


def test_every_extractor_is_pinned_somewhere():
    """Every extractor in ``google.EXTRACTOR_REGISTRY`` must appear in the union
    of at least one fixture set's ``EXPECT_NON_EMPTY`` pins.

    Per-set canaries (``test_extractor_against_set``) only assert non-emptiness
    for the extractors a set is pinned for — deliberately, since a real export
    legitimately leaves some products unused (ADR-0027's multi-set amendment).
    That means an extractor with NO pin anywhere is invisible to every one of
    those per-set assertions: add a 13th extractor to the registry and forget
    to pin it in any set, and the whole canary suite passes green while that
    extractor is silently never exercised. This test is the tripwire for that
    specific gap — pure static check over the two module-level dicts, so it
    runs (and fails loudly) in CI with no local fixtures present at all.
    """
    pinned = set().union(*EXPECT_NON_EMPTY.values()) if EXPECT_NON_EMPTY else set()
    unpinned = set(google.EXTRACTOR_REGISTRY) - pinned
    assert not unpinned, (
        f"Extractor(s) {sorted(unpinned)} appear in google.EXTRACTOR_REGISTRY but are not "
        "pinned non-empty by any EXPECT_NON_EMPTY set — add a pin (or an explicit note why "
        "no local fixture ever exercises it) before this passes silently uncovered."
    )


# ---------------------------------------------------------------------------
# Recognition
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("set_dir", _SET_PARAMS, ids=_SET_IDS)
def test_set_is_recognized(set_dir):
    if set_dir is None:
        pytest.skip(_NO_FIXTURES_REASON)
    ctx = _context_for(set_dir)
    assert ctx.validation.get_status_code_id() == 0, (
        f"{set_dir.name} was not recognized as a Google DDP"
    )
    expected_locale = EXPECTED_LOCALE.get(set_dir.name)
    if expected_locale is not None:
        assert ctx.validation.ddp_locale == expected_locale, (
            f"{set_dir.name} detected as locale {ctx.validation.ddp_locale!r}, "
            f"expected {expected_locale!r}"
        )


# ---------------------------------------------------------------------------
# Per-extractor canaries
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("set_dir", _SET_PARAMS, ids=_SET_IDS)
@pytest.mark.parametrize("name", list(google.EXTRACTOR_REGISTRY), ids=list(google.EXTRACTOR_REGISTRY))
def test_extractor_against_set(name, set_dir):
    if set_dir is None:
        pytest.skip(_NO_FIXTURES_REASON)
    ctx = _context_for(set_dir)
    if ctx.validation.get_status_code_id() != 0:
        pytest.skip(f"{set_dir.name} not recognized as a Google DDP")

    errors: Counter = Counter()
    reader = ZipArchiveReader(ctx.archive_set, ctx.validation.archive_members, errors)
    df = google.EXTRACTOR_REGISTRY[name](reader, errors, ddp_locale=ctx.validation.ddp_locale)

    expected = EXPECT_NON_EMPTY.get(set_dir.name)
    if expected is not None and name in expected:
        assert not df.empty, (
            f"{name} returned an empty DataFrame for {set_dir.name} — the extractor "
            "may have crashed, found no matching file, or the DDP format changed."
        )


@pytest.mark.parametrize("set_dir", _SET_PARAMS, ids=_SET_IDS)
def test_set_has_at_least_one_nonempty_table_when_unpinned(set_dir):
    """Fallback for a set not yet in ``EXPECT_NON_EMPTY``: at minimum, some
    extractor must produce rows, or the whole set is silently dead."""
    if set_dir is None:
        pytest.skip(_NO_FIXTURES_REASON)
    if set_dir.name in EXPECT_NON_EMPTY:
        pytest.skip(f"{set_dir.name} has explicit EXPECT_NON_EMPTY pins — see test_extractor_against_set")
    ctx = _context_for(set_dir)
    if ctx.validation.get_status_code_id() != 0:
        pytest.skip(f"{set_dir.name} not recognized as a Google DDP")

    errors: Counter = Counter()
    reader = ZipArchiveReader(ctx.archive_set, ctx.validation.archive_members, errors)
    non_empty = [
        name
        for name, fn in google.EXTRACTOR_REGISTRY.items()
        if not fn(reader, errors, ddp_locale=ctx.validation.ddp_locale).empty
    ]
    assert non_empty, f"No extractor produced rows for unpinned set {set_dir.name}"


# ---------------------------------------------------------------------------
# Whole-extraction canary: error-counter keys, Failed-Files, duplicates
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("set_dir", _SET_PARAMS, ids=_SET_IDS)
def test_whole_extraction_error_keys(set_dir):
    if set_dir is None:
        pytest.skip(_NO_FIXTURES_REASON)
    ctx = _context_for(set_dir)
    if ctx.validation.get_status_code_id() != 0:
        pytest.skip(f"{set_dir.name} not recognized as a Google DDP")

    result = google.extraction(ctx.archive_set, ctx.validation)

    unexpected = set(result.errors) - ALLOWED_ERROR_KEYS
    assert not unexpected, (
        f"Unexpected error-counter keys for {set_dir.name}: {unexpected} "
        f"(full counter: {dict(result.errors)})"
    )

    if set_dir.name in FAILED_FILES_SETS:
        assert result.errors.get("ExportReportedFailedFiles") == 6, (
            f"{set_dir.name} expected the manifest's 6 Failed-Files entries, got "
            f"{result.errors.get('ExportReportedFailedFiles')}"
        )
    else:
        assert "ExportReportedFailedFiles" not in result.errors, (
            f"{set_dir.name} unexpectedly reports ExportReportedFailedFiles "
            "(expected a clean or absent manifest for this set)"
        )

    if set_dir.name in DUPLICATE_SETS:
        assert result.errors.get("DuplicateMemberAcrossParts", 0) >= 1, (
            f"{set_dir.name} contains the browser-renamed duplicate zip but no "
            "DuplicateMemberAcrossParts was counted"
        )


# ---------------------------------------------------------------------------
# Locations content — real exports vs. the fork's scrubbed sets
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("set_dir", _SET_PARAMS, ids=_SET_IDS)
def test_search_history_location_content(set_dir):
    if set_dir is None:
        pytest.skip(_NO_FIXTURES_REASON)
    ctx = _context_for(set_dir)
    if ctx.validation.get_status_code_id() != 0:
        pytest.skip(f"{set_dir.name} not recognized as a Google DDP")

    errors: Counter = Counter()
    reader = ZipArchiveReader(ctx.archive_set, ctx.validation.archive_members, errors)
    df = google.search_history_to_df(reader, errors, ddp_locale=ctx.validation.ddp_locale)
    nonblank = int((df["Locations"].astype(str).str.strip() != "").sum()) if "Locations" in df.columns else 0

    if set_dir.name in SETS_WITH_LOCATION_CONTENT:
        assert nonblank > 0, (
            f"{set_dir.name} expected Locations content in search_history but found none"
        )
    else:
        assert nonblank == 0, (
            f"{set_dir.name} unexpectedly carries Locations content in search_history "
            "(scrub regression, or SETS_WITH_LOCATION_CONTENT is out of date)"
        )


# ---------------------------------------------------------------------------
# Content-quality canaries — mojibake and impossible timestamps
#
# Unlike the per-extractor non-empty canary above (ADR-0027: one assertion,
# "did rows come out at all"), these two look *inside* the rows every
# extractor produced, across the whole set in one pass. They exist because
# "non-empty" is blind to a whole class of real bugs: an extractor can
# faithfully return every row and still have corrupted every one of them
# (missing charset -> mojibake) or silently misdated every one of them (a
# day/month swap). Both bugs were confirmed against real fixture sets before
# either canary was written: the mojibake canary's pre-fix run against
# google_set_uu-acct-de failed on 'Ã¤'/'Â '-corrupted titles in the German
# search_history table until the HTML parser was pinned to UTF-8.
# ---------------------------------------------------------------------------

#: The byte-level double-decode markers a UTF-8 multi-byte character leaves
#: behind when read as latin-1: 'Ã' opens every mis-decoded non-ASCII character
#: (e.g. 'ä' -> 'Ã¤'), 'Â ' is what a mis-decoded NBSP (U+00A0) turns into
#: specifically. A legitimate title could in principle contain these by
#: coincidence; if a real fixture ever trips this legitimately, give it a
#: per-set allowlist here.
MOJIBAKE_MARKERS = ("Ã", "Â ")

#: The legitimate case above, confirmed: gmail-acct's ``Search/My Activity.html``
#: carries titles corrupted *in the raw zip member bytes themselves*, before any
#: parsing — e.g. ``b"R\xc3\x83\xc2\xa4uber"`` for "Räuber", a triple-encoding
#: baked in upstream of Takeout's export (verified by reading the member's bytes
#: directly out of the zip; BUG A's fix, pinning the HTML parser to UTF-8,
#: changes nothing here because the corruption predates parsing). ``en_vid`` is
#: the algosoc-2026 fork's scrubbed copy of the same account (module docstring)
#: and inherits the same titles verbatim.
MOJIBAKE_ALLOWLIST_SETS = {"google_set_gmail-acct", "google_set_en_vid"}

#: (table, Timestamp value) pairs the timestamp-parse canary below skips, each
#: with byte-level evidence a real record — not a format the timestamp
#: converter should have read — produces it. Same allowlist discipline as
#: ``MOJIBAKE_ALLOWLIST_SETS``: narrow, evidence-backed, flagged, never a
#: silent catch-all.
#:
#: gmail-acct's ``My Activity/Search/My Activity.html`` writes a voice-search
#: activity's audio player into the *second* content-cell — the one every
#: other record leaves empty (see ``test_the_empty_cell_beside_an_activity_is_not_a_record``
#: in test_google_timestamps.py). Confirmed by reading the raw member bytes at
#: that record: the cell holds
#: ``<audio controls><source src="....mp3" type="audio/mpeg">Audio file:
#: ....mp3 (located in the same directory as this page).</audio>`` — no
#: ``<br>``-separated timestamp line follows the fallback text, so the html
#: parser (built for the "second cell is empty or absent" case) reads this
#: audio-fallback text as its own one-line activity, using the same text for
#: both title and time (``{"title": "Audio file: ...", "time": "Audio file:
#: ..."}``) — the structural signature this allowlist matches on, since
#: ``en_vid``'s scrub replaces the audio filename text with lorem-ipsum
#: placeholder text at the same 9 row positions (module docstring's scrub
#: note), so the literal "Audio file: " prefix survives only in the real set.
#: This is a distinct parsing bug (an unhandled audio-player shape, not a
#: date-format one) — out of scope for Task 8c; tracked in
#: ~/src/d3i-infra/PENDING_ISSUES.md.
TIMESTAMP_ALLOWLIST_SETS = {"google_set_gmail-acct", "google_set_en_vid"}
TIMESTAMP_ALLOWLIST_TABLE = "search_history_to_df"


def _string_cells(df):
    """Yield (column, value) for every string-valued cell of *df*, table order."""
    for column in df.columns:
        for value in df[column]:
            if isinstance(value, str):
                yield column, value


@pytest.mark.parametrize("set_dir", _SET_PARAMS, ids=_SET_IDS)
def test_no_mojibake_in_any_table(set_dir):
    """No extracted table's string content may carry a double-decode marker.

    Catches BUG A: the activity HTML declares no charset, so a parser that
    does not pin UTF-8 explicitly (lxml defaults to latin-1) mangles every
    non-ASCII character across every locale's export, not just accented
    Latin scripts — ar/zh content, entirely non-Latin, would be fully
    garbled rather than merely accented-letter-wrong.
    """
    if set_dir is None:
        pytest.skip(_NO_FIXTURES_REASON)
    if set_dir.name in MOJIBAKE_ALLOWLIST_SETS:
        pytest.skip(
            f"{set_dir.name}: known pre-existing source-data mojibake, baked into "
            "the raw export bytes upstream of extraction — see MOJIBAKE_ALLOWLIST_SETS"
        )
    ctx = _context_for(set_dir)
    if ctx.validation.get_status_code_id() != 0:
        pytest.skip(f"{set_dir.name} not recognized as a Google DDP")

    errors: Counter = Counter()
    reader = ZipArchiveReader(ctx.archive_set, ctx.validation.archive_members, errors)
    offenders = []
    for name, fn in google.EXTRACTOR_REGISTRY.items():
        df = fn(reader, errors, ddp_locale=ctx.validation.ddp_locale)
        for column, value in _string_cells(df):
            for marker in MOJIBAKE_MARKERS:
                if marker in value:
                    offenders.append((name, column, marker, value[:80]))
                    break

    assert not offenders, (
        f"{set_dir.name}: mojibake marker(s) found in extracted content "
        f"(table, column, marker, sample) — first 10: {offenders[:10]}"
    )


def _export_date_bound(set_dir: Path) -> datetime:
    """The latest ``ZipInfo.date_time`` across every zip part in *set_dir*, plus
    a 1-day margin. The zip entries carry the export timestamp (they are
    written when Takeout builds the archive), so no activity record inside it
    should postdate the export itself."""
    latest = None
    for zip_path in sorted(set_dir.glob("*.zip")):
        with zipfile.ZipFile(zip_path) as zf:
            for info in zf.infolist():
                stamp = datetime(*info.date_time)
                if latest is None or stamp > latest:
                    latest = stamp
    assert latest is not None, f"{set_dir.name}: no zip entries found to derive an export-date bound"
    return latest + timedelta(days=1)


@pytest.mark.parametrize("set_dir", _SET_PARAMS, ids=_SET_IDS)
def test_no_impossible_timestamps_in_any_table(set_dir):
    """No ISO-parseable timestamp in any extracted table may postdate the set's
    own export.

    Catches BUG B: a numeric day/month swap can misparse a valid past date
    (e.g. 12 July) into an impossible future one (e.g. 7 December, months
    after the export). Cells that are not ISO-parseable are skipped — an
    unparsed/fallback timestamp is the parsing tests' concern, not this
    canary's.
    """
    if set_dir is None:
        pytest.skip(_NO_FIXTURES_REASON)
    ctx = _context_for(set_dir)
    if ctx.validation.get_status_code_id() != 0:
        pytest.skip(f"{set_dir.name} not recognized as a Google DDP")

    bound = _export_date_bound(set_dir)
    errors: Counter = Counter()
    reader = ZipArchiveReader(ctx.archive_set, ctx.validation.archive_members, errors)
    offenders = []
    for name, fn in google.EXTRACTOR_REGISTRY.items():
        df = fn(reader, errors, ddp_locale=ctx.validation.ddp_locale)
        for column, value in _string_cells(df):
            try:
                parsed = datetime.fromisoformat(value)
            except ValueError:
                continue
            if parsed.tzinfo is not None:
                # A handful of columns (e.g. youtube_comments_to_df's Timestamp)
                # carry an offset-aware ISO string from a different conversion
                # path than the activity-HTML one these bugs live in. The
                # export-date bound is itself naive (ZipInfo.date_time carries
                # no timezone), so drop the offset rather than fail to compare
                # at all — the 1-day margin already absorbs this slack.
                parsed = parsed.replace(tzinfo=None)
            if parsed > bound:
                offenders.append((name, column, value))

    assert not offenders, (
        f"{set_dir.name}: timestamp(s) later than the export bound {bound.isoformat()} "
        f"(table, column, value) — first 10: {offenders[:10]}"
    )


@pytest.mark.parametrize("set_dir", _SET_PARAMS, ids=_SET_IDS)
def test_timestamps_parse_in_every_table(set_dir):
    """Every non-empty ``Timestamp`` cell in any extracted table must be
    ISO-8601-parseable.

    Unlike ``test_no_impossible_timestamps_in_any_table`` above (which skips a
    cell it cannot parse — an unparsed/fallback timestamp is the parsing
    tests' concern, not that canary's), this one requires every non-empty
    ``Timestamp`` cell to parse at all. Catches BUG C: a date written in a
    script or format none of the fast paths *or* ``dateutil`` can read is
    silently left as the raw source string (e.g. a CJK date — confirmed
    against google_set_uu-acct-zh, where Timestamp cells were left as raw
    CJK source strings until this canary and google.py's matching fast path
    were both added), which a downstream date-grouped visualization then
    fails to render.
    """
    if set_dir is None:
        pytest.skip(_NO_FIXTURES_REASON)
    ctx = _context_for(set_dir)
    if ctx.validation.get_status_code_id() != 0:
        pytest.skip(f"{set_dir.name} not recognized as a Google DDP")

    errors: Counter = Counter()
    reader = ZipArchiveReader(ctx.archive_set, ctx.validation.archive_members, errors)
    offenders = []
    for name, fn in google.EXTRACTOR_REGISTRY.items():
        df = fn(reader, errors, ddp_locale=ctx.validation.ddp_locale)
        if "Timestamp" not in df.columns:
            continue
        allowlisted_table = (
            set_dir.name in TIMESTAMP_ALLOWLIST_SETS and name == TIMESTAMP_ALLOWLIST_TABLE
        )
        for i, value in df["Timestamp"].items():
            if not isinstance(value, str) or not value:
                continue
            try:
                datetime.fromisoformat(value)
            except ValueError:
                if allowlisted_table and "Title" in df.columns and df.loc[i, "Title"] == value:
                    continue
                offenders.append((name, value))

    assert not offenders, (
        f"{set_dir.name}: unparseable non-empty Timestamp cell(s) "
        f"(table, value) — first 10: {offenders[:10]}"
    )


# ---------------------------------------------------------------------------
# Unit tests for the fixture-set helpers — synthetic, always run (ADR-0014:
# these never touch real data, so they run in CI with an empty tests/ddp/).
# ---------------------------------------------------------------------------


def _write_zip(path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)


class TestFindFixtureSets:
    def test_empty_when_ddp_dir_absent(self, tmp_path, monkeypatch):
        monkeypatch.setattr(eih, "DDP_DIR", tmp_path / "does-not-exist")
        assert eih.find_fixture_sets("google") == []

    def test_empty_when_no_matching_dirs(self, tmp_path, monkeypatch):
        monkeypatch.setattr(eih, "DDP_DIR", tmp_path)
        (tmp_path / "google_single.zip").write_bytes(b"")
        assert eih.find_fixture_sets("google") == []

    def test_ignores_a_file_that_matches_by_name(self, tmp_path, monkeypatch):
        monkeypatch.setattr(eih, "DDP_DIR", tmp_path)
        (tmp_path / "google_set_looks_like_a_dir").write_bytes(b"actually a file")
        assert eih.find_fixture_sets("google") == []

    def test_finds_platform_set_dirs_sorted_and_scoped_to_platform(self, tmp_path, monkeypatch):
        monkeypatch.setattr(eih, "DDP_DIR", tmp_path)
        for name in ["google_set_zzz", "google_set_aaa", "chatgpt_set_x"]:
            (tmp_path / name).mkdir()
        result = eih.find_fixture_sets("google")
        assert [p.name for p in result] == ["google_set_aaa", "google_set_zzz"]


class TestDiskPart:
    def test_exposes_name_size_and_reads_bytes(self, tmp_path):
        p = tmp_path / "part-001.zip"
        p.write_bytes(b"hello world")
        part = eih.DiskPart(p)
        assert part.name == "part-001.zip"
        assert part.size == len(b"hello world")
        assert part.read() == b"hello world"

    def test_seek_and_tell(self, tmp_path):
        p = tmp_path / "part.zip"
        p.write_bytes(b"0123456789")
        part = eih.DiskPart(p)
        part.seek(3)
        assert part.tell() == 3
        assert part.read(2) == b"34"

    def test_seekable_true_for_a_disk_file(self, tmp_path):
        # Regression coverage: zipfile.ZipFile calls .seekable() on the file
        # object it is handed before it will treat it as randomly seekable —
        # a DiskPart without this raised AttributeError, silently swallowed
        # by the platform's own exception handling, when this suite's first
        # draft was run against real fixtures.
        p = tmp_path / "part.zip"
        p.write_bytes(b"data")
        part = eih.DiskPart(p)
        assert part.seekable() is True

    def test_usable_as_a_zipfile_source(self, tmp_path):
        p = tmp_path / "part.zip"
        _write_zip(p, {"a.txt": "hi"})
        part = eih.DiskPart(p)
        with zipfile.ZipFile(part) as zf:
            assert zf.namelist() == ["a.txt"]
            assert zf.read("a.txt") == b"hi"


class TestOpenFixtureSet:
    def test_builds_archive_set_over_all_zip_parts(self, tmp_path):
        set_dir = tmp_path / "google_set_synthetic"
        set_dir.mkdir()
        _write_zip(set_dir / "part-001.zip", {"a.json": "{}"})
        _write_zip(set_dir / "part-002.zip", {"b.json": "{}"})
        archive = eih.open_fixture_set(set_dir)
        assert isinstance(archive, ArchiveSet)
        assert archive.members == ["a.json", "b.json"]

    def test_ignores_non_zip_files_in_set_dir(self, tmp_path):
        set_dir = tmp_path / "google_set_synthetic"
        set_dir.mkdir()
        _write_zip(set_dir / "part-001.zip", {"a.json": "{}"})
        (set_dir / "README.md").write_text("not a zip")
        archive = eih.open_fixture_set(set_dir)
        assert archive.members == ["a.json"]

    def test_counts_a_duplicate_member_across_parts(self, tmp_path):
        # Synthetic stand-in for the browser-renamed-duplicate-download case
        # (gmail-acct / en_vid): two parts, one member path in both.
        set_dir = tmp_path / "google_set_synthetic_dup"
        set_dir.mkdir()
        _write_zip(set_dir / "part-001.zip", {"a.json": "{}"})
        _write_zip(set_dir / "part-001 (1).zip", {"a.json": "{}"})
        archive = eih.open_fixture_set(set_dir)
        assert archive.duplicates["DuplicateMemberAcrossParts"] == 1
