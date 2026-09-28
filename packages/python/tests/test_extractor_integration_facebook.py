"""Integration canaries for the Facebook platform (JSON + HTML, 19 tables).

Requires local fixture zips at ``tests/ddp/``, real exports named per
``extractor_integration_helpers``'s convention (ADR-0014: real DDPs are never
committed; this whole module collects and skips cleanly against an empty
``tests/ddp/``):

    ln -s /path/to/export.zip packages/python/tests/ddp/facebook_json_2026-09.zip
    ln -s /path/to/export.zip packages/python/tests/ddp/facebook_html_2026-09.zip

Both are the same Facebook account (Danielle's own, 2026-09), one JSON export
and one HTML export. ``find_fixture("facebook_json")`` /
``find_fixture("facebook_html")`` glob ``facebook_json_*.zip`` /
``facebook_html_*.zip`` respectively, so either symlink name's date suffix is
free to change.

``SOURCE_FILES`` pins, per extractor, the exact member name(s) or pattern(s)
each format's twin reads (mirroring the literals in ``facebook.py`` itself) —
independent of what the extractor actually returns, so
``test_every_kept_table_with_a_source_file_is_non_empty`` has a real signal
rather than checking the extraction pipeline against itself. A table missing
from this map for a format it should have would trip
``test_every_extractor_has_a_source_file_pin`` (static, no fixtures needed).

``test_json_output_matches_the_recorded_digest`` compares against
``facebook_json_2026-09.digest.json`` (also gitignored), recorded from the
pre-Task-2 tree (Task 2 removed 11 tables and ported 6; only the 11 tables
that survived Task 2 unchanged are in both the digest and the current
registry, and are compared here). Two further tables are deliberately never
compared even though this loop would otherwise reach them if they appeared in
the digest:

- ``facebook_who_youve_followed`` and ``facebook_pages_youve_liked`` returned
  0 rows at baseline (a one-directional apostrophe/underscore reader bug,
  fixed in Task 3) and so are simply absent from the digest — comparing them
  would be comparing against a baseline that predates the fix, not a
  regression.
- A table whose columns changed since the baseline is compared as a
  row-count/hash regression only when it is explicitly named, with its
  reason, in ``_COLUMNS_CHANGED_SINCE_BASELINE`` — a row-count/hash match is
  not meaningful across different columns, so a *documented* change is
  reported separately rather than compared. ``_COLUMNS_CHANGED_SINCE_BASELINE``
  is empty today (none of the 11 differ); an *undocumented* column mismatch
  is a real digest offender, not a silent skip (fix round 1: the previous
  version skipped any column mismatch unconditionally, which would have let
  a real regression pass unnoticed).

See Also
--------
docs/decisions/0014-no-real-participant-data-in-version-control.md
docs/decisions/0043-*.md : the meta-html clock encoding this module pins
"""
import hashlib
import io
import json
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pytest

from extractor_integration_helpers import find_fixture, make_reader
from test_date_columns_declared import _load_config  # noqa: E402 (see that module's sys.path setup)
import port.helpers.validate as validate
from port.helpers.extraction_helpers import ZipArchiveReader
from port.platforms import facebook

FIXTURES: dict[str, Path | None] = {
    "json": find_fixture("facebook_json"),
    "html": find_fixture("facebook_html"),
}
_NO_FIXTURE_REASON = {
    fmt: (
        f"No tests/ddp/facebook_{fmt}_*.zip fixture found (ADR-0014: real exports "
        "are never committed — see this module's docstring for the local symlink step)"
    )
    for fmt in ("json", "html")
}

_FN_NAME = {fn: name for name, fn in facebook.EXTRACTOR_REGISTRY.items()}

_DIGEST_PATH = Path(__file__).parent / "ddp" / "facebook_json_2026-09.digest.json"


def _config():
    """Build the ``TableConfig`` list straight from the ``Table config::``
    docstrings (AST-based, ``test_facebook_table_set.py``'s own pattern) —
    never ``load_port_config``, which reads the generated, gitignored
    ``port/configs/facebook_config.json``. That file only exists locally once
    ``pnpm generate-config facebook`` has run; reading it here would raise
    ``FileNotFoundError`` at collection on a clean checkout — the same
    environment where the ``tests/ddp/`` symlinks are also absent — breaking
    ADR-0027's "collects and skips cleanly" contract harder than a missing
    fixture would (fix round 1, task-4-review.md Important finding 1).
    Called inside each test, never at import time, and not cached: the AST
    parse is cheap and this keeps every test independent of import order."""
    return _load_config(facebook, "facebook")


# ---------------------------------------------------------------------------
# Per-table source-file pins (module docstring explains the "why")
# ---------------------------------------------------------------------------

#: Per extractor, the literal member name(s) (checked via
#: ``ZipArchiveReader.resolve_member``, which already tries the
#: apostrophe/underscore substitutions real exports mix) and/or compiled
#: regex pattern(s) (checked against ``archive_members`` directly, for
#: numbered/paginated files) that format's twin reads. ``None`` means that
#: format has no twin for this table (``notifications``,
#: ``group_posts_and_comments`` stay JSON-only).
SOURCE_FILES: dict[Callable, dict[str, list | None]] = {
    facebook.who_youve_followed_to_df: {
        "json": ["who_you_ve_followed.json"],
        "html": ["who_you've_followed.html"],
    },
    facebook.notifications_to_df: {
        "json": ["notifications/notifications.json"],
        "html": None,
    },
    facebook.your_search_history_to_df: {
        "json": ["logged_information/search/your_search_history.json"],
        "html": ["logged_information/search/your_search_history.html"],
    },
    facebook.ads_interests_to_df: {
        "json": ["ads_interests.json"],
        "html": ["ads_interests.html"],
    },
    facebook.content_shown_to_you_to_df: {
        "json": [
            "logged_information/interactions/recently_viewed.json",
            "logged_information/interactions/content_that_has_been_shown_to_you_in_your_feed.json",
            "logged_information/interactions/ads.json",
            "logged_information/interactions/shows_you_have_watched.json",
        ],
        "html": [
            "logged_information/interactions/recently_viewed.html",
            "logged_information/interactions/content_that_has_been_shown_to_you_in_your_feed.html",
            "logged_information/interactions/ads.html",
            "logged_information/interactions/shows_you_have_watched.html",
        ],
    },
    facebook.profile_visits_to_df: {
        "json": [
            "logged_information/interactions/profile_visits.json",
            "logged_information/interactions/groups_and_events_you've_visited.json",
            "logged_information/interactions/recently_visited.json",
        ],
        "html": [
            "profile_visits.html",
            "logged_information/interactions/groups_and_events_you've_visited.html",
            "logged_information/interactions/recently_visited.html",
        ],
    },
    facebook.your_events_to_df: {
        "json": ["your_facebook_activity/events/your_events.json"],
        "html": ["your_facebook_activity/events/your_events.html"],
    },
    facebook.group_posts_and_comments_to_df: {
        "json": ["group_posts_and_comments.json"],
        "html": None,
    },
    facebook.your_comments_in_groups_to_df: {
        "json": ["your_comments_in_groups.json"],
        "html": ["your_comments_in_groups.html"],
    },
    facebook.your_group_membership_activity_to_df: {
        "json": ["your_group_membership_activity.json"],
        "html": ["your_group_membership_activity.html"],
    },
    facebook.pages_and_profiles_you_follow_to_df: {
        "json": ["pages_and_profiles_you_follow.json"],
        "html": ["pages_and_profiles_you_follow.html"],
    },
    facebook.pages_youve_liked_to_df: {
        "json": ["pages_you_ve_liked.json"],
        "html": ["pages_you've_liked.html"],
    },
    facebook.comments_to_df: {
        "json": ["comments_and_reactions/comments.json"],
        "html": ["your_facebook_activity/comments_and_reactions/comments.html"],
    },
    facebook.likes_and_reactions_to_df: {
        "json": [re.compile(r"(^|/)likes_and_reactions_\d+\.json$")],
        "html": [re.compile(r"(^|/)likes_and_reactions_\d+\.html$")],
    },
    facebook.your_posts_check_ins_to_df: {
        "json": ["your_posts__check_ins__photos_and_videos_1.json"],
        "html": [re.compile(r"(^|/)your_posts__check_ins__photos_and_videos_\d+\.html$")],
    },
    facebook.likes_and_reactions_base_to_df: {
        "json": ["likes_and_reactions.json", re.compile(r"(^|/)likes_and_reactions_\d+\.json$")],
        "html": [re.compile(r"(^|/)likes_and_reactions_\d+\.html$")],
    },
    facebook.activity_off_meta_to_df: {
        "json": ["apps_and_websites_off_of_facebook/your_activity_off_meta_technologies.json"],
        "html": ["apps_and_websites_off_of_facebook/your_activity_off_meta_technologies.html"],
    },
    facebook.advertisers_youve_interacted_with_to_df: {
        "json": ["ads_information/advertisers_you've_interacted_with.json"],
        "html": ["advertisers_you've_interacted_with.html"],
    },
    facebook.link_history_to_df: {
        "json": ["your_facebook_activity/other_activity/link_history.json"],
        "html": ["link_history.html"],
    },
}


def _present(reader: ZipArchiveReader, candidates: list) -> bool:
    """Whether any of *candidates* (literal member names or compiled regexes)
    resolves against *reader*'s archive."""
    for candidate in candidates:
        if isinstance(candidate, re.Pattern):
            if any(candidate.search(member) for member in reader.archive_members):
                return True
        elif reader.resolve_member(candidate) is not None:
            return True
    return False


def _run(table_or_fn, reader: ZipArchiveReader, validation) -> "tuple":
    """Run a ``TableConfig`` or a bare extractor fn, forwarding ``validation``
    only to HTML-capable extractors (``facebook._TAKES_VALIDATION``)."""
    fn = table_or_fn.extractor if hasattr(table_or_fn, "extractor") else table_or_fn
    errors: Counter = Counter()
    kwargs = {"validation": validation} if fn in facebook._TAKES_VALIDATION else {}
    return fn(reader, errors, **kwargs), errors


@dataclass
class _Ctx:
    reader: ZipArchiveReader
    validation: "validate.ValidateInput"


_CACHE: dict[str, _Ctx | None] = {}


def _ctx(fmt: str) -> _Ctx | None:
    if fmt not in _CACHE:
        fixture = FIXTURES[fmt]
        if fixture is None:
            _CACHE[fmt] = None
        else:
            reader = make_reader(fixture, facebook.DDP_CATEGORIES)
            validation = validate.validate_zip(facebook.DDP_CATEGORIES, str(fixture))
            _CACHE[fmt] = _Ctx(reader, validation)
    return _CACHE[fmt]


# ---------------------------------------------------------------------------
# Registry completeness — static, no fixtures required
# ---------------------------------------------------------------------------


def test_every_extractor_has_a_source_file_pin():
    """Every extractor in ``facebook.EXTRACTOR_REGISTRY`` must appear in
    ``SOURCE_FILES`` — otherwise a future table silently falls out of every
    canary below (the loops only ever look at what ``SOURCE_FILES`` pins)."""
    missing = set(facebook.EXTRACTOR_REGISTRY.values()) - set(SOURCE_FILES)
    assert not missing, (
        f"Extractor(s) {[_FN_NAME[fn] for fn in missing]} are not pinned in "
        "SOURCE_FILES — add an entry (or an explicit None per format) before "
        "this passes silently uncovered."
    )


# ---------------------------------------------------------------------------
# Recognition
# ---------------------------------------------------------------------------

#: The DDP category each fixture must resolve to, per format.
_EXPECTED_CATEGORY_ID = {"json": "json_en", "html": "html_en"}


@pytest.mark.parametrize("fmt", ["json", "html"])
def test_fixture_is_recognised_as_facebook(fmt):
    """Mirrors ``test_extractor_integration_google.py``'s ``test_set_is_recognized``:
    every other test in this file treats non-recognition as a skip (so a
    fixture the validator no longer recognizes silently disables the whole
    file's canaries instead of failing), so this is the one test that turns a
    recognition regression into a real failure."""
    fixture = FIXTURES[fmt]
    if fixture is None:
        pytest.skip(_NO_FIXTURE_REASON[fmt])
    ctx = _ctx(fmt)
    assert ctx.validation.get_status_code_id() == 0, (
        f"{fixture.name} was not recognized as a Facebook DDP"
    )
    assert ctx.validation.current_ddp_category.id == _EXPECTED_CATEGORY_ID[fmt], (
        f"{fixture.name} resolved to category {ctx.validation.current_ddp_category.id!r}, "
        f"expected {_EXPECTED_CATEGORY_ID[fmt]!r}"
    )


# ---------------------------------------------------------------------------
# Per-table non-emptiness, keyed by whether the source file is really there
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fmt", ["json", "html"])
def test_every_kept_table_with_a_source_file_is_non_empty(fmt):
    ctx = _ctx(fmt)
    if ctx is None:
        pytest.skip(_NO_FIXTURE_REASON[fmt])
    if ctx.validation.get_status_code_id() != 0:
        pytest.skip(f"{FIXTURES[fmt].name} was not recognized as a Facebook DDP")

    offenders = []
    for fn, spec in SOURCE_FILES.items():
        candidates = spec[fmt]
        if candidates is None or not _present(ctx.reader, candidates):
            continue
        df, _ = _run(fn, ctx.reader, ctx.validation)
        if df.empty:
            offenders.append(_FN_NAME[fn])

    assert not offenders, (
        f"{fmt}: table(s) whose source file is present in the fixture returned no rows "
        f"(extractor may have crashed, source shape changed, or the pin is stale): {offenders}"
    )


@pytest.mark.parametrize("fmt", ["json", "html"])
def test_declared_date_columns_are_populated(fmt):
    """Every ``date_columns`` key a table declares must be a real, populated
    column in that table's frame — checked only for tables whose source file
    is actually present in this fixture (the same scope the test above
    uses), so a table this export never touches is not a false positive."""
    ctx = _ctx(fmt)
    if ctx is None:
        pytest.skip(_NO_FIXTURE_REASON[fmt])
    if ctx.validation.get_status_code_id() != 0:
        pytest.skip(f"{FIXTURES[fmt].name} was not recognized as a Facebook DDP")

    offenders = []
    for table in _config():
        if not table.date_columns:
            continue
        candidates = SOURCE_FILES[table.extractor][fmt]
        if candidates is None or not _present(ctx.reader, candidates):
            continue
        df, _ = _run(table, ctx.reader, ctx.validation)
        for column in table.date_columns:
            if column not in df.columns:
                offenders.append((table.id, column, "column missing from frame"))
                continue
            if not any(value not in ("", None) for value in df[column]):
                offenders.append((table.id, column, "no non-empty cell"))

    assert not offenders, (
        f"{fmt}: declared date column(s) missing or empty (table, column, problem): {offenders}"
    )


# ---------------------------------------------------------------------------
# JSON regression digest (pre-Task-2 baseline; see module docstring)
# ---------------------------------------------------------------------------

#: Tables present in both the baseline digest and the current registry whose
#: columns are *known* to have changed since the baseline, with the reason —
#: the only way a column-set mismatch is not an offender. Empty today (fix
#: round 1 verified none of the 11 comparable tables differ); a future,
#: reviewed column change is documented here, not silently passed. An
#: undocumented column mismatch is a real offender (task-4-review.md
#: Important finding: the previous version `continue`d before recording one).
_COLUMNS_CHANGED_SINCE_BASELINE: dict[str, str] = {}


def test_json_output_matches_the_recorded_digest():
    fixture = FIXTURES["json"]
    if fixture is None:
        pytest.skip(_NO_FIXTURE_REASON["json"])
    if not _DIGEST_PATH.is_file():
        pytest.skip(
            f"No baseline digest at {_DIGEST_PATH} (gitignored, ADR-0014, recorded "
            "from the pre-Task-2 tree)"
        )

    ctx = _ctx("json")
    if ctx.validation.get_status_code_id() != 0:
        pytest.skip(f"{fixture.name} was not recognized as a Facebook DDP")

    digest = json.loads(_DIGEST_PATH.read_text())
    table_by_id = {table.id: table for table in _config()}

    offenders = []
    columns_changed = []
    for table_id, recorded in digest.items():
        table = table_by_id.get(table_id)
        if table is None:
            # Removed from the registry since the baseline was recorded
            # (Task 2), e.g. facebook_your_event_responses — not comparable.
            continue

        df, _ = _run(table, ctx.reader, ctx.validation)
        if list(df.columns) != recorded["columns"]:
            reason = _COLUMNS_CHANGED_SINCE_BASELINE.get(table_id)
            if reason is not None:
                columns_changed.append((table_id, reason, recorded["columns"], list(df.columns)))
                continue
            offenders.append((
                table_id, "columns (undocumented — add to _COLUMNS_CHANGED_SINCE_BASELINE if intentional)",
                recorded["columns"], list(df.columns),
            ))
            continue

        if df.shape[0] != recorded["rows"]:
            offenders.append((table_id, "rows", recorded["rows"], df.shape[0]))
            continue

        first_row_hash = hashlib.sha256(repr(tuple(df.iloc[0])).encode()).hexdigest()
        if first_row_hash != recorded["first_row_sha256"]:
            offenders.append((table_id, "first_row_sha256", recorded["first_row_sha256"], first_row_hash))

    assert not offenders, (
        f"digest mismatch(es) (table, field, expected, actual) — documented column changes "
        f"(skipped, not compared): {columns_changed}; mismatches: {offenders}"
    )


# ---------------------------------------------------------------------------
# HTML clock shape — every date cell donates the export's own rendered text
# ---------------------------------------------------------------------------

_CLOCK_SHAPE = re.compile(r"^[A-Z][a-z]{2} \d{2}, \d{4} \d{1,2}:\d{2}:\d{2} [ap]m$")


def test_html_clock_cells_match_the_meta_html_shape():
    """Runs the whole HTML export through ``facebook.extraction`` (ADR-0043:
    a meta-html date column donates the export's own rendered clock text
    verbatim) and checks every non-empty cell of a declared date column
    against the shape this export's clock actually uses."""
    fixture = FIXTURES["html"]
    if fixture is None:
        pytest.skip(_NO_FIXTURE_REASON["html"])
    ctx = _ctx("html")
    if ctx.validation.get_status_code_id() != 0:
        pytest.skip(f"{fixture.name} was not recognized as a Facebook DDP")

    with open(fixture, "rb") as fh:
        result = facebook.extraction(io.BytesIO(fh.read()), ctx.validation)

    date_columns_by_id = {table.id: table.date_columns for table in _config()}
    checked = 0
    offenders = []
    for table_viz in result.tables:
        for column in date_columns_by_id.get(table_viz.id, {}):
            if column not in table_viz.data_frame.columns:
                continue
            for value in table_viz.data_frame[column]:
                if value in ("", None):
                    continue
                checked += 1
                if not _CLOCK_SHAPE.match(str(value)):
                    offenders.append((table_viz.id, column, value))

    assert checked > 0, (
        "no non-empty declared date cell was checked — result.tables had "
        f"{len(result.tables)} table(s); the HTML extraction may have regressed "
        "to returning no tables or no populated date columns, which would make "
        "the shape assertion below pass vacuously"
    )
    assert not offenders, (
        f"HTML date cell(s) not matching the meta-html clock shape, out of {checked} checked "
        f"(table, column, value) — first 10: {offenders[:10]}"
    )
