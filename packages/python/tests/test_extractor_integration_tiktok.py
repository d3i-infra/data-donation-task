"""Integration canaries for the TikTok extractors (ADR-0027).

Requires a real TikTok DDP zip at::

    tests/ddp/tiktok_<anything>.zip

Tests skip when no fixture is found; CI runs clean without real data (ADR-0014).

Expectation map
---------------
Which sections hold data depends on the account: the only local export writes the
sentinel "You have no data in this section" for several of them, and since the TXT
parser reads that sentinel as absence, those extractors legitimately return an empty
frame. So instead of a flat "every extractor is non-empty", ``EXPECT_NON_EMPTY`` pins,
per fixture stem, the extractors that must return rows; a pinned extractor gone empty is
real drift. Every registered extractor is still run against the fixture and must not
raise. ``UNPINNED_KNOWN_GAPS`` names the extractors no local fixture can exercise, with
the reason, and ``test_every_registry_entry_is_pinned_or_a_known_gap`` fails without any
fixture if a registry entry is neither. A fixture whose stem has no entry in
``EXPECT_NON_EMPTY`` fails every canary loudly until someone pins it.
"""
from collections import Counter
from pathlib import Path

import pytest

from extractor_integration_helpers import ExtractorSpec, find_fixture, make_reader
from port.helpers.validate import validate_zip
from port.platforms.tiktok import DDP_CATEGORIES, EXTRACTOR_REGISTRY

SPECS = [ExtractorSpec(name=name, extractor=fn) for name, fn in EXTRACTOR_REGISTRY.items()]

#: Per fixture stem (the zip's name without ``.zip``): the extractors that must return rows.
EXPECT_NON_EMPTY: dict[str, set[str]] = {
    "tiktok_txt_2026-08-27": {
        "activity_summary_to_df",
        "settings_to_df",
        "watch_history_to_df",
        "following_to_df",
        "searches_to_df",
        "share_history_to_df",
    },
}

#: Extractors no local fixture can exercise, and why. Kept out of every pin on purpose.
UNPINNED_KNOWN_GAPS: dict[str, str] = {
    "favorite_videos_to_df": "sentinel-only section in the only local export",
    "follower_to_df": "sentinel-only section in the only local export",
    "hashtag_to_df": "sentinel-only section in the only local export",
    "like_list_to_df": "sentinel-only section in the only local export",
    "comments_to_df": "sentinel-only section in the only local export",
    "off_tiktok_to_df": "Off-TikTok Activities.txt is absent from the only local export",
}


def _pins_for(stem: str) -> set[str]:
    """The pins for a fixture stem; a stem nobody has pinned is a decision not yet made,
    and a canary that silently asserts nothing is worse than none (ADR-0027)."""
    if stem not in EXPECT_NON_EMPTY:
        pytest.fail(
            f"no pins for fixture {stem!r}: add it to EXPECT_NON_EMPTY (and to "
            "UNPINNED_KNOWN_GAPS for the extractors it cannot exercise) before relying on it"
        )
    return EXPECT_NON_EMPTY[stem]


@pytest.fixture(scope="module")
def tiktok_fixture() -> Path:
    fixture = find_fixture("tiktok")
    if fixture is None:
        pytest.skip("No tiktok_*.zip fixture found in tests/ddp/")
    return fixture


@pytest.fixture(scope="module")
def tiktok_reader(tiktok_fixture):
    validation = validate_zip(DDP_CATEGORIES, str(tiktok_fixture))
    for spec in SPECS:
        spec.kwargs = {"validation": validation}
    return make_reader(tiktok_fixture, DDP_CATEGORIES)


def test_every_registry_entry_is_pinned_or_a_known_gap():
    """Fixture-free tripwire: a new extractor cannot be added without a canary decision."""
    pinned = set().union(*EXPECT_NON_EMPTY.values())
    assert pinned.isdisjoint(UNPINNED_KNOWN_GAPS), "an extractor is both pinned and a known gap"
    assert pinned | set(UNPINNED_KNOWN_GAPS) == set(EXTRACTOR_REGISTRY)


def test_an_unpinned_fixture_stem_fails_loudly():
    """Fixture-free tripwire: a missing pin is a loud failure, never a silent pass."""
    with pytest.raises(pytest.fail.Exception, match="no pins for fixture"):
        _pins_for("tiktok_some_other_export")


@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.name)
def test_extractor_runs_and_pins_hold(spec, tiktok_fixture, tiktok_reader):
    errors: Counter = Counter()
    df = spec.extractor(tiktok_reader, errors, **spec.kwargs)
    pins = _pins_for(tiktok_fixture.stem)
    if spec.name in pins:
        assert not df.empty, (
            f"{spec.name} returned an empty DataFrame on {tiktok_fixture.stem}; "
            "re-run it with an errors Counter to diagnose"
        )
