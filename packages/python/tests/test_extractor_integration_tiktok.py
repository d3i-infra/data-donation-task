"""Integration tests for TikTok extractor functions.

Requires a real TikTok DDP zip at::

    tests/ddp/tiktok_<anything>.zip

Tests skip when no fixture is found — CI runs clean without real data.
"""
import pytest

from extractor_integration_helpers import ExtractorSpec, find_fixture, make_reader
from port.platforms.tiktok import DDP_CATEGORIES, activity_summary_to_df, settings_to_df, watch_history_to_df, favorite_videos_to_df, follower_to_df, following_to_df, hashtag_to_df, like_list_to_df, searches_to_df, share_history_to_df, comments_to_df
from port.helpers.validate import ValidateInput, validate_zip
from pathlib import Path

SPECS = [
    ExtractorSpec(name="activity_summary_to_df", extractor=activity_summary_to_df),
    ExtractorSpec(name="settings_to_df", extractor=settings_to_df),
    ExtractorSpec(name="watch_history_to_df", extractor=watch_history_to_df),
    ExtractorSpec(name="favorite_videos_to_df", extractor=favorite_videos_to_df),
    ExtractorSpec(name="follower_to_df", extractor=follower_to_df),
    ExtractorSpec(name="following_to_df", extractor=following_to_df),
    ExtractorSpec(name="hashtag_to_df", extractor=hashtag_to_df),    
    ExtractorSpec(name="like_list_to_df", extractor=like_list_to_df),
    ExtractorSpec(name="searches_to_df", extractor=searches_to_df),
    ExtractorSpec(name="share_history_to_df", extractor=share_history_to_df),
    ExtractorSpec(name="comments_to_df", extractor=comments_to_df),
]

@pytest.fixture(scope="module")
def tiktok_reader():
    fixture = find_fixture("tiktok")
    if fixture is None:
        pytest.skip("No tiktok_*.zip fixture found in tests/ddp/")
    validation = validate(fixture, DDP_CATEGORIES)
    for spec in SPECS: #adds the validation object as an argument for the extractor functions
        spec.kwargs = {'validation': validation}
    return make_reader(fixture, DDP_CATEGORIES)

def validate(fixture: Path, ddp_categories: list) -> ValidateInput:
    """Validate *fixture* and return a ``ValidateInput`` to pass on to extractors."""
    validation = validate_zip(ddp_categories, str(fixture))
    return validation

@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.name)
def test_extractor_not_empty(spec, tiktok_reader):
    df = spec.run(tiktok_reader)
    assert not df.empty, (
        f"{spec.name} returned an empty DataFrame — the extractor may have "
        "crashed, found no matching file, or the DDP format changed."
    )
