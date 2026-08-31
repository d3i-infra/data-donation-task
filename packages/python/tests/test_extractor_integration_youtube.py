"""Integration tests for YouTube extractor functions.

Requires a real YouTube DDP zip at::

    tests/ddp/YouTube_<anything>.zip

Tests skip when no fixture is found — CI runs clean without real data.
"""
import pytest

from extractor_integration_helpers import ExtractorSpec, find_fixture, make_reader
from port.platforms.youtube import DDP_CATEGORIES, watch_history_to_df, search_history_to_df, subscriptions_to_df, comments_to_df
from port.helpers.validate import ValidateInput, validate_zip
from pathlib import Path

SPECS = [
    ExtractorSpec(name="watch_history_to_df", extractor=watch_history_to_df),
    ExtractorSpec(name="search_history_to_df", extractor=search_history_to_df),
    ExtractorSpec(name="subscriptions_to_df", extractor=subscriptions_to_df),
    ExtractorSpec(name="comments_to_df", extractor=comments_to_df),
]

@pytest.fixture(scope="module")
def youtube_reader():
    fixture = find_fixture("youtube")
    if fixture is None:
        pytest.skip("No YouTube_*.zip fixture found in tests/ddp/")
    validation = validate(fixture, DDP_CATEGORIES)
    for spec in SPECS: #adds the validation object as an argument for the watch and search history extractor functions
        if spec.extractor in [watch_history_to_df, search_history_to_df]:
            spec.kwargs = {'validation': validation}
    return make_reader(fixture, DDP_CATEGORIES)

def validate(fixture: Path, ddp_categories: list) -> ValidateInput:
    """Validate *fixture* and return a ``ValidateInput`` to pass on to extractors."""
    validation = validate_zip(ddp_categories, str(fixture))
    return validation

@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.name)
def test_extractor_not_empty(spec, youtube_reader):
    df = spec.run(youtube_reader)
    assert not df.empty, (
        f"{spec.name} returned an empty DataFrame — the extractor may have "
        "crashed, found no matching file, or the DDP format changed."
    )