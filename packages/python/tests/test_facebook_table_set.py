"""Task 2: the JSON side settles on the final nineteen-table Facebook set.

Six extractors are ported from the algosoc fork (JSON branch only, replacing
three of ours) and eight of ours are removed outright. This pins the
registry to exactly that set, built the same way ``pnpm generate-config``
and ``tests/test_date_columns_declared.py`` do: straight from the
``Table config::`` docstrings (AST-based), never from the gitignored
generated ``port/configs/facebook_config.json``.
"""
import json

import pandas as pd

from test_date_columns_declared import _load_config  # noqa: E402 (see that module's sys.path setup)

import port.api.props as props
from port.api.d3i_props import PropsUIPromptConsentFormTableViz
from port.platforms import facebook

#: The nineteen table ids the JSON side settles on (story facebook, task 2).
KEEP = {
    "facebook_comments", "facebook_likes_and_reactions", "facebook_likes_and_reactions_base",
    "facebook_your_posts_and_check_ins", "facebook_search_history", "facebook_pages_and_profiles_you_follow",
    "facebook_pages_youve_liked", "facebook_who_youve_followed", "facebook_group_posts_and_comments",
    "facebook_your_comments_in_groups", "facebook_your_group_membership_activity", "facebook_content_shown_to_you",
    "facebook_profile_visits", "facebook_your_events", "facebook_ads_interests",
    "facebook_advertisers_youve_interacted_with", "facebook_activity_off_meta", "facebook_notifications",
    "facebook_link_history",
}


def test_registry_is_exactly_the_keep_list():
    config = _load_config(facebook, "facebook")
    ids = {table.id for table in config}
    assert ids == KEEP
    assert set(facebook.EXTRACTOR_REGISTRY) == {table.extractor.__name__ for table in config}


def test_sort_by_numeric_column_survives_serialization():
    """Fix round 1: ``_sort_by_numeric_column`` must reset the index after
    sorting. ``PropsUIPromptConsentFormTable`` only resets the index when
    truncating past the 10,000-row cap, and ``to_json()`` keys each column by
    index *label*, not position; ``parse_table.ts`` then looks rows up by
    literal label "0", "1", "2", ... — so a sorted frame with stale labels
    renders and donates in original order, silently undoing the sort. This
    drives the helper through the real serialization path, not just checking
    row order in memory. Also covers an empty cell (must sort last and stay
    ``""``, never dropped or coerced) and epoch ``0`` (a real, valid
    timestamp — must be kept and sorted as ``0``, not treated as missing).
    """
    df = pd.DataFrame({"Date": [100, 300, 200, "", 0]})

    sorted_df = facebook._sort_by_numeric_column(df, "Date")

    viz = PropsUIPromptConsentFormTableViz(
        id="t",
        title=props.Translatable({"en": "t", "nl": "t"}),
        data_frame=sorted_df,
    )
    parsed = json.loads(viz.toDict()["data_frame"])
    ordered_dates = [parsed["Date"][str(i)] for i in range(5)]

    assert ordered_dates == [300, 200, 100, 0, ""]
