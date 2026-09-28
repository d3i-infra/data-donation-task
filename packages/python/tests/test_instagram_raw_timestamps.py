"""Tests for Instagram's raw timestamps (ADR-0042/0043).

The json export donates the raw epoch number; the html export donates the display
clock text as read, unconverted. Both are sorted newest-first over the same raw
value (``_sort_by_date``), not over a derived instant.
"""

import io
import json
import zipfile
from collections import Counter

import pandas as pd

import port.api.props as props
from port.api.d3i_props import PropsUIPromptConsentFormTableViz
from port.helpers.extraction_helpers import ZipArchiveReader
from port.platforms import instagram


def _archive(*entries: tuple[str, str | bytes]) -> io.BytesIO:
    """Build an in-memory zip with the given (name, content) entries."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in entries:
            zf.writestr(name, content)
    buf.seek(0)
    return buf


def test_instagram_json_keeps_the_epoch_number():
    data = {
        "relationships_following": [
            {"string_list_data": [{"value": "x", "timestamp": 1781548241}]}
        ]
    }
    archive = _archive(("following.json", json.dumps(data)))
    errors = Counter()
    df = instagram.following_to_df(ZipArchiveReader(archive, ["following.json"], errors), errors)
    assert df["Date"].iloc[0] == 1781548241
    assert isinstance(list(df["Date"])[0], int)  # not a float64 upcast (final review, Minor 11)
    assert not errors


def test_instagram_html_keeps_the_display_text():
    html = """
    <html><body><main>
      <div class="_a6-g">
        <div>
          <h2>Owner</h2>
          <table><tr><td>Username</td><td>the_liker</td></tr></table>
        </div>
        <div class="_a6-o">Mar 02, 2026 4:57:45 pm</div>
      </div>
    </main></body></html>
    """
    archive = _archive(("story_likes.html", html))
    errors = Counter()

    class _Validation:
        class current_ddp_category:
            ddp_filetype = instagram.DDPFiletype.HTML

    df = instagram.story_likes_to_df(
        ZipArchiveReader(archive, ["story_likes.html"], errors),
        errors,
        validation=_Validation(),
    )
    assert df["Date"].iloc[0] == "Mar 02, 2026 4:57:45 pm"
    assert not errors


def test_instagram_sorts_epochs_numerically_newest_first():
    df = instagram._sort_by_date(pd.DataFrame({"Date": [1, 3, "", 2]}), "Date")
    assert list(df["Date"]) == [3, 2, 1, ""]


def test_instagram_sort_survives_serialization():
    """Fix round 1 (facebook review, same defect in ``_sort_by_date`` here):
    ``PropsUIPromptConsentFormTable`` only resets the index when truncating
    past the 10,000-row cap, and ``to_json()`` keys each column by index
    *label*, not position; ``parse_table.ts`` then looks rows up by literal
    label "0", "1", "2", ... A sorted frame with stale pre-sort labels would
    render and donate in original order, silently undoing the sort. Also
    covers an empty cell (must sort last and stay ``""``, never dropped or
    coerced) and epoch ``0`` (a real, valid timestamp — must be kept and
    sorted as ``0``, not treated as missing).
    """
    df = pd.DataFrame({"Date": [100, 300, 200, "", 0]})

    sorted_df = instagram._sort_by_date(df, "Date")

    viz = PropsUIPromptConsentFormTableViz(
        id="t",
        title=props.Translatable({"en": "t", "nl": "t"}),
        data_frame=sorted_df,
    )
    parsed = json.loads(viz.toDict()["data_frame"])
    ordered_dates = [parsed["Date"][str(i)] for i in range(5)]

    assert ordered_dates == [300, 200, 100, 0, ""]
