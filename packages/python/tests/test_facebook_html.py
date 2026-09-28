import io
import zipfile
from collections import Counter
from types import SimpleNamespace

import pytest
from lxml import etree

from port.helpers.extraction_helpers import ZipArchiveReader
from port.helpers.validate import DDPFiletype, validate_zip
from port.platforms import facebook


def _zip(members: dict[str, str]) -> io.BytesIO:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, body in members.items():
            z.writestr(name, body)
    buf.seek(0)
    return buf


def _reader(*entries: tuple[str, str], errors: Counter | None = None) -> ZipArchiveReader:
    """In-memory archive reader for one call. Pass the same ``errors`` Counter
    you hand the extractor: ``extraction()`` shares one counter between reader
    and extractors, so reader-side counts (ambiguous lookups) are only visible
    to a test that does the same."""
    return ZipArchiveReader(
        _zip(dict(entries)), [name for name, _ in entries], errors if errors is not None else Counter()
    )


#: A ``validation`` stand-in that selects the HTML path, as real validation
#: results do via ``current_ddp_category.ddp_filetype``.
_HTML_VALIDATION = SimpleNamespace(current_ddp_category=SimpleNamespace(ddp_filetype=DDPFiletype.HTML))


_HTML_STUB = {  # enough known names to pass the validator's matching rule
    "export/your_facebook_activity/comments_and_reactions/comments.html": "<html/>",
    "export/logged_information/search/your_search_history.html": "<html/>",
    "export/connections/followers/who_you_ve_followed.html": "<html/>",
    "export/ads_information/ad_preferences.html": "<html/>",
}


def test_html_export_is_recognised():
    v = validate_zip(facebook.DDP_CATEGORIES, _zip(_HTML_STUB))
    assert v.get_status_code_id() == 0
    assert v.current_ddp_category.id == "html_en"
    assert v.current_ddp_category.ddp_filetype is DDPFiletype.HTML


def test_json_export_is_still_recognised():
    members = {f"export/{n}": "{}" for n in facebook.DDP_CATEGORIES[0].known_files[:6]}
    v = validate_zip(facebook.DDP_CATEGORIES, _zip(members))
    assert v.current_ddp_category.id == "json_en"


def test_html_known_files_use_the_export_s_underscore_spelling():
    html = next(c for c in facebook.DDP_CATEGORIES if c.id == "html_en")
    assert not [n for n in html.known_files if "'" in n], "apostrophes never appear in HTML member names"
    assert "who_you_ve_followed.html" in html.known_files


# ---------------------------------------------------------------------------
# Helpers and dispatch (Task 3, step 1)
# ---------------------------------------------------------------------------


def test_section_clock_returns_the_rendered_text_verbatim():
    section = etree.HTML('<div><footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer></div>')
    assert facebook._section_clock(section) == "Nov 08, 2005 4:19:54 am"


def test_section_clock_is_empty_when_the_footer_has_no_clock():
    assert facebook._section_clock(etree.HTML("<div><footer></footer></div>")) == ""


def test_validation_reaches_only_html_capable_extractors():
    for name, fn in facebook.EXTRACTOR_REGISTRY.items():
        takes = fn in facebook._TAKES_VALIDATION
        assert takes == ("validation" in fn.__code__.co_varnames[: fn.__code__.co_argcount]), name


# ---------------------------------------------------------------------------
# Group A: comments, reactions, posts
# ---------------------------------------------------------------------------

_COMMENTS_PAGE = """<html><body><main>
<section class="_a6-g"><h2>A. Example commented on a post.</h2>
<div class="_2pin"><div>Nice one</div></div>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer>
</section>
</main></body></html>"""

_COMMENTS_PAGE_NO_CLOCK = """<html><body><main>
<section class="_a6-g"><h2>A. Example commented on a post.</h2>
<div class="_2pin"><div>Nice one</div></div>
<footer></footer>
</section>
</main></body></html>"""


def test_comments_html_keeps_the_clock_text_and_the_comment():
    reader = _reader(("export/your_facebook_activity/comments_and_reactions/comments.html", _COMMENTS_PAGE))
    errors: Counter = Counter()
    df = facebook.comments_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Timestamp"] == "Nov 08, 2005 4:19:54 am"
    assert df.iloc[0]["Comment"] == "Nice one"


def test_html_record_without_a_clock_yields_an_empty_cell():
    reader = _reader(("export/your_facebook_activity/comments_and_reactions/comments.html", _COMMENTS_PAGE_NO_CLOCK))
    errors: Counter = Counter()
    df = facebook.comments_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert df.iloc[0]["Timestamp"] == ""
    assert errors == Counter()


def test_html_twin_is_found_under_the_underscore_spelling():
    # A posts page under the JSON-style qualified name (no apostrophe in this
    # one) resolves through the same path-boundary suffix match; group B below
    # covers a name that does carry an apostrophe.
    page = """<html><body><main>
<section class="_a6-g"><h2>A. Example was at Somewhere.</h2>
<div class="_2pin"><div>Great trip</div></div>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer>
</section>
</main></body></html>"""
    reader = _reader(("export/your_facebook_activity/posts/your_posts__check_ins__photos_and_videos_1.html", page))
    errors: Counter = Counter()
    df = facebook.your_posts_check_ins_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Title"] == "A. Example was at Somewhere."
    assert df.iloc[0]["Content"] == "Great trip"
    assert df.iloc[0]["Timestamp"] == "Nov 08, 2005 4:19:54 am"


_LIKES_PAGE = """<html><body><main>
<section class="_a6-g"><h2>A liked post</h2>
<img src="https://static.xx.fbcdn.net/rsrc.php/icons/love.png"/>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer>
</section>
</main></body></html>"""


def test_likes_and_reactions_html_reads_title_reaction_and_clock():
    reader = _reader(
        ("export/your_facebook_activity/comments_and_reactions/likes_and_reactions_1.html", _LIKES_PAGE)
    )
    errors: Counter = Counter()
    df = facebook.likes_and_reactions_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Title"] == "A liked post"
    assert df.iloc[0]["Reaction"] == "Love"
    assert df.iloc[0]["Timestamp"] == "Nov 08, 2005 4:19:54 am"


def test_likes_and_reactions_base_html_reuses_the_same_twin_with_an_empty_url():
    reader = _reader(
        ("export/your_facebook_activity/comments_and_reactions/likes_and_reactions_1.html", _LIKES_PAGE)
    )
    errors: Counter = Counter()
    df = facebook.likes_and_reactions_base_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Name"] == "A liked post"
    assert df.iloc[0]["Reaction"] == "Love"
    assert df.iloc[0]["URL"] == ""
    assert df.iloc[0]["Timestamp"] == "Nov 08, 2005 4:19:54 am"


# ---------------------------------------------------------------------------
# Group B: follows, pages, groups, events
# ---------------------------------------------------------------------------

_FOLLOWED_PAGE = """<html><body><main>
<section class="_a6-g"><h2>Oldest Page</h2><footer><div class="_a72d">Jan 05, 2024 1:00:00 pm</div></footer></section>
<section class="_a6-g"><h2>Newest Page</h2><footer><div class="_a72d">Jun 04, 2025 6:46:10 pm</div></footer></section>
</main></body></html>"""


@pytest.mark.parametrize(
    "member",
    ["export/connections/followers/who_you've_followed.html",
     "export/connections/followers/who_you_ve_followed.html"],
)
def test_who_youve_followed_html_resolves_both_spellings_and_keeps_export_order(member):
    reader = _reader((member, _FOLLOWED_PAGE))
    errors: Counter = Counter()
    df = facebook.who_youve_followed_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert list(df["Name"]) == ["Oldest Page", "Newest Page"]  # export order kept, no sort
    assert list(df["Timestamp"]) == ["Jan 05, 2024 1:00:00 pm", "Jun 04, 2025 6:46:10 pm"]


def test_pages_and_profiles_you_follow_html():
    page = """<html><body><main>
<section class="_a6-g"><h2>A Page</h2><footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer></section>
</main></body></html>"""
    reader = _reader(("export/pages_and_profiles_you_follow.html", page))
    errors: Counter = Counter()
    df = facebook.pages_and_profiles_you_follow_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Title"] == "A Page"
    assert df.iloc[0]["Timestamp"] == "Nov 08, 2005 4:19:54 am"


def test_pages_youve_liked_html_reads_the_footer_link():
    page = """<html><body><main>
<section class="_a6-g"><h2>Liked Page</h2>
<footer><a href="https://www.facebook.com/liked-page"><div class="_a72d">Nov 08, 2005 4:19:54 am</div></a></footer>
</section>
</main></body></html>"""
    reader = _reader(("export/pages_you've_liked.html", page))
    errors: Counter = Counter()
    df = facebook.pages_youve_liked_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Name"] == "Liked Page"
    assert df.iloc[0]["URL"] == "https://www.facebook.com/liked-page"
    assert df.iloc[0]["Timestamp"] == "Nov 08, 2005 4:19:54 am"


def test_your_comments_in_groups_html_reads_group_and_comment():
    page = """<html><body><main>
<section class="_a6-g"><h2>A. Example commented on a post.</h2>
<div class="_2pin"><div class="_3-95"><span class="_a6_m">Group: </span>A group</div>A comment</div>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer>
</section>
</main></body></html>"""
    reader = _reader(("export/your_comments_in_groups.html", page))
    errors: Counter = Counter()
    df = facebook.your_comments_in_groups_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Group"] == "A group"
    assert df.iloc[0]["Comment"] == "A comment"
    assert df.iloc[0]["Timestamp"] == "Nov 08, 2005 4:19:54 am"


def test_your_group_membership_activity_html_keeps_the_full_sentence_as_title():
    page = """<html><body><main>
<section class="_a6-g"><h2>You became a member of A Group.</h2>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer>
</section>
</main></body></html>"""
    reader = _reader(("export/your_group_membership_activity.html", page))
    errors: Counter = Counter()
    df = facebook.your_group_membership_activity_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Title"] == "You became a member of A Group."
    # Fix round 1, item 1: no injected placeholder — an absent value is "" (ADR-0042).
    assert df.iloc[0]["Group name"] == ""
    assert df.iloc[0]["Timestamp"] == "Nov 08, 2005 4:19:54 am"


def test_your_events_html():
    page = """<html><body><main>
<section class="_a6-g"><h2>A Party</h2>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer>
</section>
</main></body></html>"""
    reader = _reader(("export/your_facebook_activity/events/your_events.html", page))
    errors: Counter = Counter()
    df = facebook.your_events_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Name"] == "A Party"
    assert df.iloc[0]["Created"] == "Nov 08, 2005 4:19:54 am"


# ---------------------------------------------------------------------------
# Group C: logged information and ads
# ---------------------------------------------------------------------------


def test_your_search_history_html_reads_the_qualified_path():
    page = """<html><body><main>
<section class="_a6-g"><div class="_2pin"><div>"search term"</div></div>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer></section>
</main></body></html>"""
    reader = _reader(("export/logged_information/search/your_search_history.html", page))
    errors: Counter = Counter()
    df = facebook.your_search_history_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Search term"] == "search term"
    assert df.iloc[0]["Date"] == "Nov 08, 2005 4:19:54 am"


def test_ads_interests_html():
    page = """<html><body><main>
<section class="_a6-g"><h2>Gardening</h2></section>
</main></body></html>"""
    reader = _reader(("export/ads_interests.html", page))
    errors: Counter = Counter()
    df = facebook.ads_interests_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert list(df["Ad"]) == ["Gardening"]


_CONTENT_SHOWN_GROUPED_PAGE = """<html><body><main>
<section class="_a6-g"><h2>Posts that have been shown to you in your Feed</h2>
<section class="_a6-g"><div class="_a6-p"><div>An older post</div></div>
<footer><a href="https://www.facebook.com/p/1"><div class="_a72d">Nov 08, 2005 4:19:54 am</div></a></footer></section>
</section>
</main></body></html>"""


def test_content_shown_html_grouped_layout():
    reader = _reader(
        ("export/logged_information/interactions/recently_viewed.html", _CONTENT_SHOWN_GROUPED_PAGE)
    )
    errors: Counter = Counter()
    df = facebook.content_shown_to_you_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Category"] == "Posts that have been shown to you in your Feed"
    assert df.iloc[0]["Name"] == "An older post"
    assert df.iloc[0]["Link"] == "https://www.facebook.com/p/1"
    assert df.iloc[0]["Date"] == "Nov 08, 2005 4:19:54 am"


_CONTENT_SHOWN_FEED_PAGE = """<html><body><main>
<table><tr><td class="_a6_q" colspan="2">Posts
<div><section class="_a6-g"><table>
<tr><td class="_a6_q">Event</td><td class="_a6_r">Someone shared</td></tr>
<tr><td class="_a6_q" colspan="2">URL<div><a href="https://www.facebook.com/p/2">https://www.facebook.com/p/2</a></div></td></tr>
<tr><td class="_a6_q">Time</td><td class="_a6_r">Nov 08, 2005 4:19:54 am</td></tr>
</table></section></div>
</td></tr></table>
</main></body></html>"""


def test_content_shown_html_feed_layout():
    reader = _reader((
        "export/logged_information/interactions/content_that_has_been_shown_to_you_in_your_feed.html",
        _CONTENT_SHOWN_FEED_PAGE,
    ))
    errors: Counter = Counter()
    df = facebook.content_shown_to_you_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Category"] == "Posts"
    assert df.iloc[0]["Name"] == "Someone shared"
    assert df.iloc[0]["Link"] == "https://www.facebook.com/p/2"
    assert df.iloc[0]["Date"] == "Nov 08, 2005 4:19:54 am"


_CONTENT_SHOWN_ADS_PAGE = """<html><body><main>
<section class="_a6-g"><table>
<tr><td class="_a6_q" colspan="2">Ad<div><a href="https://www.facebook.com/ads/1">An advertiser</a></div></td></tr>
<tr><td class="_a6_q">Time</td><td class="_a6_r">Nov 08, 2005 4:19:54 am</td></tr>
</table>
<footer><div class="_a72d"></div></footer>
</section>
</main></body></html>"""


def test_content_shown_html_ads_layout():
    reader = _reader(("export/logged_information/interactions/ads.html", _CONTENT_SHOWN_ADS_PAGE))
    errors: Counter = Counter()
    df = facebook.content_shown_to_you_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Category"] == "Ads"
    assert df.iloc[0]["Name"] == "An advertiser"
    assert df.iloc[0]["Link"] == "https://www.facebook.com/ads/1"
    assert df.iloc[0]["Date"] == "Nov 08, 2005 4:19:54 am"


_CONTENT_SHOWN_SHOWS_PAGE = """<html><body><main>
<section class="_a6-g"><table>
<tr><td class="_a6_q" colspan="2">Title<div><a href="https://www.facebook.com/s/1">A Show</a></div></td></tr>
</table>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer>
</section>
</main></body></html>"""


def test_content_shown_html_shows_layout_falls_back_to_the_footer_clock():
    reader = _reader((
        "export/logged_information/interactions/shows_you_have_watched.html", _CONTENT_SHOWN_SHOWS_PAGE
    ))
    errors: Counter = Counter()
    df = facebook.content_shown_to_you_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Category"] == "Videos you have watched"
    assert df.iloc[0]["Name"] == "A Show"
    assert df.iloc[0]["Link"] == "https://www.facebook.com/s/1"
    assert df.iloc[0]["Date"] == "Nov 08, 2005 4:19:54 am"


def test_profile_visits_html_split_layout():
    page = """<html><body><main>
<section class="_a6-g"><table>
<tr><td class="_a6_q">Name</td><td class="_a6_r">A. Example</td></tr>
</table>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer>
</section>
</main></body></html>"""
    reader = _reader(("export/profile_visits.html", page))
    errors: Counter = Counter()
    df = facebook.profile_visits_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Category"] == "Profile visits"
    assert df.iloc[0]["Name"] == "A. Example"
    assert df.iloc[0]["Timestamp"] == "Nov 08, 2005 4:19:54 am"


def test_profile_visits_html_groups_and_events_marks_events():
    page = """<html><body><main>
<section class="_a6-g"><table>
<tr><td class="_a6_q">Name</td><td class="_a6_r">A Party</td></tr>
<tr><td class="_a6_q">Start time</td><td class="_a6_r">Nov 08, 2005 4:19:54 am</td></tr>
</table>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer>
</section>
</main></body></html>"""
    reader = _reader(("export/logged_information/interactions/groups_and_events_you've_visited.html", page))
    errors: Counter = Counter()
    df = facebook.profile_visits_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Category"] == "Events visited"
    assert df.iloc[0]["Name"] == "A Party"


def test_profile_visits_html_grouped_layout_when_split_files_absent():
    page = """<html><body><main>
<section class="_a6-g"><h2>Page visits</h2>
<section class="_a6-g"><div class="_a6-p"><div>A Page</div></div>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer></section>
</section>
</main></body></html>"""
    reader = _reader(("export/logged_information/interactions/recently_visited.html", page))
    errors: Counter = Counter()
    df = facebook.profile_visits_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Category"] == "Page visits"
    assert df.iloc[0]["Name"] == "A Page"
    assert df.iloc[0]["Timestamp"] == "Nov 08, 2005 4:19:54 am"


def test_advertisers_youve_interacted_with_html():
    page = """<html><body><main>
<section class="_a6-g"><table>
<tr><td class="_a6_q">Action</td><td class="_a6_r">Click</td></tr>
<tr><td class="_a6_q">Title</td><td class="_a6_r">An Ad</td></tr>
<tr><td class="_a6_q" colspan="2"><a href="https://www.facebook.com/ads/9">https://www.facebook.com/ads/9</a></td></tr>
</table>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer>
</section>
</main></body></html>"""
    reader = _reader(("export/ads_information/advertisers_you_ve_interacted_with.html", page))
    errors: Counter = Counter()
    df = facebook.advertisers_youve_interacted_with_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Action"] == "Click"
    assert df.iloc[0]["Title"] == "An Ad"
    assert df.iloc[0]["URL"] == "https://www.facebook.com/ads/9"
    assert df.iloc[0]["Timestamp"] == "Nov 08, 2005 4:19:54 am"


_OFF_META_INDEX_PAGE = """<html><body><main>
<section class="_a6-g"><a href="apps_and_websites_off_of_facebook/your_activity_off_meta_technologies/business_1.html">A Business</a></section>
</main></body></html>"""

_OFF_META_BUSINESS_PAGE = """<html><body><main>
<h2>A Business</h2>
<table>
<tr><td class="_a6_q">ID</td><td class="_a6_r">123</td></tr>
<tr><td class="_a6_q">Event</td><td class="_a6_r">PAGE_VIEW</td></tr>
<tr><td class="_a6_q">Received on</td><td class="_a6_r">Nov 08, 2005 4:19:54 am</td></tr>
</table>
</main></body></html>"""


def test_activity_off_meta_html_reads_the_linked_business_page():
    reader = _reader(
        ("export/apps_and_websites_off_of_facebook/your_activity_off_meta_technologies.html", _OFF_META_INDEX_PAGE),
        (
            "export/apps_and_websites_off_of_facebook/your_activity_off_meta_technologies/business_1.html",
            _OFF_META_BUSINESS_PAGE,
        ),
    )
    errors: Counter = Counter()
    df = facebook.activity_off_meta_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Business"] == "A Business"
    assert df.iloc[0]["Event"] == "PAGE_VIEW"
    assert df.iloc[0]["Date"] == "Nov 08, 2005 4:19:54 am"


def test_activity_off_meta_html_missing_linked_page_is_not_an_error():
    reader = _reader(
        ("export/apps_and_websites_off_of_facebook/your_activity_off_meta_technologies.html", _OFF_META_INDEX_PAGE)
    )
    errors: Counter = Counter()
    df = facebook.activity_off_meta_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert df.empty
    assert not errors


def test_link_history_html():
    page = """<html><body><main>
<section class="_a6-g"><table>
<tr><td class="_a6_q" colspan="2"><a href="https://example.org/article">https://example.org/article</a></td></tr>
<tr><td class="_a6_q">Title of website page you visited</td><td class="_a6_r">An Article</td></tr>
</table>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer>
</section>
</main></body></html>"""
    reader = _reader(("export/your_facebook_activity/other_activity/link_history.html", page))
    errors: Counter = Counter()
    df = facebook.link_history_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["URL"] == "https://example.org/article"
    assert df.iloc[0]["Title"] == "An Article"
    assert df.iloc[0]["Timestamp"] == "Nov 08, 2005 4:19:54 am"


# ---------------------------------------------------------------------------
# Fix round 1 (review findings)
# ---------------------------------------------------------------------------


def test_content_shown_html_grouped_layout_donates_a_row_without_a_clock():
    """Item 3: a leaf whose footer carries no clock (e.g. a Marketplace
    counter) must still be donated, with an empty date cell, rather than
    silently dropped (ADR-0031: the dataset never shrinks silently)."""
    page = """<html><body><main>
<section class="_a6-g"><h2>Marketplace Interactions</h2>
<section class="_a6-g"><div class="_a6-p"><div>Nov 14, 2023</div></div>
<footer><div class="_a72d"></div></footer></section>
</section>
</main></body></html>"""
    reader = _reader(("export/logged_information/interactions/recently_viewed.html", page))
    errors: Counter = Counter()
    df = facebook.content_shown_to_you_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert len(df) == 1
    assert df.iloc[0]["Category"] == "Marketplace Interactions"
    assert df.iloc[0]["Name"] == "Nov 14, 2023"
    assert df.iloc[0]["Date"] == ""


def test_profile_visits_html_grouped_layout_donates_a_row_without_a_clock():
    """Item 3, second twin: same rule for the profile-visits grouped page."""
    page = """<html><body><main>
<section class="_a6-g"><h2>Marketplace Visits</h2>
<section class="_a6-g"><div class="_a6-p"><div>Nov 14, 2023</div></div>
<footer><div class="_a72d"></div></footer></section>
</section>
</main></body></html>"""
    reader = _reader(("export/logged_information/interactions/recently_visited.html", page))
    errors: Counter = Counter()
    df = facebook.profile_visits_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert len(df) == 1
    assert df.iloc[0]["Category"] == "Marketplace Visits"
    assert df.iloc[0]["Name"] == "Nov 14, 2023"
    assert df.iloc[0]["Timestamp"] == ""


def test_comments_html_captures_text_after_br_and_inside_inline_links():
    """Item 4: the old `.text` capture lost everything after a <br> or inside
    an inline <a> (and its tail). The full node text must survive."""
    page = """<html><body><main>
<section class="_a6-g"><h2>A. Example commented on a post.</h2>
<div class="_2pin"><div>Nice <a href="https://example.org">link</a> here<br/>second line</div></div>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer>
</section>
</main></body></html>"""
    reader = _reader(("export/your_facebook_activity/comments_and_reactions/comments.html", page))
    errors: Counter = Counter()
    df = facebook.comments_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Comment"] == "Nice link here second line"


def test_your_posts_check_ins_html_captures_text_after_br_and_inside_inline_links():
    """Item 4, second twin: same rule for the posts/check-ins content div."""
    page = """<html><body><main>
<section class="_a6-g"><h2>A. Example was at Somewhere.</h2>
<div class="_2pin"><div>Great <a href="https://example.org">trip</a> today<br/>loved it</div></div>
<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer>
</section>
</main></body></html>"""
    reader = _reader(("export/your_facebook_activity/posts/your_posts__check_ins__photos_and_videos_1.html", page))
    errors: Counter = Counter()
    df = facebook.your_posts_check_ins_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Content"] == "Great trip today loved it"


def test_html_text_fields_are_not_latin1_repaired():
    """Item 5: HTML exports are UTF-8 already (declared via the real export's
    own `<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">`,
    reproduced here) — eh.fix_latin1_string exists to repair JSON's latin-1
    escaping and must not run on HTML text, or it would re-encode already
    correct UTF-8 text into mojibake."""
    page = (
        '<html><head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8" /></head>'
        '<body><main>'
        '<section class="_a6-g"><h2>Café \U0001F600</h2>'
        '<div class="_2pin"><div>Café \U0001F600</div></div>'
        '<footer><div class="_a72d">Nov 08, 2005 4:19:54 am</div></footer>'
        '</section>'
        '</main></body></html>'
    )
    reader = _reader(("export/your_facebook_activity/comments_and_reactions/comments.html", page))
    errors: Counter = Counter()
    df = facebook.comments_to_df(reader, errors, validation=_HTML_VALIDATION)
    assert not errors
    assert df.iloc[0]["Title"] == "Café \U0001F600"
    assert df.iloc[0]["Comment"] == "Café \U0001F600"
