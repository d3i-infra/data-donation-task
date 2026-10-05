"""
Facebook

This module contains an example flow of a Facebook data donation study

Assumptions:
It handles DDPs in the english language with filetype JSON.

Configuration
-------------
The ``extraction`` function is driven by ``port_config.json``.  Generate one with::

    pnpm generate-config facebook

Each extractor function carries its own table config in a ``Table config::``
JSON block inside its docstring.  The generator reads those blocks and
assembles the JSON file.

Platform info::

    {
        "name": "Facebook",
        "filetypes": ["json"],
        "languages": ["en", "nl"],
        "description": "Handles DDPs in English. These data donation flows have not been tested yet, if you find anything wrong with them report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "not yet implemented"
    }
"""

import logging
import re
from collections import Counter
from typing import Callable

from lxml import etree

import pandas as pd

import port.helpers.extraction_helpers as eh
import port.helpers.validate as validate
from port.helpers.extraction_helpers import ZipArchiveReader
from port.helpers.flow_builder import FlowBuilder

from port.helpers.validate import (
    DDPCategory,
    DDPFiletype,
    Language,
)
from port.api.d3i_props import ExtractionResult
from port.api.file_utils import SeekableBinaryReader
from port.helpers.table_extractor import (
    load_port_config,
    run_extraction,
)

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
"subscription_for_no_ads.json", "other_categories_used_to_reach_you.json", "ads_feedback_activity.json", "ads_personalization_consent.json", "advertisers_you've_interacted_with.json", "advertisers_using_your_activity_or_information.json", "story_views_in_past_7_days.json", "ad_preferences.json", "groups_you've_searched_for.json", "your_search_history.json", "primary_public_location.json", "timezone.json", "primary_location.json", "your_privacy_jurisdiction.json", "people_and_friends.json", "ads_interests.json", "notifications.json", "notification_of_meta_privacy_policy_update.json", "recently_viewed.json", "recently_visited.json", "your_avatar.json", "meta_avatars_post_backgrounds.json", "contacts_sync_settings.json", "timezone.json", "autofill_information.json", "profile_information.json", "profile_update_history.json", "your_transaction_survey_information.json", "your_recently_followed_history.json", "your_recently_used_emojis.json", "navigation_bar_activity.json", "pages_and_profiles_you_follow.json", "pages_you've_liked.json", "your_saved_items.json", "fundraiser_posts_you_likely_viewed.json", "your_fundraiser_donations_information.json", "your_event_responses.json", "event_invitations.json", "your_event_invitation_links.json", "likes_and_reactions_1.json", "your_uncategorized_photos.json", "payment_history.json", "your_answers_to_membership_questions.json", "your_group_membership_activity.json", "your_contributions.json", "group_posts_and_comments.json", "your_comments_in_groups.json", "instant_games.json", "your_page_or_groups_badges.json", "instant_games_usage_data.json", "who_you've_followed.json", "people_you_may_know.json", "received_friend_requests.json", "your_friends.json", "likes_and_reactions.json", "controls.json",
        ],
    ),
    DDPCategory(
        # HTML exports write apostrophes as underscores in member names
        # ("who_you_ve_followed.html"); the validator matches names exactly.
        id="html_en",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.EN,
        known_files=[
"subscription_for_no_ads.html", "other_categories_used_to_reach_you.html", "ads_feedback_activity.html", "advertisers_you_ve_interacted_with.html", "advertisers_using_your_activity_or_information.html", "story_views_in_past_7_days.html", "ad_preferences.html", "your_search_history.html", "primary_public_location.html", "primary_location.html", "your_privacy_jurisdiction.html", "people_and_friends.html", "ads_interests.html", "notifications.html", "contacts_sync_settings.html", "autofill_information.html", "profile_information.html", "profile_update_history.html", "your_transaction_survey_information.html", "your_recently_used_emojis.html", "pages_and_profiles_you_follow.html", "pages_you_ve_liked.html", "your_saved_items.html", "fundraiser_posts_you_likely_viewed.html", "your_fundraiser_donations_information.html", "your_events.html", "event_invitations.html", "your_event_invitation_links.html", "likes_and_reactions_1.html", "payment_history.html", "your_group_membership_activity.html", "your_contributions.html", "your_page_or_groups_badges.html", "who_you_ve_followed.html", "people_you_may_know.html", "received_friend_requests.html", "your_friends.html", "likes_and_reactions.html", "comments.html", "your_posts__check_ins__photos_and_videos_1.html", "archived_stories.html", "connected_apps_and_websites.html", "your_activity_off_meta_technologies.html", "content_that_has_been_shown_to_you_in_your_feed.html", "items_viewed.html", "profile_visits.html", "start_here.html", "your_comments_in_groups.html"
        ],
    ),
]


def who_youve_followed_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract the list of profiles and pages you follow on Facebook.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Name``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook profile or page that the participant follows, including the name and the time they started following.",
          "source_file": "who_you_ve_followed.json / who_you_ve_followed.html",
          "columns": {
            "Name": "Name of the followed profile or page.",
            "Timestamp": "Time of the follow (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_who_youve_followed",
          "title": {
            "en": "Who you follow",
            "nl": "Wie je volgt"
          },
          "description": {
            "en": "This table shows the Facebook profiles and pages you currently follow.",
            "nl": "Deze tabel toont de Facebook-profielen en -pagina's die je momenteel volgt."
          },
          "headers": {
            "Name": {"en": "Name", "nl": "Naam"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          },
          "date_columns": {"Timestamp": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _who_youve_followed_html(reader, errors)

    result = reader.json("who_you_ve_followed.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["following_v3"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item.get("name", "")),
                item.get("timestamp", "")
            ))

        out = pd.DataFrame(datapoints, columns=["Name", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _who_youve_followed_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("who_you've_followed.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        sections = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and .//h2]")
        for section in sections:
            h2 = section.xpath(".//h2")
            name = h2[0].text.strip() if h2 and h2[0].text else ""
            timestamp = _section_clock(section)

            datapoints.append((name, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Name", "Timestamp"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def notifications_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract Facebook notifications history.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Text``, ``Link``, ``Read``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a notification the participant received from Facebook, including the text, link, read status, and timestamp.",
          "source_file": "notifications/notifications.json",
          "columns": {
            "Text": "Text content of the notification.",
            "Link": "URL the notification links to.",
            "Read": "Whether the notification was read.",
            "Date": "Notification time (Unix seconds)."
          }
        }

    Table config::

        {
          "id": "facebook_notifications",
          "title": {
            "en": "Notifications Facebook sent you",
            "nl": "Notificaties die Facebook je stuurde"
          },
          "description": {
            "en": "This table contains a history of the notifications you've received from Facebook.",
            "nl": "Deze tabel bevat een overzicht van de notificaties die je van Facebook hebt ontvangen."
          },
          "headers": {
            "Text": {"en": "Text", "nl": "Tekst"},
            "Link": {"en": "Link", "nl": "Link"},
            "Read": {"en": "Read", "nl": "Gelezen"},
            "Date": {"en": "Date", "nl": "Datum"}
          },
          "date_columns": {"Date": {"encoding": "epoch-seconds"}}
        }
    """
    result = reader.json("notifications/notifications.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["notifications_v2"]  # pyright: ignore
        for item in items:
            denested_dict = eh.dict_denester(item)
            datapoints.append((
                eh.find_item(denested_dict, "text"),
                eh.find_item(denested_dict, "href"),
                eh.find_item(denested_dict, "unread"),
                eh.raw_timestamp(denested_dict, "timestamp"),
            ))

        out = pd.DataFrame(datapoints, columns=["Text", "Link", "Read", "Date"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def your_search_history_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract Facebook search history.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Search term``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a search query the participant made on Facebook, including the search term and date.",
          "source_file": "logged_information/search/your_search_history.json / .html",
          "columns": {
            "Search term": "The search query entered by the participant.",
            "Date": "Time of the search (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_search_history",
          "title": {
            "en": "Your search history",
            "nl": "Je zoekgeschiedenis"
          },
          "description": {
            "en": "This table contains a record of your search queries on Facebook.",
            "nl": "Deze tabel bevat een overzicht van je zoekopdrachten op Facebook."
          },
          "headers": {
            "Search term": {"en": "Search term", "nl": "Zoekterm"},
            "Date": {"en": "Date", "nl": "Datum"}
          },
          "visualizations": [
            {
              "title": {"en": "Terms you searched for", "nl": "Zoektermen waar je naar zocht"},
              "type": "wordcloud",
              "textColumn": "Search term",
              "tokenize": false
            }
          ],
          "date_columns": {"Date": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _your_search_history_html(reader, errors)

    result = reader.json("logged_information/search/your_search_history.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["searches_v2"]  # pyright: ignore
        for item in items:
            denested_dict = eh.dict_denester(item)

            datapoints.append((
                eh.fix_latin1_string(eh.find_item(denested_dict, "text")),
                eh.raw_timestamp(denested_dict, "timestamp"),
            ))

        out = pd.DataFrame(datapoints, columns=["Search term", "Date"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _your_search_history_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    # Qualified like the JSON twin: exports also carry a Marketplace
    # your_facebook_activity/facebook_marketplace/your_search_history.html,
    # which is a different log (and a different markup).
    result = reader.raw("logged_information/search/your_search_history.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())
        sections = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g')]")
        for section in sections:
            term_divs = section.xpath(".//div[contains(@class, '_2pin')]//div[not(div)]")
            term = term_divs[0].text.strip().strip('"') if term_divs and term_divs[0].text else ""
            date = _section_clock(section)
            datapoints.append((term, date))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Search term", "Date"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def ads_interests_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract Facebook ad interests.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Ad``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents an interest topic Facebook has associated with the participant for ad targeting purposes.",
          "source_file": "ads_interests.json / ads_interests.html",
          "columns": {
            "Ad": "Interest topic used for ad targeting."
          }
        }

    Table config::

        {
          "id": "facebook_ads_interests",
          "title": {
            "en": "Your ad interests",
            "nl": "Je advertentie-interesses"
          },
          "description": {
            "en": "This table shows the interests Facebook has identified for showing you personalized ads.",
            "nl": "Deze tabel toont de interesses die Facebook heeft geïdentificeerd om je gepersonaliseerde advertenties te tonen."
          },
          "headers": {
            "Ad": {"en": "Ad", "nl": "Advertentie"}
          }
        }
    """
    if _is_html(validation):
        return _ads_interests_html(reader, errors)

    result = reader.json("ads_interests.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["topics_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item),
            ))
        out = pd.DataFrame(datapoints, columns=["Ad"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _ads_interests_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("ads_interests.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())
        sections = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g')]")
        for section in sections:
            h2 = section.xpath(".//h2")
            ad = h2[0].text.strip() if h2 and h2[0].text else ""
            if ad:
                datapoints.append((ad,))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Ad"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def _sort_by_numeric_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Sort rows newest-first on a numeric side key derived from ``column``,
    without rewriting the cell itself (ADR-0042). Non-numeric or missing
    values sort last; this never mutates the donated value, only row order.

    Resets the index afterward: ``PropsUIPromptConsentFormTable`` only calls
    ``reset_index`` when truncating past its 10,000-row cap, and
    ``DataFrame.to_json()`` keys each column by index *label*, not position —
    ``parse_table.ts`` then looks rows up by literal label "0", "1", "2", ...
    A sorted frame with stale (pre-sort) labels would render and donate in
    original order, silently undoing this sort.
    """
    if df.empty or column not in df.columns:
        return df
    return (
        df.assign(_k=pd.to_numeric(df[column], errors="coerce"))
        .sort_values("_k", ascending=False, na_position="last")
        .drop(columns="_k")
        .reset_index(drop=True)
    )


def _records(data) -> list:
    """The label/value records of a JSON file, as a list.

    Facebook writes a list of ``{timestamp?, media, label_values, fbid}``
    records — except that a file with exactly one record holds that record
    bare, as an object. Anything else is not a record file and yields
    nothing.
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "label_values" in data:
        return [data]
    return []


def _section_clock(section) -> str:
    """Clock text of one record in an HTML export, exactly as rendered.

    Meta writes a record's time as a display string (e.g. ``Nov 08, 2005
    4:19:54 am``) in a ``_a72d`` div inside the record's footer. Returned
    verbatim, with no parsing or normalising (ADR-0042 — the cell holds the
    export's own value); ``""`` when the footer carries no clock.
    """
    divs = section.xpath(".//footer//div[contains(@class, '_a72d')]")
    return divs[0].text.strip() if divs and divs[0].text else ""


def _node_text(node) -> str:
    """Full text content of *node*: its own text plus every descendant's —
    text after a ``<br>``, inside an inline ``<a>``, or in a tail — never
    just the leading ``.text`` (fix round 1, item 4). Runs of whitespace are
    collapsed to a single space and the result stripped; a ``<br>`` is
    canonicalised to a space first so text on either side of a rendered line
    break stays separated rather than run together.
    """
    for br in node.iter("br"):
        br.tail = " " + (br.tail or "")
    return re.sub(r"\s+", " ", "".join(node.itertext())).strip()


def _leaf_fields(node) -> tuple[dict[str, str], dict[str, str]]:
    """Label → text and label → href of the label/value rows under *node*,
    the first occurrence of a label winning.

    A row is a ``_a6_q`` label cell beside a ``_a6_r`` value cell, or — for a
    link — a colspan label cell that holds the anchor itself (the anchor's
    text is the value, its ``href`` the link). A colspan cell that holds
    neither (a list of nested records) is not a field.
    """
    values: dict[str, str] = {}
    hrefs: dict[str, str] = {}
    for row in eh.xpath_nodes(node, ".//tr[td[contains(@class, '_a6_q')]]"):
        label_td = eh.xpath_nodes(row, "td[contains(@class, '_a6_q')]")[0]
        label = label_td.text.strip() if label_td.text else ""
        if not label or label in values:
            continue
        value_td = eh.xpath_nodes(row, "td[contains(@class, '_a6_r')]")
        if value_td:
            values[label] = value_td[0].text.strip() if value_td[0].text else ""
            continue
        anchors = eh.xpath_nodes(label_td, ".//a[@href]")
        if anchors:
            values[label] = anchors[0].text.strip() if anchors[0].text else ""
            hrefs[label] = str(anchors[0].get("href", ""))
    return values, hrefs


_CONTENT_SHOWN_COLUMNS = ["Category", "Name", "Link", "Date"]

#: Categories of the split files that carry no list label of their own; worded
#: as the grouped layout named the matching sections.
_ADS_CATEGORY = "Ads"
_SHOWS_WATCHED_CATEGORY = "Videos you have watched"

_RECENTLY_VIEWED_JSON = "logged_information/interactions/recently_viewed.json"
_RECENTLY_VIEWED_HTML = "logged_information/interactions/recently_viewed.html"
_CONTENT_SHOWN_FEED_JSON = "logged_information/interactions/content_that_has_been_shown_to_you_in_your_feed.json"
_CONTENT_SHOWN_FEED_HTML = "logged_information/interactions/content_that_has_been_shown_to_you_in_your_feed.html"
_ADS_JSON = "logged_information/interactions/ads.json"
_ADS_HTML = "logged_information/interactions/ads.html"
_SHOWS_WATCHED_JSON = "logged_information/interactions/shows_you_have_watched.json"
_SHOWS_WATCHED_HTML = "logged_information/interactions/shows_you_have_watched.html"


def content_shown_to_you_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract the content Facebook showed the participant (the exposure log).

    Facebook writes this log in one of two layouts. Exports up to June 2026
    use the *grouped* one: ``recently_viewed`` holds sections (feed posts,
    videos, ads, Marketplace, web pages opened off Facebook), each with
    ``entries`` or with ``children`` that hold entries. A record is an entry
    with a ``timestamp``; the section's ``name`` becomes the row's category,
    so Meta's renamings between exports pass through unchanged. Marketplace
    activity counters (entries that carry only a date ``value``) are not
    rows.

    From September 2026 the log is *split* over files of label/value records
    in ``logged_information/interactions``: the feed file (one record whose
    Posts / Videos / Links lists hold Event · URL · Time items; the list
    label is the category), ``ads`` (Ad · Time records, category "Ads") and
    ``shows_you_have_watched`` (Title · URL records with a record timestamp,
    category "Videos you have watched"). Every file present is read and the
    rows concatenated.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Category``, ``Name``, ``Link``, ``Date``. Newest first for
        the JSON path; the HTML path keeps the export's own row order.
        Empty DataFrame when the files are absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row is an item Facebook showed the participant or the participant watched in roughly the last 90 days: posts shown in the feed, videos watched, ads shown, Marketplace items viewed, web pages opened off Facebook. Read from the grouped recently_viewed file (exports up to June 2026) and, from September 2026, from the split content_that_has_been_shown_to_you_in_your_feed (Posts / Videos / Links lists), ads and shows_you_have_watched files; every file present contributes rows. Marketplace activity counters that carry only a date are not rows.",
          "source_file": "logged_information/interactions/recently_viewed.json (grouped, up to June 2026); content_that_has_been_shown_to_you_in_your_feed.json + ads.json + shows_you_have_watched.json (split, from September 2026); .html twins",
          "columns": {
            "Category": "The export's section or list name for the item (e.g. Posts that have been shown to you in your Feed, Posts, Videos, Ads, Videos you have watched, Marketplace Items).",
            "Name": "Name or title of the item as the export gives it.",
            "Link": "URL of the item (the share URL for web pages opened off Facebook).",
            "Date": "Time the item was shown or watched (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_content_shown_to_you",
          "title": {
            "en": "Content shown to you on Facebook",
            "nl": "Content die Facebook je heeft laten zien"
          },
          "description": {
            "en": "This table lists the posts, videos and ads Facebook showed you and the items you viewed in roughly the last 90 days.",
            "nl": "Deze tabel toont de berichten, video's en advertenties die Facebook je in ongeveer de laatste 90 dagen heeft laten zien, en de items die je hebt bekeken."
          },
          "headers": {
            "Category": {"en": "Category", "nl": "Categorie"},
            "Name": {"en": "Name", "nl": "Naam"},
            "Link": {"en": "Link", "nl": "Link"},
            "Date": {"en": "Date", "nl": "Datum en tijd"}
          },
          "date_columns": {"Date": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _content_shown_html(reader, errors)

    datapoints: list = []

    sources: list[tuple[str, Callable[..., list]]] = [
        (_RECENTLY_VIEWED_JSON, _content_shown_grouped_json),
        (_CONTENT_SHOWN_FEED_JSON, _content_shown_feed_json),
        (_ADS_JSON, _content_shown_ads_json),
        (_SHOWS_WATCHED_JSON, _content_shown_shows_json),
    ]
    for member, read in sources:
        result = reader.json(member)
        if not result.found:
            continue
        try:
            datapoints.extend(read(result.data, errors))
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1

    if not datapoints:
        return pd.DataFrame()

    out = pd.DataFrame(datapoints, columns=_CONTENT_SHOWN_COLUMNS)  # pyright: ignore
    return _sort_by_numeric_column(out, "Date")


def _content_shown_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    datapoints: list = []

    sources: list[tuple[str, Callable[..., list]]] = [
        (_RECENTLY_VIEWED_HTML, _content_shown_grouped_html),
        (_CONTENT_SHOWN_FEED_HTML, _content_shown_feed_html),
        (_ADS_HTML, _content_shown_ads_html),
        (_SHOWS_WATCHED_HTML, _content_shown_shows_html),
    ]
    for member, read in sources:
        result = reader.raw(member)
        if not result.found:
            continue
        try:
            datapoints.extend(read(etree.HTML(result.data.read()), errors))
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1

    if datapoints:
        return pd.DataFrame(datapoints, columns=_CONTENT_SHOWN_COLUMNS)  # pyright: ignore

    return pd.DataFrame()


def _content_shown_grouped_json(d, errors: Counter) -> list:
    """Rows of the grouped ``recently_viewed`` file: ``{"recently_viewed":
    [group…]}`` where a group has ``entries`` or ``children`` (never both)."""
    rows = []
    for group in d.get("recently_viewed", []):
        for section in group.get("children") or [group]:
            category = eh.fix_latin1_string(section.get("name", ""))
            for entry in section.get("entries", []):
                if "timestamp" not in entry:
                    continue  # a Marketplace activity counter: only a date value
                data = entry.get("data", {})
                rows.append((
                    category,
                    eh.fix_latin1_string(data.get("name", "")),
                    data.get("uri") or data.get("share") or "",
                    eh.raw_timestamp(entry, "timestamp"),
                ))
    return rows


def _content_shown_feed_json(d, errors: Counter) -> list:
    """Rows of the feed file: one record whose ``label_values`` are the lists
    (``{label: "Posts", vec: [{dict: [{label: "Event", value}, {label: "URL",
    value, href}, {label: "Time", timestamp_value}]}…]}``); the list label
    is the category."""
    rows = []
    for record in _records(d):
        for lv in record.get("label_values", []):
            category = eh.fix_latin1_string(lv.get("label", ""))
            for item in lv.get("vec", []):
                name = link = ""
                date = ""
                for entry in item.get("dict", []):
                    label = entry.get("label")
                    if label == "Event":
                        name = entry.get("value", "")
                    elif label == "URL":
                        link = entry.get("href") or entry.get("value", "")
                    elif label == "Time":
                        date = eh.raw_timestamp(entry, "timestamp_value")
                rows.append((category, eh.fix_latin1_string(name), link, date))
    return rows


def _content_shown_records_json(d, category: str, name_label: str, errors: Counter) -> list:
    """Rows of a split file of ``{timestamp?, label_values: [{label, value,
    href?} | {label: "Time", timestamp_value}]}`` records: the name is the
    *name_label* value (its ``href``, if any, the link), a ``URL`` entry the
    link, a ``Time`` entry the date, else the record's own ``timestamp``."""
    rows = []
    for record in _records(d):
        name = link = ""
        date = eh.raw_timestamp(record, "timestamp")
        for lv in record.get("label_values", []):
            label = lv.get("label")
            if label == name_label:
                name = lv.get("value", "")
                link = lv.get("href") or link
            elif label == "URL":
                link = lv.get("href") or lv.get("value", "")
            elif label == "Time":
                date = eh.raw_timestamp(lv, "timestamp_value")
        rows.append((category, eh.fix_latin1_string(name), link, date))
    return rows


def _content_shown_ads_json(d, errors: Counter) -> list:
    return _content_shown_records_json(d, _ADS_CATEGORY, "Ad", errors)


def _content_shown_shows_json(d, errors: Counter) -> list:
    return _content_shown_records_json(d, _SHOWS_WATCHED_CATEGORY, "Title", errors)


def _content_shown_grouped_html(tree, errors: Counter) -> list:
    """Rows of the grouped ``recently_viewed`` page. A record is a leaf
    ``section._a6-g`` (no section inside it): the name is the first non-empty
    div of its ``_a6-p`` body, the link the footer's anchor, the time the
    footer's ``_a72d``; the category is the nearest enclosing section that
    owns an ``h2``. A leaf without a footer clock (e.g. a Marketplace
    counter) is still donated, with an empty date cell (ADR-0031: the dataset
    never shrinks silently)."""
    rows = []
    leaves = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and not(.//section)]")
    for leaf in leaves:
        date = _section_clock(leaf)
        headings = eh.xpath_nodes(leaf, "ancestor::section[contains(@class, '_a6-g')][h2][1]/h2")
        category = headings[0].text.strip() if headings and headings[0].text else ""
        name_divs = eh.xpath_nodes(leaf, ".//div[contains(@class, '_a6-p')]//div[normalize-space(text()) != '']")
        name = name_divs[0].text.strip() if name_divs and name_divs[0].text else ""
        hrefs = eh.xpath_nodes(leaf, ".//footer//a/@href")
        link = str(hrefs[0]) if hrefs else ""
        rows.append((category, name, link, date))
    return rows


def _content_shown_feed_html(tree, errors: Counter) -> list:
    """Rows of the feed page. Each item is a leaf ``section._a6-g`` holding an
    Event / URL / Time table (the time a display timestamp in the cell, no
    footer); the items of a list sit inside a colspan label cell of the
    enclosing table whose text is the list name — the nearest such ancestor
    cell is the category."""
    rows = []
    for leaf in eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and not(.//section)]"):
        values, hrefs = _leaf_fields(leaf)
        if not values:
            continue
        cells = eh.xpath_nodes(leaf, "ancestor::td[contains(@class, '_a6_q')][1]")
        category = cells[0].text.strip() if cells and cells[0].text else ""
        rows.append((
            category,
            values.get("Event", ""),
            hrefs.get("URL") or values.get("URL", ""),
            values.get("Time", ""),
        ))
    return rows


def _content_shown_records_html(tree, category: str, name_label: str, errors: Counter) -> list:
    """Rows of a split page of records: one top-level ``section._a6-g`` per
    record with a label/value table and a footer. The name is the
    *name_label* cell (its anchor, if it is a link row, the link), a ``URL``
    row the link; the date is a ``Time`` cell when there is one (``ads``
    writes an empty footer), else the footer's ``_a72d``."""
    rows = []
    for section in eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and not(ancestor::section)]"):
        values, hrefs = _leaf_fields(section)
        if not values:
            continue
        date = values.get("Time", "")
        rows.append((
            category,
            values.get(name_label, ""),
            hrefs.get(name_label) or hrefs.get("URL") or values.get("URL", ""),
            date or _section_clock(section),
        ))
    return rows


def _content_shown_ads_html(tree, errors: Counter) -> list:
    return _content_shown_records_html(tree, _ADS_CATEGORY, "Ad", errors)


def _content_shown_shows_html(tree, errors: Counter) -> list:
    return _content_shown_records_html(tree, _SHOWS_WATCHED_CATEGORY, "Title", errors)


_PROFILE_VISITS_COLUMNS = ["Category", "Name", "Timestamp"]

_PROFILE_VISITS_SPLIT_CATEGORY = "Profile visits"
_EVENTS_VISITED_CATEGORY = "Events visited"
_GROUPS_VISITED_CATEGORY = "Groups visited"
#: A groups-and-events record carrying either of these is an event; a group has only a Name.
_EVENT_LABELS = frozenset({"Start time", "End time"})
_PROFILE_VISITS_SPLIT_JSON = "logged_information/interactions/profile_visits.json"
_PROFILE_VISITS_SPLIT_HTML = "profile_visits.html"
_GROUPS_AND_EVENTS_JSON = "logged_information/interactions/groups_and_events_you've_visited.json"
_GROUPS_AND_EVENTS_HTML = "logged_information/interactions/groups_and_events_you've_visited.html"
_RECENTLY_VISITED_JSON = "logged_information/interactions/recently_visited.json"
_RECENTLY_VISITED_HTML = "logged_information/interactions/recently_visited.html"


def profile_visits_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract the profiles, pages, groups and events the participant opened.

    Facebook writes these visits in one of two layouts. Exports up to June
    2026 use the *grouped* one: ``recently_visited`` holds sections (Profile
    visits, Page visits, Events visited, Groups visited, Marketplace Visits),
    each with ``entries``. A record is an entry with a ``timestamp``; the
    section's ``name`` becomes the row's category, so Meta's renamings
    between exports pass through unchanged. Marketplace visit counters
    (entries that carry only a date ``value``) are not rows. From September
    2026 the *split* layout writes two files of label/value records in
    ``logged_information/interactions``: ``profile_visits`` (Name records,
    category "Profile visits") and ``groups_and_events_you've_visited``,
    where a record with a Start time or End time is an event ("Events
    visited") and one with only a Name a group ("Groups visited"). The split
    files are read, and concatenated, when either is present; otherwise the
    grouped one.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Category``, ``Name``, ``Timestamp``. Newest first for the
        JSON path; the HTML path keeps the export's own row order.
        Empty DataFrame when the files are absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row is a Facebook profile, page, group or event the participant opened in roughly the last 90 days. Read from the grouped recently_visited file (exports up to June 2026) or, from September 2026, from the split profile_visits and groups_and_events_you've_visited files (an event carries a Start time or End time, a group only a Name). Marketplace visit counters that carry only a date are not rows.",
          "source_file": "logged_information/interactions/recently_visited.json (grouped, up to June 2026); profile_visits.json + groups_and_events_you've_visited.json (split, from September 2026); .html twins",
          "columns": {
            "Category": "The export's section name for the visit (Profile visits, Page visits, Events visited, Groups visited); in the split layout Profile visits for the profile_visits file and Events visited or Groups visited for the groups-and-events file.",
            "Name": "Name of the visited profile, page, group or event.",
            "Timestamp": "Time the visit occurred (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_profile_visits",
          "title": {
            "en": "Profiles you visited recently",
            "nl": "Profielen die je recentelijk hebt bezocht"
          },
          "description": {
            "en": "This table shows the Facebook profiles, pages, groups and events you opened in roughly the last 90 days.",
            "nl": "Deze tabel toont de Facebook-profielen, pagina's, groepen en evenementen die je in ongeveer de laatste 90 dagen hebt geopend."
          },
          "headers": {
            "Category": {"en": "Category", "nl": "Categorie"},
            "Name": {"en": "Name", "nl": "Naam"},
            "Timestamp": {"en": "Date", "nl": "Datum en tijd"}
          },
          "date_columns": {"Timestamp": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _profile_visits_html(reader, errors)

    datapoints: list = []

    split_layout = False
    sources: list[tuple[str, Callable[..., list]]] = [
        (_PROFILE_VISITS_SPLIT_JSON, _profile_visits_split_json),
        (_GROUPS_AND_EVENTS_JSON, _groups_and_events_visited_json),
    ]
    for member, read in sources:
        result = reader.json(member)
        if not result.found:
            continue
        split_layout = True
        try:
            datapoints.extend(read(result.data, errors))
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    if not split_layout:
        grouped = reader.json(_RECENTLY_VISITED_JSON)
        if grouped.found:
            try:
                datapoints = _profile_visits_grouped_json(grouped.data, errors)
            except Exception as e:
                logger.error("Exception caught: %s", e)
                errors[type(e).__name__] += 1

    if not datapoints:
        return pd.DataFrame()

    out = pd.DataFrame(datapoints, columns=_PROFILE_VISITS_COLUMNS)  # pyright: ignore
    return _sort_by_numeric_column(out, "Timestamp")


def _profile_visits_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    datapoints: list = []

    split_layout = False
    sources: list[tuple[str, Callable[..., list]]] = [
        (_PROFILE_VISITS_SPLIT_HTML, _profile_visits_split_html),
        (_GROUPS_AND_EVENTS_HTML, _groups_and_events_visited_html),
    ]
    for member, read in sources:
        result = reader.raw(member)
        if not result.found:
            continue
        split_layout = True
        try:
            datapoints.extend(read(etree.HTML(result.data.read()), errors))
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    if not split_layout:
        grouped = reader.raw(_RECENTLY_VISITED_HTML)
        if grouped.found:
            try:
                datapoints = _profile_visits_grouped_html(etree.HTML(grouped.data.read()), errors)
            except Exception as e:
                logger.error("Exception caught: %s", e)
                errors[type(e).__name__] += 1

    if datapoints:
        return pd.DataFrame(datapoints, columns=_PROFILE_VISITS_COLUMNS)  # pyright: ignore

    return pd.DataFrame()


def _profile_visits_split_json(d, errors: Counter) -> list:
    """Rows of the split ``profile_visits`` file: ``{timestamp, label_values:
    [{label: "Name", value}]}`` records; the name is the first value."""
    rows = []
    for item in _records(d):
        denested_dict = eh.dict_denester(item)
        rows.append((
            _PROFILE_VISITS_SPLIT_CATEGORY,
            eh.fix_latin1_string(eh.find_item(denested_dict, "-value")),
            eh.raw_timestamp(item, "timestamp"),
        ))
    return rows


def _groups_and_events_visited_json(d, errors: Counter) -> list:
    """Rows of the split ``groups_and_events_you've_visited`` file:
    ``{timestamp, label_values: [{label: "Name", value}, …]}`` records where
    an event also carries Start time / End time (``timestamp_value``),
    Description and URL (``href``) entries and a group only the Name."""
    rows = []
    for record in _records(d):
        label_values = record.get("label_values", [])
        labels = {lv.get("label") for lv in label_values}
        name = next((lv.get("value", "") for lv in label_values if lv.get("label") == "Name"), "")
        rows.append((
            _EVENTS_VISITED_CATEGORY if labels & _EVENT_LABELS else _GROUPS_VISITED_CATEGORY,
            eh.fix_latin1_string(name),
            eh.raw_timestamp(record, "timestamp"),
        ))
    return rows


def _profile_visits_grouped_json(d, errors: Counter) -> list:
    """Rows of the grouped ``recently_visited`` file: ``{"visited_things_v2":
    [{name, description, entries}…]}``."""
    rows = []
    for section in d.get("visited_things_v2", []):
        category = eh.fix_latin1_string(section.get("name", ""))
        for entry in section.get("entries", []):
            if "timestamp" not in entry:
                continue  # a Marketplace visit counter: only a date value
            rows.append((
                category,
                eh.fix_latin1_string(entry.get("data", {}).get("name", "")),
                eh.raw_timestamp(entry, "timestamp"),
            ))
    return rows


def _profile_visits_split_html(tree, errors: Counter) -> list:
    """Rows of the split ``profile_visits`` page: one top-level section per
    record with the name in a ``_a6_r`` cell and a dated footer."""
    rows = []
    sections = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and not(ancestor::section)]")
    for section in sections:
        name_td = section.xpath(".//td[contains(@class, '_a6_r')]")
        name = name_td[0].text.strip() if name_td and name_td[0].text else ""
        date = _section_clock(section)
        if name or date:
            rows.append((_PROFILE_VISITS_SPLIT_CATEGORY, name, date))
    return rows


def _groups_and_events_visited_html(tree, errors: Counter) -> list:
    """Rows of the split ``groups_and_events_you've_visited`` page: one
    top-level section per record with a Name row — an event also has Start
    time / End time, Description and URL rows — and a dated footer."""
    rows = []
    for section in eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and not(ancestor::section)]"):
        values, _ = _leaf_fields(section)
        if not values:
            continue
        rows.append((
            _EVENTS_VISITED_CATEGORY if _EVENT_LABELS & values.keys() else _GROUPS_VISITED_CATEGORY,
            values.get("Name", ""),
            _section_clock(section),
        ))
    return rows


def _profile_visits_grouped_html(tree, errors: Counter) -> list:
    """Rows of the grouped ``recently_visited`` page. A record is a leaf
    ``section._a6-g`` (no section inside it): the name is the first non-empty
    div of its ``_a6-p`` body, the time the footer's ``_a72d``; the category
    is the nearest enclosing section that owns an ``h2``. A leaf without a
    footer clock (e.g. a Marketplace counter) is still donated, with an
    empty date cell (ADR-0031: the dataset never shrinks silently)."""
    rows = []
    leaves = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and not(.//section)]")
    for leaf in leaves:
        date = _section_clock(leaf)
        headings = eh.xpath_nodes(leaf, "ancestor::section[contains(@class, '_a6-g')][h2][1]/h2")
        category = headings[0].text.strip() if headings and headings[0].text else ""
        name_divs = eh.xpath_nodes(leaf, ".//div[contains(@class, '_a6-p')]//div[normalize-space(text()) != '']")
        name = name_divs[0].text.strip() if name_divs and name_divs[0].text else ""
        rows.append((category, name, date))
    return rows


def your_events_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract Facebook events the participant created or was invited to.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Name``, ``Created``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook event the participant created or was invited to, including the event name and creation timestamp.",
          "source_file": "your_facebook_activity/events/your_events.json / your_events.html",
          "columns": {
            "Name": "Name of the Facebook event.",
            "Created": "Time the event was created (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_your_events",
          "title": {
            "en": "Events",
            "nl": "Evenementen"
          },
          "description": {
            "en": "This table contains Facebook events you created or were invited to.",
            "nl": "Deze tabel bevat Facebook-evenementen die je hebt aangemaakt of waarvoor je bent uitgenodigd."
          },
          "headers": {
            "Name": {"en": "Name", "nl": "Naam"},
            "Created": {"en": "Date", "nl": "Datum en tijd"}
          },
          "date_columns": {"Created": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _your_events_html(reader, errors)

    result = reader.json("your_facebook_activity/events/your_events.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["your_events_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item.get("name", "")),
                eh.raw_timestamp(item, "create_timestamp"),
            ))

        out = pd.DataFrame(datapoints, columns=["Name", "Created"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return _sort_by_numeric_column(out, "Created")


def _your_events_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("your_facebook_activity/events/your_events.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())
        sections = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and not(ancestor::section)]")
        for section in sections:
            h2 = section.xpath(".//h2")
            name = h2[0].text.strip() if h2 and h2[0].text else ""
            created = _section_clock(section)
            if name or created:
                datapoints.append((name, created))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Name", "Created"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def group_posts_and_comments_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract posts and comments you made in Facebook groups.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Post``, ``Date``, ``URL``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a post or comment the participant made in a Facebook group, including the title, post content, date, and URL.",
          "source_file": "group_posts_and_comments.json",
          "columns": {
            "Title": "Title of the group post.",
            "Post": "Text content of the post.",
            "Date": "Time of the post or comment (Unix seconds).",
            "URL": "URL of the group post."
          }
        }

    Table config::

        {
          "id": "facebook_group_posts_and_comments",
          "title": {
            "en": "Your posts and comments in groups",
            "nl": "Je berichten en commentaren in groepen"
          },
          "description": {
            "en": "This table shows your posts and comments within Facebook groups.",
            "nl": "Deze tabel toont je berichten en commentaren in Facebook-groepen."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Post": {"en": "Post", "nl": "Bericht"},
            "Date": {"en": "Date", "nl": "Datum"},
            "URL": {"en": "URL", "nl": "URL"}
          },
          "date_columns": {"Date": {"encoding": "epoch-seconds"}}
        }
    """
    result = reader.json("group_posts_and_comments.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        l = d["group_posts_v2"]  # pyright: ignore
        for item in l:
            denested_dict = eh.dict_denester(item)

            datapoints.append((
                eh.fix_latin1_string(eh.find_item(denested_dict, "title")),
                eh.fix_latin1_string(eh.find_item(denested_dict, "post")),
                eh.raw_timestamp(denested_dict, "timestamp"),
                eh.find_item(denested_dict, "url"),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Post", "Date", "URL"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def your_comments_in_groups_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract your comments in Facebook groups.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Comment``, ``Group``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a comment the participant made in a Facebook group, including the title, comment text, group name, and timestamp.",
          "source_file": "your_comments_in_groups.json / your_comments_in_groups.html",
          "columns": {
            "Title": "Title of the post the comment was made on.",
            "Comment": "Text content of the comment.",
            "Group": "Name of the Facebook group.",
            "Timestamp": "Time of the comment (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_your_comments_in_groups",
          "title": {
            "en": "Your comments in groups",
            "nl": "Je commentaren in groepen"
          },
          "description": {
            "en": "This table specifically lists the comments you have made in Facebook groups.",
            "nl": "Deze tabel toont specifiek de commentaren die je in Facebook-groepen hebt geplaatst."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Comment": {"en": "Comment", "nl": "Reactie"},
            "Group": {"en": "Group", "nl": "Groep"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          },
          "date_columns": {"Timestamp": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _your_comments_in_groups_html(reader, errors)

    result = reader.json("your_comments_in_groups.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        l = d["group_comments_v2"]  # pyright: ignore
        for item in l:
            denested_dict = eh.dict_denester(item)

            datapoints.append((
                eh.fix_latin1_string(eh.find_item(denested_dict, "title")),
                eh.fix_latin1_string(eh.find_item(denested_dict, "comment-comment")),
                eh.fix_latin1_string(eh.find_item(denested_dict, "group")),
                eh.raw_timestamp(denested_dict, "timestamp"),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Comment", "Group", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _your_comments_in_groups_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("your_comments_in_groups.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())
        sections = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and not(ancestor::section)]")
        for section in sections:
            h2 = section.xpath(".//h2")
            title = h2[0].text.strip() if h2 and h2[0].text else ""

            # The group is the value of a labelled row, and the comment is the text that
            # follows that row inside the same block::
            #
            #     <div class="_3-95"><span class="_a6_m">Group: </span>A group</div>A comment
            group = ""
            comment = ""
            label_divs = section.xpath(".//div[contains(@class, '_2pin')]//div[contains(@class, '_3-95') and span]")
            if label_divs:
                group = (label_divs[0].xpath("span")[0].tail or "").strip()
                comment = (label_divs[0].tail or "").strip()
            if not comment:
                # A comment on a post outside a group has no labelled row above it.
                comment_divs = section.xpath(".//div[contains(@class, '_2pin')]//div[not(div) and not(span)]")
                comment = comment_divs[0].text.strip() if comment_divs and comment_divs[0].text else ""
            timestamp = _section_clock(section)

            if title or comment or group or timestamp:
                datapoints.append((title, comment, group, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Title", "Comment", "Group", "Timestamp"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def your_group_membership_activity_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract Facebook group membership activity.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Group name``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook group the participant joined. In the HTML format the title contains the full membership sentence (e.g. 'You became a member of X.') because separating the group name from the sentence is fragile and language-dependent; Group name is then not extracted separately.",
          "source_file": "your_group_membership_activity.json / your_group_membership_activity.html",
          "columns": {
            "Title": "Title or description of the membership activity.",
            "Group name": "Name of the Facebook group. Not extracted separately from an HTML export; see Title.",
            "Timestamp": "Time the group was joined (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_your_group_membership_activity",
          "title": {
            "en": "Facebook groups you are a member of",
            "nl": "Facebookgroepen waar je lid van bent"
          },
          "description": {
            "en": "This table lists the Facebook groups you are currently a member of.",
            "nl": "Deze tabel toont de Facebookgroepen waar je momenteel lid van bent."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Group name": {"en": "Group name", "nl": "Groepsnaam"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          },
          "date_columns": {"Timestamp": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _your_group_membership_activity_html(reader, errors)

    result = reader.json("your_group_membership_activity.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["groups_joined_v2"]  # pyright: ignore
        for item in items:
            denested_dict = eh.dict_denester(item)

            datapoints.append((
                eh.fix_latin1_string(eh.find_item(denested_dict, "title")),
                eh.fix_latin1_string(eh.find_item(denested_dict, "name")),
                eh.raw_timestamp(denested_dict, "timestamp"),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Group name", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _your_group_membership_activity_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("your_group_membership_activity.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())
        sections = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and not(ancestor::section)]")
        for section in sections:
            h2 = section.xpath(".//h2")
            title = h2[0].text.strip() if h2 and h2[0].text else ""
            date = _section_clock(section)
            if title or date:
                datapoints.append((title, "", date))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Title", "Group name", "Timestamp"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def pages_and_profiles_you_follow_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract pages and profiles you follow on Facebook.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook Page or profile the participant follows, including the title and time they started following.",
          "source_file": "pages_and_profiles_you_follow.json / pages_and_profiles_you_follow.html",
          "columns": {
            "Title": "Title of the followed Page or profile.",
            "Timestamp": "Time of the follow (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_pages_and_profiles_you_follow",
          "title": {
            "en": "Pages and profiles that you follow",
            "nl": "Pagina's en profielen die je volgt"
          },
          "description": {
            "en": "This table displays the Facebook Pages and profiles that you actively follow.",
            "nl": "Deze tabel toont de Facebookpagina's en -profielen die je actief volgt."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          },
          "date_columns": {"Timestamp": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _pages_and_profiles_you_follow_html(reader, errors)

    result = reader.json("pages_and_profiles_you_follow.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["pages_followed_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item.get("title", "")),
                item.get("timestamp", "")
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _pages_and_profiles_you_follow_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("pages_and_profiles_you_follow.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        sections = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and .//h2]")
        for section in sections:
            h2 = section.xpath(".//h2")
            title = h2[0].text.strip() if h2 and h2[0].text else ""
            timestamp = _section_clock(section)

            datapoints.append((title, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Title", "Timestamp"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def pages_youve_liked_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract Facebook pages you have liked.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Name``, ``URL``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook Page the participant has liked, including the page name, URL, and timestamp.",
          "source_file": "pages_you_ve_liked.json / pages_you've_liked.html",
          "columns": {
            "Name": "Name of the liked Facebook Page.",
            "URL": "URL of the liked Facebook Page.",
            "Timestamp": "Time of the like (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_pages_youve_liked",
          "title": {
            "en": "Pages that you have liked",
            "nl": "Pagina's die je leuk vindt"
          },
          "description": {
            "en": "This table contains a history of the Facebook Pages you have liked.",
            "nl": "Deze tabel bevat een overzicht van de Facebookpagina's die je leuk vindt."
          },
          "headers": {
            "Name": {"en": "Name", "nl": "Naam"},
            "URL": {"en": "URL", "nl": "URL"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          },
          "date_columns": {"Timestamp": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _pages_youve_liked_html(reader, errors)

    result = reader.json("pages_you_ve_liked.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["page_likes_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item.get("name", "")),
                item.get("url", ""),
                item.get("timestamp", "")
            ))

        out = pd.DataFrame(datapoints, columns=["Name", "URL", "Timestamp"]) # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _pages_youve_liked_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("pages_you've_liked.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        sections = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and .//h2]")
        for section in sections:
            h2 = section.xpath(".//h2")
            name = h2[0].text.strip() if h2 and h2[0].text else ""

            url_anchors = section.xpath(".//footer//a/@href")
            url = url_anchors[0] if url_anchors else ""
            timestamp = _section_clock(section)

            datapoints.append((name, url, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Name", "URL", "Timestamp"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def comments_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract all comments you made on Facebook.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Comment``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a comment the participant made on a Facebook post or other content, including the title, comment text, and timestamp.",
          "source_file": "comments_and_reactions/comments.json / .html",
          "columns": {
            "Title": "Title of the post the comment was made on.",
            "Comment": "Text content of the comment.",
            "Timestamp": "Time of the comment (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_comments",
          "title": {
            "en": "Your comments",
            "nl": "Je commentaren"
          },
          "description": {
            "en": "This table shows all the comments you have made on Facebook posts and other content.",
            "nl": "Deze tabel toont alle commentaren die je op Facebook-berichten en andere content hebt geplaatst."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Comment": {"en": "Comment", "nl": "Reactie"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          },
          "date_columns": {"Timestamp": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _comments_html(reader, errors)

    result = reader.json("comments_and_reactions/comments.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["comments_v2"]  # pyright: ignore
        for item in items:
            denested_dict = eh.dict_denester(item)

            datapoints.append((
                eh.fix_latin1_string(eh.find_item(denested_dict, "title")),
                eh.fix_latin1_string(eh.find_item(denested_dict, "comment-comment")),
                eh.raw_timestamp(denested_dict, "timestamp"),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Comment", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _comments_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("your_facebook_activity/comments_and_reactions/comments.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        sections = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and .//h2]")
        for section in sections:
            h2 = section.xpath(".//h2")
            title = h2[0].text.strip() if h2 and h2[0].text else ""

            comment_divs = section.xpath(".//div[contains(@class, '_2pin')]/div[not(div)]")
            comment = _node_text(comment_divs[0]) if comment_divs else ""
            timestamp = _section_clock(section)

            datapoints.append((title, comment, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Title", "Comment", "Timestamp"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def likes_and_reactions_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract likes and reactions with titles from Facebook.

    Reads ``likes_and_reactions_x`` numbered files. The HTML twin also feeds
    ``likes_and_reactions_base_to_df`` (no separate HTML markup distinguishes
    the two; the base table's URL column is then always empty).

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Reaction``, ``Timestamp``.
        Empty DataFrame when no matching files are found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a post the participant liked or reacted to on Facebook, including the post title, reaction type, and timestamp.",
          "source_file": "likes_and_reactions_1.json (and numbered variants) / likes_and_reactions_*.html",
          "columns": {
            "Title": "Title of the post that was liked or reacted to.",
            "Reaction": "Type of reaction (e.g. Like, Love, Haha).",
            "Timestamp": "Time of the reaction (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_likes_and_reactions",
          "title": {
            "en": "Posts you liked (with title)",
            "nl": "Posts die je leuk vond (met titel)"
          },
          "description": {
            "en": "This table shows the titles of posts you liked on Facebook.",
            "nl": "Deze tabel toont de titels van posts die je leuk vond op Facebook."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Reaction": {"en": "Reaction", "nl": "Reactie"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          },
          "date_columns": {"Timestamp": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _likes_and_reactions_html(reader, errors)

    out = pd.DataFrame()
    datapoints = []

    results = reader.json_all(r"(^|/)likes_and_reactions_\d+\.json$")
    if not results:
        return pd.DataFrame()

    try:
        for result in results:
            for item in result.data:
                denested_dict = eh.dict_denester(item)

                datapoints.append((
                    eh.fix_latin1_string(eh.find_item(denested_dict, "title")),
                    eh.fix_latin1_string(eh.find_item(denested_dict, "reaction-reaction")),
                    eh.raw_timestamp(denested_dict, "timestamp"),
                ))

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
        return pd.DataFrame()

    out = pd.DataFrame(datapoints, columns=["Title", "Reaction", "Timestamp"]) #pyright: ignore

    return out


def _likes_and_reactions_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    results = reader.raw_all(r"(^|/)likes_and_reactions_\d+\.html$")
    if not results:
        return pd.DataFrame()

    datapoints = []

    try:
        for result in results:
            tree = etree.HTML(result.data.read())

            sections = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and .//h2]")
            for section in sections:
                h2 = section.xpath(".//h2")
                title = h2[0].text.strip() if h2 and h2[0].text else ""

                # Reaction type from icon img filename (e.g. icons/like.png -> Like)
                img = section.xpath(".//img/@src")
                reaction = ""
                if img:
                    fname = img[0].rsplit("/", 1)[-1] if "/" in img[0] else img[0]
                    reaction = fname.rsplit(".", 1)[0] if "." in fname else fname
                    reaction = reaction.capitalize()
                timestamp = _section_clock(section)

                datapoints.append((title, reaction, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Title", "Reaction", "Timestamp"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def your_posts_check_ins_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract your posts and check-ins on Facebook.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Content``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a post or check-in the participant made on Facebook, including the title, the post text when present, and timestamp.",
          "source_file": "your_posts__check_ins__photos_and_videos_1.json / .html",
          "columns": {
            "Title": "Title of the post or check-in.",
            "Content": "Text of the post, when the export carries one.",
            "Timestamp": "Time of the post or check-in (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_your_posts_and_check_ins",
          "title": {
            "en": "Your posts and check-ins",
            "nl": "Je posts en check-ins"
          },
          "description": {
            "en": "This table shows the posts and places you have checked into on Facebook.",
            "nl": "Deze tabel toont de berichten en plaatsen waar je op Facebook hebt ingecheckt."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Content": {"en": "Content", "nl": "Inhoud"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          },
          "date_columns": {"Timestamp": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _your_posts_check_ins_html(reader, errors)

    result = reader.json("your_posts__check_ins__photos_and_videos_1.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        for item in d:
            post = next(
                (entry["post"] for entry in item.get("data", []) if isinstance(entry, dict) and "post" in entry),
                "",
            )
            datapoints.append((
                eh.fix_latin1_string(item.get("title", "")),
                eh.fix_latin1_string(post),
                item.get("timestamp", ""),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Content", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _your_posts_check_ins_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    results = reader.raw_all(r"(^|/)your_posts__check_ins__photos_and_videos_\d+\.html$")
    if not results:
        return pd.DataFrame()

    datapoints = []

    try:
        for result in results:
            tree = etree.HTML(result.data.read())

            sections = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and .//h2]")
            for section in sections:
                h2 = section.xpath(".//h2")
                title = h2[0].text.strip() if h2 and h2[0].text else ""

                # Post text from the first _2pin div's full text content
                post_divs = section.xpath(".//div[contains(@class, '_2pin')]/div[not(div)]")
                post = _node_text(post_divs[0]) if post_divs else ""
                timestamp = _section_clock(section)

                datapoints.append((title, post, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Title", "Content", "Timestamp"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def likes_and_reactions_base_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract likes and reactions from Facebook (base format).

    Reads ``likes_and_reactions.json`` (no number suffix) or, if absent, the
    numbered variants ``likes_and_reactions_1.json``, ``_2.json``, etc.
    Each item is structured with ``label_values`` containing Reaction, Name,
    and URL. The HTML export carries no separate URL for this table, so the
    HTML path (shared with ``likes_and_reactions_to_df``, whose Title becomes
    this table's Name) always yields an empty URL column.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Reaction``, ``Name``, ``URL``, ``Timestamp``.
        Empty DataFrame when no matching files are found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a like or reaction the participant gave on Facebook, including the reaction type, name, URL, and timestamp.",
          "source_file": "likes_and_reactions.json or likes_and_reactions_1.json (and numbered variants) / likes_and_reactions_*.html",
          "columns": {
            "Reaction": "Type of reaction (e.g. Like, Love, Haha).",
            "Name": "Name of the content that was reacted to.",
            "URL": "URL of the content that was reacted to. Always empty for an HTML export.",
            "Timestamp": "Time of the reaction (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_likes_and_reactions_base",
          "title": {
            "en": "Likes and reactions on Facebook",
            "nl": "Likes en reacties op Facebook"
          },
          "description": {
            "en": "This table shows your likes and reactions to posts and other content on Facebook.",
            "nl": "Deze tabel toont je likes en reacties op berichten en andere content op Facebook."
          },
          "headers": {
            "Reaction": {"en": "Reaction", "nl": "Reactie"},
            "Name": {"en": "Name", "nl": "Naam"},
            "URL": {"en": "URL", "nl": "URL"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          },
          "date_columns": {"Timestamp": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        html = _likes_and_reactions_html(reader, errors)
        if html.empty:
            return html
        return pd.DataFrame({
            "Reaction": html["Reaction"],
            "Name": html["Title"],
            "URL": "",
            "Timestamp": html["Timestamp"],
        })

    datapoints = []

    def _parse_items(d: list) -> None:
        for item in d:
            lv = {x.get("label", ""): x.get("value", "") for x in item.get("label_values", [])}
            datapoints.append((
                lv.get("Reaction", ""),
                eh.fix_latin1_string(lv.get("Name", "")),
                lv.get("URL", ""),
                item.get("timestamp", ""),
            ))

    try:
        result = reader.json("likes_and_reactions.json")
        if result.found:
            _parse_items(result.data)  # pyright: ignore
        else:
            # Fall back to numbered files for DDPs that only export _1, _2, ...
            results = reader.json_all(r"(^|/)likes_and_reactions_\d+\.json$")
            for r in results:
                _parse_items(r.data)  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    out = pd.DataFrame(datapoints, columns=["Reaction", "Name", "URL", "Timestamp"]) if datapoints else pd.DataFrame()  # pyright: ignore
    return out


_OFF_META_JSON = "apps_and_websites_off_of_facebook/your_activity_off_meta_technologies.json"
_OFF_META_HTML = "apps_and_websites_off_of_facebook/your_activity_off_meta_technologies.html"
_OFF_META_COLUMNS = ["Business", "Event", "Date"]


def activity_off_meta_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract the activity businesses reported to Meta about the participant
    from their own websites and apps.

    Facebook writes the JSON in one of two shapes. The 2025 device export
    holds one object keyed ``off_facebook_activity_v2``: a list of businesses,
    each with a ``name`` and flat ``events`` (``id``, ``type``, epoch
    ``timestamp``). The 2026 exports write a top-level list of records
    (``title``, ``fbid``, ``label_values``) whose ``Events`` entry holds a
    ``vec`` of ``ID`` / ``Event`` / ``Received on`` dicts. The two are told
    apart on the top-level type. The HTML export is an index page with one
    section per business linking, root-relative, to that business's own page
    (``h2`` = name; one leaf table per event with ``ID`` / ``Event`` /
    ``Received on`` rows). The linked pages are read one at a time by exact
    path; a page the archive does not hold is an absence, not an error
    (ADR-0024).

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Business``, ``Event``, ``Date``. Newest first for the JSON
        path; the HTML path keeps the export's own row order.
        Empty DataFrame when the files are absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row is one activity that a business or organisation reported to Meta about the participant on its own website or app (page view, search, purchase, app open, ...). JSON has two shapes (off_facebook_activity_v2 with events[]; 2026 record format with label_values); HTML is an index page plus one page per business, read one at a time.",
          "source_file": "apps_and_websites_off_of_facebook/your_activity_off_meta_technologies.json / .html + your_activity_off_meta_technologies/<business>.html",
          "columns": {
            "Business": "Name of the business or app that reported the activity.",
            "Event": "Meta's event code (PAGE_VIEW, VIEW_CONTENT, SEARCH, PURCHASE, CUSTOM, ...).",
            "Date": "Time Meta received the event (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_activity_off_meta",
          "title": {
            "en": "Your activity off Meta technologies",
            "nl": "Je activiteit buiten Meta"
          },
          "description": {
            "en": "Businesses and apps share with Meta what you do on their websites and apps, such as page views, searches and purchases. This table lists what they reported about you.",
            "nl": "Bedrijven en apps delen met Meta wat je op hun websites en apps doet, zoals paginaweergaven, zoekopdrachten en aankopen. Deze tabel toont wat zij over jou hebben doorgegeven."
          },
          "headers": {
            "Business": {"en": "Business", "nl": "Bedrijf"},
            "Event": {"en": "Event", "nl": "Gebeurtenis"},
            "Date": {"en": "Date", "nl": "Datum en tijd"}
          },
          "date_columns": {"Date": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _your_activity_off_meta_html(reader, errors)

    result = reader.json(_OFF_META_JSON)
    if not result.found:
        return pd.DataFrame()

    datapoints: list = []
    try:
        if isinstance(result.data, dict):
            datapoints = _off_meta_v2_json(result.data, errors)
        else:
            datapoints = _off_meta_records_json(result.data, errors)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    if not datapoints:
        return pd.DataFrame()

    out = pd.DataFrame(datapoints, columns=_OFF_META_COLUMNS)  # pyright: ignore
    return _sort_by_numeric_column(out, "Date")


def _off_meta_v2_json(d, errors: Counter) -> list:
    """Rows of the 2025 shape: ``{"off_facebook_activity_v2": [{name,
    events: [{id, type, timestamp}]}]}``."""
    rows = []
    for business in d.get("off_facebook_activity_v2", []):
        name = eh.fix_latin1_string(business.get("name", ""))
        for event in business.get("events", []):
            rows.append((
                name,
                event.get("type", ""),
                eh.raw_timestamp(event, "timestamp"),
            ))
    return rows


def _off_meta_records_json(d, errors: Counter) -> list:
    """Rows of the 2026 shape: a list of ``{title, fbid, media, label_values:
    [{label: "Events", vec: [{dict: [{label, value|timestamp_value}]}]}]}``
    records."""
    rows = []
    for record in _records(d):
        name = eh.fix_latin1_string(record.get("title", ""))
        for lv in record.get("label_values", []):
            if lv.get("label") != "Events":
                continue
            for item in lv.get("vec", []):
                event = ""
                received = ""
                for entry in item.get("dict", []):
                    label = entry.get("label")
                    if label == "Event":
                        event = entry.get("value", "")
                    elif label == "Received on":
                        received = eh.raw_timestamp(entry, "timestamp_value")
                rows.append((name, event, received))
    return rows


def _your_activity_off_meta_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    index = reader.raw(_OFF_META_HTML)
    if not index.found:
        return pd.DataFrame()

    # The index links each business page root-relative; the export root is
    # whatever precedes the index's own path in the archive (a Drive delivery
    # wraps the export in one or two folders, a device download in none).
    member_path = index.member_path or _OFF_META_HTML
    root = member_path[: -len(_OFF_META_HTML)] if member_path.endswith(_OFF_META_HTML) else ""

    links: list[tuple[str, str]] = []
    try:
        tree = etree.HTML(index.data.read())
        for anchor in eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g')]//a[@href]"):
            links.append((anchor.text.strip() if anchor.text else "", str(anchor.get("href"))))
        del tree
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
        return pd.DataFrame()

    datapoints: list[tuple[str, str, str]] = []
    for linked_name, href in links:
        page = reader.raw(root + href)
        if not page.found:
            continue  # the page sits in another part of a split export (ADR-0024)
        try:
            datapoints.extend(_off_meta_page_html(etree.HTML(page.data.read()), linked_name, errors))
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1

    if datapoints:
        return pd.DataFrame(datapoints, columns=_OFF_META_COLUMNS)  # pyright: ignore

    return pd.DataFrame()


def _off_meta_page_html(tree, linked_name: str, errors: Counter) -> list[tuple[str, str, str]]:
    """Rows of one business page. The business is the page's ``h2`` (the
    index's anchor text when the page has none). Each event is a leaf table
    (no table nested inside it) of ``ID`` / ``Event`` / ``Received on`` rows;
    the wrapper table that holds the business ID and nests the event tables
    has no ``Event`` row and yields nothing."""
    headings = eh.xpath_nodes(tree, "//h2")
    name = headings[0].text.strip() if headings and headings[0].text else ""
    rows = []
    for table in eh.xpath_nodes(tree, "//table[not(.//table)]"):
        lv_map: dict[str, str] = {}
        for row in eh.xpath_nodes(table, "./tr[td[contains(@class, '_a6_q')] and td[contains(@class, '_a6_r')]]"):
            label_td = eh.xpath_nodes(row, "td[contains(@class, '_a6_q')]")
            value_td = eh.xpath_nodes(row, "td[contains(@class, '_a6_r')]")
            label = label_td[0].text.strip() if label_td and label_td[0].text else ""
            value = value_td[0].text.strip() if value_td and value_td[0].text else ""
            if label and label not in lv_map:
                lv_map[label] = value
        if "Event" not in lv_map:
            continue
        rows.append((
            name or linked_name,
            lv_map["Event"],
            lv_map.get("Received on", ""),
        ))
    return rows


def advertisers_youve_interacted_with_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract ads you clicked or engaged with on Facebook.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``Action``, ``Title``, ``URL``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents an ad the participant clicked or engaged with on Facebook, including the action taken, ad title, URL, and timestamp.",
          "source_file": "ads_information/advertisers_you've_interacted_with.json / advertisers_you_ve_interacted_with.html",
          "columns": {
            "Action": "Type of interaction with the ad (e.g. Click).",
            "Title": "Title of the ad interacted with.",
            "URL": "URL of the ad or post interacted with.",
            "Timestamp": "Time the interaction occurred (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_advertisers_youve_interacted_with",
          "title": {
            "en": "Ads you clicked or engaged with",
            "nl": "Advertenties waarop je hebt geklikt of gereageerd"
          },
          "description": {
            "en": "This table shows the ads you have clicked or otherwise engaged with on Facebook.",
            "nl": "Deze tabel toont de advertenties waarop je hebt geklikt of waarmee je hebt gecommuniceerd op Facebook."
          },
          "headers": {
            "Action": {"en": "Action", "nl": "Actie"},
            "Title": {"en": "Title", "nl": "Titel"},
            "URL": {"en": "URL", "nl": "URL"},
            "Timestamp": {"en": "Date", "nl": "Datum en tijd"}
          },
          "date_columns": {"Timestamp": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _advertisers_youve_interacted_with_html(reader, errors)

    result = reader.json("ads_information/advertisers_you've_interacted_with.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        for item in d:
            label_values = item.get("label_values", [])
            lv_map = {}
            for lv in label_values:
                lv_map[lv.get("label", "")] = lv.get("value", "")
            datapoints.append((
                eh.fix_latin1_string(lv_map.get("Action", "")),
                eh.fix_latin1_string(lv_map.get("Title", "")),
                lv_map.get("URL", ""),
                eh.raw_timestamp(item, "timestamp"),
            ))

        out = pd.DataFrame(datapoints, columns=["Action", "Title", "URL", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return _sort_by_numeric_column(out, "Timestamp")


def _advertisers_youve_interacted_with_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("advertisers_you've_interacted_with.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        # Each top-level section contains a table with key-value rows and a footer with timestamp
        sections = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and .//table and .//footer]")
        for section in sections:
            kv_rows = section.xpath(".//tr[td[contains(@class, '_a6_q') and not(@colspan)] and td[contains(@class, '_a6_r')]]")
            lv_map = {}
            for row in kv_rows:
                label_td = row.xpath("td[contains(@class, '_a6_q')]")
                value_td = row.xpath("td[contains(@class, '_a6_r')]")
                label = label_td[0].text.strip() if label_td and label_td[0].text else ""
                value = value_td[0].text.strip() if value_td and value_td[0].text else ""
                if label:
                    lv_map[label] = value

            # URL is in a colspan td with an <a> tag
            url_anchors = section.xpath(".//td[contains(@class, '_a6_q') and @colspan]//a/@href")
            url = ""
            for href in url_anchors:
                if href:
                    url = href
                    break

            timestamp = _section_clock(section)

            datapoints.append((
                lv_map.get("Action", ""),
                lv_map.get("Title", ""),
                url,
                timestamp,
            ))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Action", "Title", "URL", "Timestamp"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def link_history_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract links visited from Facebook's in-app browser.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or HTML files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation result; its DDP category selects the JSON or HTML path.

    Returns
    -------
    pd.DataFrame
        Columns: ``URL``, ``Title``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents an outbound link the participant opened in Facebook's in-app browser, with a timestamp.",
          "source_file": "your_facebook_activity/other_activity/link_history.json / link_history.html",
          "columns": {
            "URL": "URL of the link visited.",
            "Title": "Title of the website page visited.",
            "Timestamp": "Time the link was visited (Unix seconds, or the date text from an HTML export)."
          }
        }

    Table config::

        {
          "id": "facebook_link_history",
          "title": {
            "en": "Links visited from Facebook",
            "nl": "Links bezocht vanuit Facebook"
          },
          "description": {
            "en": "This table shows outbound links you opened in Facebook's in-app browser.",
            "nl": "Deze tabel toont uitgaande links die je hebt geopend in de in-app browser van Facebook."
          },
          "headers": {
            "URL": {"en": "URL", "nl": "URL"},
            "Title": {"en": "Title", "nl": "Titel"},
            "Timestamp": {"en": "Date", "nl": "Datum en tijd"}
          },
          "date_columns": {"Timestamp": {"encoding": ["epoch-seconds", "meta-html"]}}
        }
    """
    if _is_html(validation):
        return _link_history_html(reader, errors)

    result = reader.json("your_facebook_activity/other_activity/link_history.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        for item in _records(d):
            denested_dict = eh.dict_denester(item)
            title = ""
            label_values = item.get("label_values", [])
            for lv in label_values:
                if lv.get("label") == "Title of website page you visited":
                    title = lv.get("value", "")
                    break
            datapoints.append((
                eh.find_item(denested_dict, "href"),
                title,
                eh.raw_timestamp(denested_dict, "timestamp"),
            ))

        out = pd.DataFrame(datapoints, columns=["URL", "Title", "Timestamp"])

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return _sort_by_numeric_column(out, "Timestamp")


def _link_history_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("link_history.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())
        sections = eh.xpath_nodes(tree, "//section[contains(@class, '_a6-g') and not(ancestor::section)]")
        for section in sections:
            a_tags = section.xpath(".//a[@href]")
            url = a_tags[0].get("href", "") if a_tags else ""
            title_tds = section.xpath(".//tr[td[contains(@class, '_a6_q') and contains(text(), 'Title of website page you visited')]]/td[contains(@class, '_a6_r')]")
            title = title_tds[0].text.strip() if title_tds and title_tds[0].text else ""
            date = _section_clock(section)
            if url or title or date:
                datapoints.append((url, title, date))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["URL", "Title", "Timestamp"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Extractor registry & platform info
# ---------------------------------------------------------------------------

#: Mapping from the string names used in port_config.json to actual extractor functions.
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "who_youve_followed_to_df": who_youve_followed_to_df,
    "notifications_to_df": notifications_to_df,
    "your_search_history_to_df": your_search_history_to_df,
    "ads_interests_to_df": ads_interests_to_df,
    "content_shown_to_you_to_df": content_shown_to_you_to_df,
    "profile_visits_to_df": profile_visits_to_df,
    "your_events_to_df": your_events_to_df,
    "group_posts_and_comments_to_df": group_posts_and_comments_to_df,
    "your_comments_in_groups_to_df": your_comments_in_groups_to_df,
    "your_group_membership_activity_to_df": your_group_membership_activity_to_df,
    "pages_and_profiles_you_follow_to_df": pages_and_profiles_you_follow_to_df,
    "pages_youve_liked_to_df": pages_youve_liked_to_df,
    "comments_to_df": comments_to_df,
    "likes_and_reactions_to_df": likes_and_reactions_to_df,
    "your_posts_check_ins_to_df": your_posts_check_ins_to_df,
    "likes_and_reactions_base_to_df": likes_and_reactions_base_to_df,
    "activity_off_meta_to_df": activity_off_meta_to_df,
    "advertisers_youve_interacted_with_to_df": advertisers_youve_interacted_with_to_df,
    "link_history_to_df": link_history_to_df,
}


def _is_html(validation) -> bool:
    """Whether *validation* selected the HTML DDP category, so an extractor
    that carries an HTML twin should take that path instead of JSON."""
    return validation is not None and validation.current_ddp_category.ddp_filetype is DDPFiletype.HTML


#: Every public extractor that has an HTML twin and so accepts a ``validation``
#: keyword argument. ``extraction()`` forwards ``validation`` only to these;
#: the rest (``group_posts_and_comments_to_df``, ``notifications_to_df``) stay
#: JSON-only.
_TAKES_VALIDATION = (
    who_youve_followed_to_df,
    your_search_history_to_df,
    ads_interests_to_df,
    content_shown_to_you_to_df,
    profile_visits_to_df,
    your_events_to_df,
    your_comments_in_groups_to_df,
    your_group_membership_activity_to_df,
    pages_and_profiles_you_follow_to_df,
    pages_youve_liked_to_df,
    comments_to_df,
    likes_and_reactions_to_df,
    your_posts_check_ins_to_df,
    likes_and_reactions_base_to_df,
    activity_off_meta_to_df,
    advertisers_youve_interacted_with_to_df,
    link_history_to_df,
)


# ---------------------------------------------------------------------------
# Main extraction & flow
# ---------------------------------------------------------------------------

def extraction(facebook_zip: SeekableBinaryReader, validation) -> ExtractionResult:
    """Extract data from a Facebook DDP zip and return consent-form tables.

    Parameters
    ----------
    facebook_zip:
        Seekable binary reader over the Facebook DDP zip — the upload
        adapter itself, never a path (ADR-0026).
    validation:
        Validation result object whose ``archive_members`` attribute is passed
        to ``ZipArchiveReader``.
    """
    config = load_port_config(EXTRACTOR_REGISTRY, "facebook")
    for table in config:
        if table.extractor in _TAKES_VALIDATION:
            table.extractor_kwargs = {**table.extractor_kwargs, "validation": validation}
    errors: Counter = Counter()
    reader = ZipArchiveReader(facebook_zip, validation.archive_members, errors)
    return run_extraction(reader, errors, config)


class FacebookFlow(FlowBuilder):
    """Flow implementation for the Facebook data donation study."""

    def __init__(self, session_id: str):
        super().__init__(session_id, "Facebook")

    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)

    def extract_data(self, file_value, validation):
        return extraction(file_value, validation)


def process(session_id):
    flow = FacebookFlow(session_id)
    return flow.start_flow()
