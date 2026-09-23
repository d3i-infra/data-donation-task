"""
Instagram

This module contains an example flow of a Instagram data donation study

Assumptions:
It handles DDPs in the english language with filetype JSON or HTML.

Timestamps
----------
Every date column is written as ``YYYY-MM-DD HH:MM:SS`` in the reference timezone named by
``extraction_helpers.REFERENCE_TIMEZONE``, so that a date means the same thing here as it
does in the TikTok, Facebook and Google tables.

The json export records epoch seconds, which name an absolute instant, so placing them in
that zone is exact.

The html export names no timezone, but it is not written in the timezone of the
participant either: it is rendered eight hours behind UTC, always. Knowing that offset is
what makes it convertible, so the two export formats now agree rather than sitting nine or
ten hours apart.

That offset was measured, not assumed. ``scripts/meta_html_timezone_probe.py`` matches
records held in both formats and reports the difference; run over two donated archives it
put every source at -8 — eight sources apiece, one archive reaching back to 2012, with no
exception anywhere. It is a *fixed* -8 rather than US Pacific, which it otherwise resembles:
records falling inside US daylight saving, where Pacific stands seven hours behind, are
eight hours behind here too, so no daylight saving rule of its own is needed.

It also belongs to Instagram rather than to the person. The Facebook export of those same
two accounts is on a different clock again — Amsterdam for one, UTC for the other — which
is why ``facebook.py`` cannot do the same thing and leaves its html clock alone.

Configuration
-------------
The ``extraction`` function is driven by ``port_config.json``.  Generate one with::

    pnpm generate-config instagram

Each extractor function carries its own table config in a ``Table config::``
JSON block inside its docstring.  The generator reads those blocks and
assembles the JSON file.

Platform info::

    {
        "name": "Instagram",
        "filetypes": ["json", "html"],
        "languages": ["en", "nl"],
        "description": "Note that supported DDP language also includes Dutch and probably other languages as well. You get an english DDP regardless of the Dutch language setting. These data donation flows have not been tested yet, if you find anything wrong with them report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "not yet implemented"
    }
"""

import logging
import os
import re
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Callable

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


#: Months by the first three letters of how the html export abbreviates them, lowercased,
#: across the languages it is written in that use Latin script. An account writes its export
#: in whatever language it is set to, which is not always the language of the study, so the
#: same table the Google extractor reads its html dates with is used here.
_HTML_MONTHS = {
    "jan": 1, "oca": 1, "ene": 1,
    "feb": 2, "şub": 2, "sub": 2,
    "mar": 3, "mrt": 3, "mär": 3, "mrz": 3,
    "apr": 4, "nis": 4, "abr": 4,
    "may": 5, "mei": 5, "mai": 5,
    "jun": 6, "haz": 6,
    "jul": 7, "tem": 7,
    "aug": 8, "ağu": 8, "agu": 8, "ago": 8,
    "sep": 9, "eyl": 9, "set": 9,
    "oct": 10, "okt": 10, "eki": 10,
    "nov": 11, "kas": 11,
    "dec": 12, "dez": 12, "ara": 12, "dic": 12,
}

#: ``Aug 09, 2026 9:49 am`` — how the html export writes a timestamp: the month as a word, a 12-hour
#: clock in lower case, and the seconds left off. The meridiem is optional so that a
#: 24-hour locale reads too.
_HTML_TIMESTAMP = re.compile(
    r"^([^\s\d]+)\.?\s+(\d{1,2}),?\s+(\d{4})[\s,]+(\d{1,2}):(\d{2})(?::(\d{2}))?"
    r"(?:\s*([AaPp])\.?[Mm]\.?)?\s*$"
)


#: How far the html export stands behind UTC. It names no timezone, so this was measured
#: rather than assumed: ``scripts/meta_html_timezone_probe.py`` matched records held in both
#: export formats across two donated archives — eight sources apiece, one of them reaching
#: back to 2012 — and every one of them came out eight hours behind UTC.
#:
#: It is not the timezone of the participant. The Facebook export of the same two accounts
#: is on a different clock again (Amsterdam for one, UTC for the other), so this belongs to
#: Instagram rather than to the person or the account.
#:
#: Nor is it US Pacific, which the offset otherwise resembles. Records falling inside US
#: daylight saving, where Pacific stands seven hours behind, are eight hours behind here
#: too — so the offset is fixed and needs no daylight saving rule of its own.
HTML_EXPORT_UTC_OFFSET = timedelta(hours=-8)


def _html_timestamp(timestamp: str, errors: Counter | None = None) -> str:
    """Write a timestamp read out of the html export in the shared datetime format.

    The html names no timezone and is not written in the timezone of the participant; it is
    rendered at the fixed ``HTML_EXPORT_UTC_OFFSET`` measured above. Knowing that offset is
    what lets this column be converted like any other, so an html donation now agrees with
    a json one rather than sitting nine or ten hours away from it.

    Args:
        timestamp: Text of the date element, e.g. ``Aug 09, 2026 9:49 am``.
        errors: Optional counter that aggregates error types.

    Returns:
        str: The formatted timestamp, ``""`` for an absent one, or the input unchanged
        when it cannot be read.

    Examples::

        >>> _html_timestamp("Aug 09, 2026 9:49 am")   # 2026-08-09 17:49 UTC
        "2026-08-09 19:49:00"
    """
    if not timestamp or not isinstance(timestamp, str):
        return ""

    match = _HTML_TIMESTAMP.match(timestamp.strip())
    if match:
        month, day, year, hour, minute, second, meridiem = match.groups()
        number = _HTML_MONTHS.get(month[:3].lower())

        if number is not None:
            hour = int(hour)
            if meridiem:
                # A 12-hour clock counts noon as 12 pm and midnight as 12 am.
                hour = hour % 12 + (12 if meridiem.lower() == "p" else 0)
            try:
                moment = datetime(int(year), number, int(day), hour, int(minute), int(second or 0))
            except ValueError:
                moment = None

            if moment is not None:
                return eh.local_time_to_datetime_string(
                    moment, HTML_EXPORT_UTC_OFFSET, errors=errors
                )

    logger.error("Could not read an html timestamp: %s", timestamp)
    if errors is not None:
        errors["TimestampParseError"] += 1

    return timestamp


DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "secret_conversations.json",
            "personal_information.json",
            "account_privacy_changes.json",
            "account_based_in.json",
            "recently_deleted_content.json",
            "liked_posts.json",
            "stories.json",
            "profile_photos.json",
            "followers.json",
            "signup_information.json",
            "comments_allowed_from.json",
            "login_activity.json",
            "your_topics.json",
            "camera_information.json",
            "recent_follow_requests.json",
            "devices.json",
            "professional_information.json",
            "follow_requests_you've_received.json",
            "eligibility.json",
            "pending_follow_requests.json",
            "videos_watched.json",
            "ads_viewed.json",
            "ads_clicked.json",
            "ads_interests.json",
            "account_searches.json",
            "profile_searches.json",
            "followers_1.json",
            "saved_posts.json",
            "following.json",
            "posts_viewed.json",
            "post_comments_1.json",
            "recently_unfollowed_accounts.json",
            "post_comments.json",
            "account_information.json",
            "accounts_you're_not_interested_in.json",
            "liked_comments.json",
            "story_likes.json",
            "threads_viewed.json",
            "use_cross-app_messaging.json",
            "profile_changes.json",
            "reels.json",
        ],
    ),
    DDPCategory(
        id="html_en",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.EN,
        known_files=[
            "ads_viewed.html",
            "ads_clicked.html",
            "followers_1.html",
            "following.html", 
            "follow_requests_you've_received.html", 
            "recent_follow_requests.html", 
            "recently_unfollowed_profiles.html", 
            "removed_suggestions.html", 
            "posts_viewed.html", 
            "posts_you're_not_interested_in.html", 
            "videos_watched.html", 
            "advertisers_using_your_activity_or_information.html", 
            "other_categories_used_to_reach_you.html", 
            "word_or_phrase_searches.html", 
            "camera_information.html", 
            "locations_of_interest.html", 
            "profile_based_in.html", 
            "account_supervision.html", 
            "instagram_profile_information.html", 
            "note_and_repost_interactions.html", 
            "personal_information.html", 
            "last_known_location.html", "login_activity.html", 
            "profile_activity.html", 
            "signup_details.html", 
            "post_comments_1.html", 
            "liked_comments.html", 
            "liked_posts.html", 
            "profile_photos.html", 
            "stories.html", 
            "chats.html", 
            "secret_conversations.html", 
            "eligibility.html", 
            "surveys.html", 
            "your_information_download_requests.html", 
            "saved_music.html", 
            "saved_posts.html", 
            "checkout_payment_information.html", 
            "recently_viewed_items.html", 
            "polls.html", 
            "stories_viewed.html", 
            "story_likes.html", 
            "start_here.html",
        ],
    ),
]



# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _sort_by_date(out: pd.DataFrame, date_column: str) -> pd.DataFrame:
    """Sort *out* by *date_column* using timestamp ordering.

    Parameters
    ----------
    out:
        DataFrame to sort.
    date_column:
        Name of the column that contains timestamp strings.
        Rows with empty timestamps are placed last.
    """
    return out.sort_values(by=date_column, key=eh.sort_isotimestamp_empty_timestamp_last)


def _first_present(data: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    """Return the first dict value found for the given keys, or empty dict.

    Parameters
    ----------
    data:
        Dictionary to search.
    keys:
        Ordered list of keys to try; the value of the first key whose
        corresponding value is a ``dict`` is returned.
    """
    for key in keys:
        value = data.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _extract_owner_details(label_values: list[dict[str, Any]]) -> tuple[str, str, str]:
    """Extract ``(owner_name, owner_username, url)`` from a nested label_values structure.

    This structure is used in newer Instagram export formats.

    Parameters
    ----------
    label_values:
        Nested list/dict structure from the Instagram DDP containing labelled
        metadata fields such as ``"Name"``, ``"Username"``, and ``"URL"``.

    Returns
    -------
    tuple[str, str, str]
        A three-tuple of ``(owner_name, owner_username, url)``.  Any field
        not found in *label_values* is returned as an empty string.
    """
    owner_name = ""
    owner_username = ""
    url = ""

    def visit(node: Any) -> None:
        nonlocal owner_name, owner_username, url

        if isinstance(node, list):
            for item in node:
                visit(item)
            return

        if not isinstance(node, dict):
            return

        label = str(node.get("label", ""))
        value = str(node.get("value", ""))
        href = str(node.get("href", ""))

        if label == "URL" and not url:
            url = href or value
        elif label in {"Naam", "Name"} and not owner_name:
            owner_name = eh.fix_latin1_string(value)
        elif label in {"Gebruikersnaam", "Username", "Author"} and not owner_username:
            owner_username = eh.fix_latin1_string(value)

        for child in node.values():
            visit(child)

    visit(label_values)
    return owner_name, owner_username, url


def _extract_owner_from_html(section) -> tuple[str, str]:
    """Extract ``(owner_name, owner_username)`` from an HTML Owner subsection.

    Looks for an ``<h2>Owner</h2>`` inside *section*, then reads the
    innermost ``<table>`` (one without nested tables) to find the
    ``Name`` and ``Username`` rows.

    Returns ``("", "")`` when no Owner block is found.
    """
    owner_h2 = section.xpath('.//h2[text()="Owner"]')
    if not owner_h2:
        return "", ""
    owner_div = owner_h2[0].getparent()
    tables = owner_div.xpath('.//table[not(.//table)]')
    name = ""
    username = ""
    for table in tables:
        for tr in table.xpath('.//tr'):
            tds = tr.xpath('td')
            if len(tds) == 2:
                label = tds[0].text.strip() if tds[0].text else ""
                value = tds[1].text.strip() if tds[1].text else ""
                if label == "Name" and not name:
                    name = value
                elif label == "Username" and not username:
                    username = value
    return name, username


# ---------------------------------------------------------------------------
# Per-table extraction functions
# ---------------------------------------------------------------------------

def following_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract the list of followed accounts into a DataFrame.

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
        Columns: ``Account``, ``URL``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one account that the participant follows on Instagram, including when they started following.",
          "source_file": "following.json / following.html",
          "columns": {
            "Account": "Username or display name of the followed account.",
            "URL": "Direct URL to the followed account's Instagram profile.",
            "Date": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the participant started following this account."
          }
        }

    Table config::

        {
          "id": "instagram_following",
          "title": {
            "en": "Followed Accounts",
            "nl": "Gevolgde Accounts"
          },
          "description": {
            "en": "In this table, you find the accounts that you follow on Instagram.",
            "nl": "In deze tabel zie je de accounts die je volgt op Instagram."
          },
          "headers": {
            "Account": {"en": "Account", "nl": "Account"},
            "URL": {"en": "URL", "nl": "URL"},
            "Date": {"en": "Date", "nl": "Datum en tijd"}
          }
        }
    """
    if validation and validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        return _following_html(reader, errors)

    return _following_json(reader, errors)


def _following_json(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.json("following.json")
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data["relationships_following"]  # pyright: ignore
        for item in items:
            d = eh.dict_denester(item)
            datapoints.append((
                eh.fix_latin1_string(eh.find_item(d, "title") or eh.find_item(d, "value")),
                eh.find_item(d, "href"),
                eh.epoch_to_datetime_string(eh.find_item(d, "timestamp"), errors=errors),
            ))
        out = pd.DataFrame(datapoints, columns=["Account", "URL", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _following_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("following.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        sections = eh.xpath_nodes(tree, "//main/div[contains(@class, '_a6-g')]")
        for section in sections:
            h2 = section.xpath(".//h2")
            account = h2[0].text.strip() if h2 and h2[0].text else ""

            a = section.xpath(".//a[@href]")
            url = a[0].get("href", "") if a else ""

            # Timestamp is the div sibling after the <a> link
            date_divs = section.xpath(".//div[contains(@class, '_a6-p')]//div[not(@class) and not(div) and not(a)]")
            timestamp = ""
            for d in date_divs:
                if d.text and d.text.strip():
                    timestamp = _html_timestamp(d.text.strip(), errors)
                    break

            datapoints.append((account, url, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Account", "URL", "Date"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def posts_viewed_to_df(reader: ZipArchiveReader, errors: Counter, validation=None) -> pd.DataFrame:
    """Extract the list of viewed posts into a DataFrame.

    Handles both the older ``string_map_data`` format (dict root keyed by
    ``"impressions_history_posts_seen"``) and the newer ``label_values``
    list-at-root format.

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
        Columns: ``Author``, ``URL``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one post that appeared in the participant's Instagram feed and was registered as viewed. Captures the author and timing of each impression.",
          "source_file": "posts_viewed.json / posts_viewed.html",
          "columns": {
            "Author": "Username or display name of the account that published the viewed post.",
            "URL": "Direct URL to the viewed post.",
            "Date": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the post was viewed."
          }
        }

    Table config::

        {
          "id": "instagram_posts_viewed",
          "title": {
            "en": "Posts viewed on Instagram",
            "nl": "Berichten bekeken op Instagram"
          },
          "description": {
            "en": "In this table you find the accounts of posts you viewed on Instagram sorted over time. Below, you find visualizations of different parts of this table. First, you find a timeline showing you the number of posts you viewed over time. Next, you find a histogram indicating how many posts you have viewed per hour of the day.",
            "nl": "In deze tabel zie je de accounts van berichten die je op Instagram hebt bekeken, gesorteerd op tijd. Hieronder vind je visualisaties van verschillende onderdelen van deze tabel. Eerst zie je een tijdlijn met het aantal berichten dat je in de loop van de tijd hebt bekeken. Daarna zie je een histogram dat aangeeft hoeveel berichten je per uur van de dag hebt bekeken."
          },
          "headers": {
            "Author": {"en": "Author", "nl": "Account"},
            "URL": {"en": "URL", "nl": "URL"},
            "Date": {"en": "Date", "nl": "Datum en tijd"}
          },
          "visualizations": [
            {
              "title": {
                "en": "The total number of Instagram posts you viewed over time",
                "nl": "Het totale aantal Instagram-berichten dat je in de loop van de tijd hebt bekeken"
              },
              "type": "area",
              "group": {"column": "Date", "dateFormat": "auto", "label": {"en": "Date", "nl": "Datum"}},
              "values": [{"label": {"en": "Number of posts", "nl": "Aantal berichten"}, "aggregate": "count"}]
            },
            {
              "title": {
                "en": "The total number of Instagram posts you have viewed per hour of the day",
                "nl": "Het totale aantal Instagram-berichten dat je per uur van de dag hebt bekeken"
              },
              "type": "bar",
              "group": {"column": "Date", "dateFormat": "hour_cycle", "label": {"en": "Hour of the day", "nl": "Uur van de dag"}},
              "values": [{"label": {"en": "Number of posts", "nl": "Aantal berichten"}}]
            }
          ]
        }
    """
    if validation and validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        return _posts_viewed_html(reader, errors)

    return _posts_viewed_json(reader, errors)


def _posts_viewed_json(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.json("posts_viewed.json")
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data["impressions_history_posts_seen"]  # pyright: ignore
            for item in items:
                string_map_data = item.get("string_map_data", {})
                author = _first_present(string_map_data, ["Author", "Auteur"])
                time = _first_present(string_map_data, ["Time", "Tijd"])
                url = _first_present(string_map_data, ["URL"])
                datapoints.append((
                    eh.fix_latin1_string(str(author.get("value", ""))),
                    url.get("href", ""),
                    eh.epoch_to_datetime_string(time.get("timestamp", ""), errors=errors),
                ))
        else:
            for item in data:  # pyright: ignore
                owner_name, owner_username, url = _extract_owner_details(item.get("label_values", []))
                datapoints.append((
                    owner_username or owner_name,
                    url,
                    eh.epoch_to_datetime_string(item.get("timestamp", ""), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Author", "URL", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _posts_viewed_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("posts_viewed.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        sections = eh.xpath_nodes(tree, "//main/div[contains(@class, '_a6-g')]")
        for section in sections:
            name, username = _extract_owner_from_html(section)
            author = username or name

            url_a = section.xpath(".//td[contains(@class, '_a6_q') and starts-with(text(), 'URL')]//a")
            url = url_a[0].get("href", "") if url_a else ""

            ts = section.xpath(".//div[contains(@class, '_a6-o')]")
            timestamp = _html_timestamp(ts[0].text.strip() if ts and ts[0].text else "", errors)

            datapoints.append((author, url, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Author", "URL", "Date"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def videos_watched_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    validation=None,
) -> pd.DataFrame:
    """Extract the list of watched videos into a DataFrame.

    Handles both the older ``string_map_data`` format (dict root keyed by
    ``"impressions_history_videos_watched"``) and the newer ``label_values``
    list-at-root format.

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
        Columns: ``Author``, ``URL``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one video (including Reels) that the participant watched on Instagram. Captures the creator and timing of each view event.",
          "source_file": "videos_watched.json / videos_watched.html",
          "columns": {
            "Author": "Username or display name of the account that published the watched video.",
            "URL": "Direct URL to the watched video.",
            "Date": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the video was watched."
          }
        }

    Table config::

        {
          "id": "instagram_videos_watched",
          "title": {
            "en": "Videos watched on Instagram",
            "nl": "Video's bekeken op Instagram"
          },
          "description": {
            "en": "In this table you find the accounts of videos you watched on Instagram sorted over time. Below, you find a timeline showing you the number of videos you watched over time.",
            "nl": "In deze tabel zie je de accounts van video's die je op Instagram hebt bekeken, gesorteerd op tijd. Hieronder zie je een tijdlijn met het aantal video's dat je in de loop van de tijd hebt bekeken."
          },
          "headers": {
            "Author": {"en": "Author", "nl": "Account"},
            "URL": {"en": "URL", "nl": "URL"},
            "Date": {"en": "Date", "nl": "Datum en tijd"}
          },
          "visualizations": [
            {
              "title": {
                "en": "The total number of videos watched on Instagram over time",
                "nl": "Het totale aantal video's dat je op Instagram hebt bekeken in de loop van de tijd"
              },
              "type": "area",
              "group": {"column": "Date", "dateFormat": "auto", "label": {"en": "Date", "nl": "Datum"}},
              "values": [{"aggregate": "count", "label": {"en": "Videos watched", "nl": "Bekeken video's"}}]
            }
          ]
        }
    """
    if validation and validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        return _videos_watched_html(reader, errors)
    return _videos_watched_json(reader, errors)


def _videos_watched_json(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.json("videos_watched.json")
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data["impressions_history_videos_watched"]  # pyright: ignore
            for item in items:
                string_map_data = item.get("string_map_data", {})
                author = _first_present(string_map_data, ["Author", "Auteur"])
                time = _first_present(string_map_data, ["Time", "Tijd"])
                url = _first_present(string_map_data, ["URL"])
                datapoints.append((
                    eh.fix_latin1_string(str(author.get("value", ""))),
                    url.get("href", ""),
                    eh.epoch_to_datetime_string(time.get("timestamp", ""), errors=errors),
                ))
        else:
            for item in data:  # pyright: ignore
                owner_name, owner_username, url = _extract_owner_details(item.get("label_values", []))
                datapoints.append((
                    owner_username or owner_name,
                    url,
                    eh.epoch_to_datetime_string(item.get("timestamp", ""), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Author", "URL", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _videos_watched_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("videos_watched.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        sections = eh.xpath_nodes(tree, "//main/div[contains(@class, '_a6-g')]")
        for section in sections:
            name, username = _extract_owner_from_html(section)
            author = username or name

            url_a = section.xpath(".//td[contains(@class, '_a6_q') and starts-with(text(), 'URL')]//a")
            url = url_a[0].get("href", "") if url_a else ""

            ts = section.xpath(".//div[contains(@class, '_a6-o')]")
            timestamp = _html_timestamp(ts[0].text.strip() if ts and ts[0].text else "", errors)

            datapoints.append((author, url, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Author", "URL", "Date"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def post_comments_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    validation=None,
) -> pd.DataFrame:
    """Extract all post comments across multiple matching files into a DataFrame.

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
        Columns: ``Comment``, ``Media owner``, ``Date``.
        Empty DataFrame when no matching files are found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one comment the participant posted on an Instagram post. Covers all matching comment files in the archive (e.g. post_comments.json, post_comments_1.json).",
          "source_file": "post_comments*.json / post_comments*.html",
          "columns": {
            "Comment": "The full text of the comment posted by the participant.",
            "Media owner": "Username of the account that owns the post the comment was placed on.",
            "Date": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the comment was posted."
          }
        }

    Table config::

        {
          "id": "instagram_post_comments",
          "title": {
            "en": "Comments posted on Instagram",
            "nl": "Reacties geplaatst op Instagram"
          },
          "description": {
            "en": "List of comments you posted on Instagram.",
            "nl": "Lijst van reacties die je op Instagram hebt geplaatst."
          },
          "headers": {
            "Comment": {"en": "Comment", "nl": "Reactie"},
            "Media owner": {"en": "Media owner", "nl": "Account"},
            "Date": {"en": "Date", "nl": "Datum en tijd"}
          }
        }
    """
    if validation and validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        return _post_comments_html(reader, errors)
    return _post_comments_json(reader, errors)


def _post_comments_json(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    out = pd.DataFrame()
    datapoints = []

    try:
        results = reader.json_all(r"(^|/)post_comments(?:_\d+)?\.json$")

        if not results:
            return pd.DataFrame()

        for result in results:
            data = result.data
            if isinstance(data, list):
                items = data
            elif "string_map_data" in data:
                items = [data]
            else:
                items = data.get("comments_media_comments", [])
            for item in items:  # pyright: ignore[assignment]
                string_map_data = item.get("string_map_data", {})
                comment = _first_present(string_map_data, ["Comment", "Opmerking"])
                owner = _first_present(string_map_data, ["Media Owner", "Media-eigenaar"])
                time = _first_present(string_map_data, ["Time", "Tijd"])
                datapoints.append((
                    eh.fix_latin1_string(str(comment.get("value", ""))),
                    eh.fix_latin1_string(str(owner.get("value", ""))),
                    eh.epoch_to_datetime_string(time.get("timestamp", ""), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Comment", "Media owner", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _post_comments_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    results = reader.raw_all(r"(^|/)post_comments(?:_\d+)?\.html$")
    if not results:
        return pd.DataFrame()

    datapoints = []

    try:
        for result in results:
            tree = etree.HTML(result.data.read())

            sections = eh.xpath_nodes(tree, "//main/div[contains(@class, '_a6-g')]")
            for section in sections:
                comment = ""
                media_owner = ""
                timestamp = ""

                tds = section.xpath(".//td[contains(@class, '_a6_q')]")
                for td in tds:
                    label = td.text.strip() if td.text else ""
                    if label == "Comment":
                        val_div = td.xpath(".//div/div")
                        comment = val_div[0].text.strip() if val_div and val_div[0].text else ""
                    elif label == "Media Owner":
                        val_div = td.xpath(".//div/div")
                        media_owner = val_div[0].text.strip() if val_div and val_div[0].text else ""
                    elif label == "Time":
                        sibling = td.getnext()
                        if sibling is not None and sibling.text:
                            timestamp = _html_timestamp(sibling.text.strip(), errors)

                datapoints.append((comment, media_owner, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Comment", "Media owner", "Date"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def liked_comments_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    validation=None,
) -> pd.DataFrame:
    """Extract the list of liked comments into a DataFrame.

    Handles both the older ``string_list_data`` format (dict root keyed by
    ``"likes_comment_likes"``) and the newer ``label_values`` list-at-root
    format.  Note that the comment text is not available in the newer format.

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
        Columns: ``Account name``, ``Value``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one comment the participant liked on Instagram. Comment text may be absent in newer export formats.",
          "source_file": "liked_comments.json / liked_comments.html",
          "columns": {
            "Account name": "Username of the account whose comment was liked.",
            "Value": "Text of the liked comment, if available in the export (empty in newer export formats).",
            "Date": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the comment was liked."
          }
        }

    Table config::

        {
          "id": "instagram_liked_comments",
          "title": {
            "en": "Instagram liked comments",
            "nl": "Instagram-reacties die je leuk vond"
          },
          "description": {
            "en": "List of comments that you liked on Instagram.",
            "nl": "Lijst van reacties die je leuk vond op Instagram."
          },
          "headers": {
            "Account name": {"en": "Account name", "nl": "Account"},
            "Value": {"en": "Comment", "nl": "Reactie"},
            "Date": {"en": "Date", "nl": "Datum en tijd"}
          }
        }
    """
    if validation and validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        return _liked_comments_html(reader, errors)
    return _liked_comments_json(reader, errors)


def _liked_comments_json(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.json("liked_comments.json")
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data["likes_comment_likes"]  # pyright: ignore
            for item in items:
                entry = item.get("string_list_data", [{}])[0]
                datapoints.append((
                    eh.fix_latin1_string(item.get("title", "")),
                    eh.fix_latin1_string(entry.get("value", "")),
                    eh.epoch_to_datetime_string(entry.get("timestamp", ""), errors=errors),
                ))
        else:
            for item in data:  # pyright: ignore
                owner_name, owner_username, url = _extract_owner_details(item.get("label_values", []))
                datapoints.append((
                    owner_username or owner_name,
                    "",  # comment text not available in label_values format
                    eh.epoch_to_datetime_string(item.get("timestamp", ""), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Account name", "Value", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _liked_comments_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("liked_comments.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        sections = eh.xpath_nodes(tree, "//main/div[contains(@class, '_a6-g')]")
        for section in sections:
            h2 = section.xpath(".//h2")
            account_name = h2[0].text.strip() if h2 and h2[0].text else ""

            # Value is the link text (e.g. thumbs up emoji)
            a = section.xpath(".//a")
            value = a[0].text.strip() if a and a[0].text else ""

            # Timestamp is the plain div after the <a> link
            date_divs = section.xpath(".//div[contains(@class, '_a6-p')]//div[not(@class) and not(div) and not(a)]")
            timestamp = ""
            for d in date_divs:
                if d.text and d.text.strip():
                    timestamp = _html_timestamp(d.text.strip(), errors)
                    break

            datapoints.append((account_name, value, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Account name", "Value", "Date"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def liked_posts_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    validation=None,
) -> pd.DataFrame:
    """Extract the list of liked posts into a DataFrame.

    Handles both the older ``dict_denester`` format (dict root keyed by
    ``"likes_media_likes"``) and the newer ``label_values`` list-at-root
    format.

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
        Columns: ``Account name``, ``Value``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one post the participant liked on Instagram, including the account whose post was liked and when the like was given.",
          "source_file": "liked_posts.json / liked_posts.html",
          "columns": {
            "Account name": "Username of the account whose post was liked.",
            "Value": "Display name or additional label for the liked post, depending on export format.",
            "Date": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the post was liked."
          }
        }

    Table config::

        {
          "id": "instagram_liked_posts",
          "title": {
            "en": "Instagram liked posts",
            "nl": "Instagram-berichten die je leuk vond"
          },
          "description": {"en": "This table shows posts you liked on Instagram, including the account whose post was liked and when the like was given.", "nl": "In deze tabel ziet u de Instagram-berichten die u leuk vond, met de account die het bericht plaatste."},
          "headers": {
            "Account name": {"en": "Account name", "nl": "Account"},
            "Value": {"en": "Display name", "nl": "Weergavenaam"},
            "Date": {"en": "Date", "nl": "Datum en tijd"}
          },
          "visualizations": [
            {
              "title": {"en": "Most liked accounts", "nl": "Meest gelikete accounts"},
              "type": "wordcloud",
              "textColumn": "Account name",
              "tokenize": false
            }
          ]
        }
    """
    if validation and validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        return _liked_posts_html(reader, errors)
    return _liked_posts_json(reader, errors)


def _liked_posts_json(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.json("liked_posts.json")
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data["likes_media_likes"]  # pyright: ignore
            for item in items:
                d = eh.dict_denester(item)
                datapoints.append((
                    eh.fix_latin1_string(eh.find_item(d, "title")),
                    eh.fix_latin1_string(eh.find_item(d, "value")),
                    eh.epoch_to_datetime_string(eh.find_item(d, "timestamp"), errors=errors),
                ))
        else:
            for item in data:  # pyright: ignore
                owner_name, owner_username, url = _extract_owner_details(item.get("label_values", []))
                datapoints.append((
                    owner_username or owner_name,
                    owner_name,
                    eh.epoch_to_datetime_string(item.get("timestamp", ""), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Account name", "Value", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _liked_posts_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("liked_posts.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        sections = eh.xpath_nodes(tree, "//main/div[contains(@class, '_a6-g')]")
        for section in sections:
            name, username = _extract_owner_from_html(section)
            account_name = username or name

            ts = section.xpath(".//div[contains(@class, '_a6-o')]")
            timestamp = _html_timestamp(ts[0].text.strip() if ts and ts[0].text else "", errors)

            datapoints.append((account_name, name, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Account name", "Value", "Date"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def story_likes_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    validation=None,
) -> pd.DataFrame:
    """Extract the list of liked stories into a DataFrame.

    Handles both the older ``string_list_data`` format (dict root keyed by
    ``"story_activities_story_likes"``) and the newer ``label_values``
    list-at-root format.

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
        Columns: ``Account name``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one Instagram Story the participant liked, recording the account whose story was liked and when.",
          "source_file": "story_likes.json / story_likes.html",
          "columns": {
            "Account name": "Username of the account whose story was liked.",
            "Date": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the story was liked."
          }
        }

    Table config::

        {
          "id": "instagram_story_likes",
          "title": {"en": "Liked Stories", "nl": "Gelikete Stories"},
          "description": {
            "en": "List of Instagram stories you liked.",
            "nl": "Lijst van Instagram-stories die je leuk vond."
          },
          "headers": {
            "Account name": {"en": "Account name", "nl": "Account"},
            "Date": {"en": "Date", "nl": "Datum en tijd"}
          }
        }
    """
    if validation and validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        return _story_likes_html(reader, errors)
    return _story_likes_json(reader, errors)


def _story_likes_json(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.json("story_likes.json")
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data["story_activities_story_likes"]  # pyright: ignore
            for item in items:
                entry = item.get("string_list_data", [{}])[0]
                datapoints.append((
                    eh.fix_latin1_string(item.get("title", "")),
                    eh.epoch_to_datetime_string(entry.get("timestamp", ""), errors=errors),
                ))
        else:
            for item in data:  # pyright: ignore
                owner_name, owner_username, _ = _extract_owner_details(item.get("label_values", []))
                datapoints.append((
                    owner_username or owner_name,
                    eh.epoch_to_datetime_string(item.get("timestamp", ""), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Account name", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _story_likes_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("story_likes.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        sections = eh.xpath_nodes(tree, "//main/div[contains(@class, '_a6-g')]")
        for section in sections:
            name, username = _extract_owner_from_html(section)
            account_name = username or name

            ts = section.xpath(".//div[contains(@class, '_a6-o')]")
            timestamp = _html_timestamp(ts[0].text.strip() if ts and ts[0].text else "", errors)

            datapoints.append((account_name, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Account name", "Date"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def saved_posts_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    validation=None,
) -> pd.DataFrame:
    """Extract the list of saved posts into a DataFrame.

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
        Columns: ``Caption``, ``URL``, ``Username``, ``Hashtags``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one post the participant bookmarked (saved) on Instagram for later viewing.",
          "source_file": "saved_posts.json / saved_posts.html",
          "columns": {
            "Caption": "Caption text of the saved post.",
            "URL": "URL linking to the saved post.",
            "Username": "Username of the account that created the saved post.",
            "Hashtags": "Space-separated hashtags associated with the saved post, or 'No hashtags' if none.",
            "Timestamp": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the post was saved."
          }
        }

    Table config::

        {
          "id": "instagram_saved_posts",
          "title": {
            "en": "Saved posts",
            "nl": "Opgeslagen berichten"
          },
          "description": {
            "en": "List of posts you have saved on Instagram.",
            "nl": "Lijst van berichten die je hebt opgeslagen op Instagram."
          },
          "headers": {
            "Caption": {"en": "Caption", "nl": "Bijschrift"},
            "URL": {"en": "URL", "nl": "URL"},
            "Username": {"en": "Username", "nl": "Account"},
            "Hashtags": {"en": "Hashtags", "nl": "Hashtags"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    if validation and validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        return _saved_posts_html(reader, errors)
    return _saved_posts_json(reader, errors)


def _saved_posts_json(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.json("saved_posts.json")
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data if isinstance(data, list) else data.get("saved_saved_media", [])  # pyright: ignore
        for item in items:
            caption = ""
            url = ""
            username = ""
            hashtags = ""

            for lv in item.get("label_values", []):
                if "value" in lv:
                    # Flavour 1: {"label": "...", "value": "..."}
                    label = lv.get("label", "")
                    value = eh.fix_latin1_string(lv.get("value", ""))
                    if label == "Caption":
                        caption = value
                    elif label == "URL":
                        url = value
                    elif label == "Username":
                        username = value
                elif "dict" in lv:
                    # Flavour 2: {"dict": [...], "title": "..."}
                    title = lv.get("title", "")
                    if title == "Hashtags":
                        dict_list = lv.get("dict", [])
                        tags = []
                        for dict_item in dict_list:
                            denested = eh.dict_denester(dict_item)
                            tag = eh.find_item(denested, "value")
                            if tag:
                                tags.append(eh.fix_latin1_string(tag))
                        hashtags = " ".join(tags) if tags else "No hashtags"
                    elif title == "Owner":
                        dict_list = lv.get("dict", [])
                        for dict_item in dict_list:
                            for inner in dict_item.get("dict", []):
                                if inner.get("label") == "Username":
                                    username = eh.fix_latin1_string(inner.get("value", ""))

            if not hashtags:
                hashtags = "No hashtags"

            datapoints.append((
                caption,
                url,
                username,
                hashtags,
                eh.epoch_to_datetime_string(item.get("timestamp", ""), errors=errors),
            ))
        out = pd.DataFrame(datapoints, columns=["Caption", "URL", "Username", "Hashtags", "Timestamp"])  # pyright: ignore
        out = _sort_by_date(out, "Timestamp")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _saved_posts_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("saved_posts.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        sections = eh.xpath_nodes(tree, "//main/div[contains(@class, '_a6-g')]")
        for section in sections:
            # URL
            url_a = section.xpath(".//td[contains(@class, '_a6_q') and starts-with(text(), 'URL')]//a")
            url = url_a[0].get("href", "") if url_a else ""

            # Caption
            caption_tds = section.xpath(".//td[contains(@class, '_a6_q') and text()='Caption']")
            caption = ""
            if caption_tds:
                sibling = caption_tds[0].getnext()
                if sibling is not None and sibling.text:
                    caption = sibling.text.strip()

            # Owner username
            _, username = _extract_owner_from_html(section)

            # Hashtags
            hashtags = "No hashtags"
            hashtag_h2 = section.xpath('.//h2[text()="Hashtags"]')
            if hashtag_h2:
                hashtag_div = hashtag_h2[0].getparent()
                tag_divs = hashtag_div.xpath('.//div[contains(@class, "_a6-p")]')
                tags = [t.text.strip() for t in tag_divs if t.text and t.text.strip()]
                if tags:
                    hashtags = " ".join(tags)

            # Timestamp
            ts = section.xpath(".//div[contains(@class, '_a6-o')]")
            timestamp = _html_timestamp(ts[0].text.strip() if ts and ts[0].text else "", errors)

            datapoints.append((caption, url, username, hashtags, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Caption", "URL", "Username", "Hashtags", "Timestamp"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def word_or_phrase_searches_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    validation=None,
) -> pd.DataFrame:
    """Extract keyword searches into a DataFrame.

    Reads the older ``string_map_data`` format keyed by
    ``"searches_keyword"``.  Each entry contains a search term and timestamp.

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
        Columns: ``Search term``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one keyword or phrase search the participant performed on Instagram.",
          "source_file": "word_or_phrase_searches.json / word_or_phrase_searches.html",
          "columns": {
            "Search term": "The word or phrase that was searched for.",
            "Date": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the search was performed."
          }
        }

    Table config::

        {
          "id": "instagram_word_or_phrase_searches",
          "title": {
            "en": "Searches",
            "nl": "Zoekopdrachten"
          },
          "description": {
            "en": "List of words or phrases you have searched for on Instagram.",
            "nl": "Lijst van woorden of zinnen die je op Instagram hebt gezocht."
          },
          "headers": {
            "Search term": {"en": "Search term", "nl": "Zoekterm"},
            "Date": {"en": "Date", "nl": "Datum en tijd"}
          }
        }
    """
    if validation and validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        return _word_or_phrase_searches_html(reader, errors)
    return _word_or_phrase_searches_json(reader, errors)


def _word_or_phrase_searches_json(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.json("word_or_phrase_searches.json")
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data.get("searches_keyword", [])
        else:
            items = data  # pyright: ignore

        for item in items:
            string_map_data = item.get("string_map_data", {})
            # The English, Dutch and German keys come from real DDPs; the Spanish,
            # Arabic, Turkish and Chinese ones are derived from Instagram's own
            # translations and have not been checked against a real export yet.
            search = _first_present(string_map_data, [
                "Search", "Zoekopdracht", "Zoeken", "Suche",
                "Búsqueda", "Buscar",
                "بحث", "البحث",
                "Arama", "Ara",
                "搜索", "搜索内容",
            ])
            time = _first_present(string_map_data, [
                "Time", "Tijd", "Datum/Uhrzeit der Suche", "Uhrzeit",
                "Hora", "Fecha y hora",
                "الوقت", "التاريخ والوقت",
                "Saat", "Zaman",
                "时间", "日期和时间",
            ])
            datapoints.append((
                eh.fix_latin1_string(str(search.get("value", ""))),
                eh.epoch_to_datetime_string(time.get("timestamp", ""), errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Search term", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _word_or_phrase_searches_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("word_or_phrase_searches.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        sections = eh.xpath_nodes(tree, "//main/div[contains(@class, '_a6-g')]")
        for section in sections:
            search_term = ""
            timestamp = ""

            tds = section.xpath(".//td[contains(@class, '_a6_q')]")
            for td in tds:
                label = td.text.strip() if td.text else ""
                if label == "Search":
                    val_div = td.xpath(".//div/div")
                    search_term = val_div[0].text.strip() if val_div and val_div[0].text else ""
                elif label == "Time":
                    sibling = td.getnext()
                    if sibling is not None and sibling.text:
                        timestamp = _html_timestamp(sibling.text.strip(), errors)

            datapoints.append((search_term, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Search term", "Date"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def stories_published_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    validation=None,
) -> pd.DataFrame:
    """Extract published stories into a DataFrame.

    Reads the ``"ig_stories"`` key from the JSON.  Each entry contains a
    title and a ``creation_timestamp``.

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
        Columns: ``Text``, ``Media type``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one Instagram Story published by the participant.",
          "source_file": "stories.json / stories.html",
          "columns": {
            "Text": "Caption or text of the story, or 'Story has no text' when empty.",
            "Media type": "File extension of the story media asset (e.g. .jpg, .mp4).",
            "Date": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the story was created."
          }
        }

    Table config::

        {
          "id": "instagram_stories_published",
          "title": {
            "en": "Published stories",
            "nl": "Geplaatste stories"
          },
          "description": {
            "en": "List of stories you have published on Instagram.",
            "nl": "Lijst van stories die je op Instagram hebt geplaatst."
          },
          "headers": {
            "Text": {"en": "Text", "nl": "Tekst"},
            "Media type": {"en": "File type", "nl": "Bestandstype"},
            "Date": {"en": "Date", "nl": "Datum en tijd"}
          }
        }
    """
    if validation and validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        return _stories_published_html(reader, errors)
    return _stories_published_json(reader, errors)


def _stories_published_json(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.json("stories.json")
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data.get("ig_stories", [])
        else:
            items = data  # pyright: ignore

        for item in items:
            title = eh.fix_latin1_string(item.get("title", ""))
            if not title:
                title = "Story has no text"
            uri = item.get("uri", "")
            ext = os.path.splitext(uri)[1] if uri else ""
            datapoints.append((
                title,
                ext,
                eh.epoch_to_datetime_string(item.get("creation_timestamp", ""), errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Text", "Media type", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _stories_published_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("stories.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        sections = eh.xpath_nodes(tree, "//main/div[contains(@class, '_a6-g')]")
        for section in sections:
            # URI from media link
            a = section.xpath(".//a[@href]")
            uri = a[0].get("href", "") if a else ""
            ext = os.path.splitext(uri)[1] if uri else ""

            # Title
            title_h2 = section.xpath(".//h2[contains(@class, '_a6-h') and contains(@class, '_a6-i')]")
            title = title_h2[0].text.strip() if title_h2 and title_h2[0].text else ""
            if not title:
                title = "Story has no text"

            # Timestamp
            ts = section.xpath(".//div[contains(@class, '_a6-o')]")
            timestamp = _html_timestamp(ts[0].text.strip() if ts and ts[0].text else "", errors)

            datapoints.append((title, ext, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Text", "Media type", "Date"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def advertisers_using_activity_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    validation=None,
) -> pd.DataFrame:
    """Extract advertisers using participant activity into a DataFrame.

    Handles the newer ``label_values`` format where advertisers are grouped
    under category labels, and the older format keyed by
    ``"ig_custom_audiences_all_types"``.

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
        Columns: ``Advertiser``, ``Category``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one advertiser that used the participant's activity or information to target them on Instagram.",
          "source_file": "advertisers_using_your_activity_or_information.json / advertisers_using_your_activity_or_information.html",
          "columns": {
            "Advertiser": "Name of the advertiser.",
            "Category": "Category describing how the advertiser used the participant's data."
          }
        }

    Table config::

        {
          "id": "instagram_advertisers_using_activity",
          "title": {
            "en": "Advertisers using your activity or information",
            "nl": "Adverteerders die je activiteit of informatie gebruiken"
          },
          "description": {
            "en": "List of advertisers that used your activity or information to reach you on Instagram.",
            "nl": "Lijst van adverteerders die je activiteit of informatie hebben gebruikt om je te bereiken op Instagram."
          },
          "headers": {
            "Advertiser": {"en": "Advertiser", "nl": "Adverteerder"},
            "Category": {"en": "How they reached you", "nl": "Hoe zij u bereikten"}
          }
        }
    """
    if validation and validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        return _advertisers_using_activity_html(reader, errors)
    return _advertisers_using_activity_json(reader, errors)


def _advertisers_using_activity_json(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.json("advertisers_using_your_activity_or_information.json")
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            # Newer format: label_values at root level
            label_values = data.get("label_values", [])
            if label_values:
                for group in label_values:
                    category = group.get("label", "")
                    for entry in group.get("vec", []):
                        datapoints.append((
                            eh.fix_latin1_string(entry.get("value", "")),
                            category,
                        ))
            else:
                # Older format: ig_custom_audiences_all_types
                items = data.get("ig_custom_audiences_all_types", [])
                for item in items:
                    datapoints.append((
                        eh.fix_latin1_string(item.get("advertiser_name", "")),
                        "",
                    ))

        out = pd.DataFrame(datapoints, columns=["Advertiser", "Category"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _advertisers_using_activity_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("advertisers_using_your_activity_or_information.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        headed_tds = eh.xpath_nodes(tree, "//td[contains(@class, '_a6_q') and @colspan]")
        for td in headed_tds:
            category = td.text.strip() if td.text else ""
            if not category:
                continue

            value_divs = td.xpath(".//div[contains(@class, '_a6-g')]/div[contains(@class, '_a6-p')]")
            for div in value_divs:
                advertiser = div.text.strip() if div.text else ""
                if advertiser:
                    datapoints.append((advertiser, category))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Advertiser", "Category"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def ads_viewed_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "ads_viewed.json",
    validation=None,
) -> pd.DataFrame:
    """Extract the list of viewed ads into a DataFrame.

    Supports both the list-at-root format and the dict format keyed by
    ``"impressions_history_ads_seen"``.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"ads_viewed.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Account name``, ``Name``, ``URL``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one advertisement impression shown to the participant on Instagram. Includes the advertiser identity and when the ad was displayed.",
          "source_file": "ads_viewed.json / ads_viewed.html",
          "columns": {
            "Account name": "Username of the advertiser's Instagram account.",
            "Name": "Display name of the advertiser.",
            "URL": "URL associated with the advertisement.",
            "Date": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the ad was shown to the participant."
          }
        }

    Table config::

        {
          "id": "instagram_ads_viewed",
          "title": {
            "en": "Ads viewed on Instagram",
            "nl": "Advertenties bekeken op Instagram"
          },
          "description": {
            "en": "List of ads that you viewed on Instagram.",
            "nl": "Lijst van advertenties die je op Instagram hebt bekeken."
          },
          "headers": {
            "Account name": {"en": "Account name", "nl": "Account"},
            "Name": {"en": "Name", "nl": "Naam"},
            "URL": {"en": "URL", "nl": "URL"},
            "Date": {"en": "Date", "nl": "Datum en tijd"}
          }
        }
    """
    if validation and validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        return _ads_viewed_html(reader, errors)

    return _ads_viewed_json(reader, errors, filename=filename)


def _ads_viewed_json(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "ads_viewed.json",
) -> pd.DataFrame:
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("impressions_history_ads_seen", [])  # pyright: ignore
        else:
            items = []

        for item in items:  # pyright: ignore
            owner_name, owner_username, url = _extract_owner_details(item.get("label_values", []))
            datapoints.append((
                owner_username or owner_name,
                owner_name,
                url,
                eh.epoch_to_datetime_string(item.get("timestamp", ""), errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Account name", "Name", "URL", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _ads_viewed_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("ads_viewed.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        sections = eh.xpath_nodes(tree, "//main/div[contains(@class, '_a6-g')]")
        for section in sections:
            name, username = _extract_owner_from_html(section)

            url_a = section.xpath(".//td[contains(@class, '_a6_q') and starts-with(text(), 'URL')]//a")
            url = url_a[0].get("href", "") if url_a else ""

            ts = section.xpath(".//div[contains(@class, '_a6-o')]")
            timestamp = _html_timestamp(ts[0].text.strip() if ts and ts[0].text else "", errors)

            datapoints.append((username or name, name, url, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Account name", "Name", "URL", "Date"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()



def profile_searches_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "profile_searches.json",
    validation=None,
) -> pd.DataFrame:
    """Extract the list of profile searches into a DataFrame.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"profile_searches.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Timestamp``, ``Name``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one profile search performed by the participant on Instagram, recording what was searched and when.",
          "source_file": "profile_searches.json",
          "columns": {
            "Name": "Username or display name that was searched for.",
            "Timestamp": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the search was performed."
          }
        }

    Table config::

        {
          "id": "instagram_profile_searches",
          "title": {
            "en": "Profile searches",
            "nl": "Profielzoekopdrachten"
          },
          "description": {
            "en": "List of profiles you have searched for on Instagram.",
            "nl": "Lijst van profielen die je op Instagram hebt gezocht."
          },
          "headers": {
            "Name": {"en": "Name", "nl": "Naam"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data["searches_user"]  # pyright: ignore
        for item in items:
            d = eh.dict_denester(item)
            datapoints.append((
                eh.epoch_to_datetime_string(eh.find_item(d, "timestamp"), errors=errors),
                eh.fix_latin1_string(eh.find_item(d, "title") or eh.find_item(d, "value")),
            ))
        out = pd.DataFrame(datapoints, columns=["Timestamp", "Name"])  # pyright: ignore
        out = _sort_by_date(out, "Timestamp")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def threads_viewed_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "threads_viewed.json",
    validation=None,
) -> pd.DataFrame:
    """Extract the list of viewed Threads posts into a DataFrame.

    Handles both the older ``string_map_data`` format (dict root keyed by
    ``"text_post_app_text_post_app_posts_seen"``) and the newer
    ``label_values`` list-at-root format.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"threads_viewed.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Author``, ``URL``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one post on Threads (Meta's text-based social network linked to Instagram) that the participant viewed, including the author and timing.",
          "source_file": "threads_viewed.json",
          "columns": {
            "Author": "Username or display name of the account that published the viewed Threads post.",
            "URL": "Direct URL to the viewed Threads post.",
            "Date": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the post was viewed."
          }
        }

    Table config::

        {
          "id": "instagram_threads_viewed",
          "title": {"en": "Threads viewed", "nl": "Threads bekeken"},
          "description": {
            "en": "List of Threads posts you viewed.",
            "nl": "Lijst van Threads-berichten die je hebt bekeken."
          },
          "headers": {
            "Author": {"en": "Author", "nl": "Account"},
            "URL": {"en": "URL", "nl": "URL"},
            "Date": {"en": "Date", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data["text_post_app_text_post_app_posts_seen"]  # pyright: ignore
            for item in items:
                string_map_data = item.get("string_map_data", {})
                author = _first_present(string_map_data, ["Author", "Auteur"])
                time = _first_present(string_map_data, ["Time", "Tijd"])
                url = _first_present(string_map_data, ["URL"])
                datapoints.append((
                    eh.fix_latin1_string(str(author.get("value", ""))),
                    url.get("href", ""),
                    eh.epoch_to_datetime_string(time.get("timestamp", ""), errors=errors),
                ))
        else:
            for item in data:  # pyright: ignore
                owner_name, owner_username, url = _extract_owner_details(item.get("label_values", []))
                datapoints.append((
                    owner_username or owner_name,
                    url,
                    eh.epoch_to_datetime_string(item.get("timestamp", ""), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Author", "URL", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def ads_clicked_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "ads_clicked.json",
    validation=None,
) -> pd.DataFrame:
    """Extract the list of clicked ads into a DataFrame.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"ads_clicked.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Action``, ``Title``, ``URL``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one advertisement the participant clicked on Instagram.",
          "source_file": "ads_clicked.json",
          "columns": {
            "Action": "The action performed on the ad (e.g. Click).",
            "Title": "Title or name of the clicked advertisement.",
            "URL": "URL of the clicked advertisement.",
            "Timestamp": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the ad was clicked."
          }
        }

    Table config::

        {
          "id": "instagram_ads_clicked",
          "title": {
            "en": "Ads clicked on Instagram",
            "nl": "Advertenties aangeklikt op Instagram"
          },
          "description": {
            "en": "List of ads you clicked on Instagram.",
            "nl": "Lijst van advertenties die je op Instagram hebt aangeklikt."
          },
          "headers": {
            "Action": {"en": "Action", "nl": "Actie"},
            "Title": {"en": "Title", "nl": "Titel"},
            "URL": {"en": "URL", "nl": "URL"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data if isinstance(data, list) else [data]  # pyright: ignore
        for item in items:
            action = ""
            title = ""
            url = ""

            for lv in item.get("label_values", []):
                if "value" in lv:
                    label = lv.get("label", "")
                    value = eh.fix_latin1_string(lv.get("value", ""))
                    if label == "Action":
                        action = value
                    elif label == "Title":
                        title = value
                    elif label == "URL":
                        url = value

            datapoints.append((
                action,
                title,
                url,
                eh.epoch_to_datetime_string(item.get("timestamp", ""), errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Action", "Title", "URL", "Timestamp"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def posts_published_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename_pattern: str = r"(^|/)posts(?:_\d+)?\.json$",
    validation=None,
) -> pd.DataFrame:
    """Extract published posts across multiple matching files into a DataFrame.

    Reads files matching ``posts_1.json``, ``posts_2.json``, etc.  Each entry
    contains a title and a ``creation_timestamp``.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename_pattern:
        Regular expression matched against archive member paths.  Defaults to
        a pattern that matches ``posts.json``, ``posts_1.json``, etc.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Timestamp``.
        Empty DataFrame when no matching files are found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one post published by the participant on Instagram.",
          "source_file": "posts_*.json / posts_*.html",
          "columns": {
            "Title": "Caption or title text of the post.",
            "Timestamp": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the post was created."
          }
        }

    Table config::

        {
          "id": "instagram_posts_published",
          "title": {
            "en": "Posts",
            "nl": "Geplaatste berichten"
          },
          "description": {
            "en": "List of posts you have published on Instagram.",
            "nl": "Lijst van publieke berichten die je op Instagram hebt geplaatst."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    if validation and validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        return _posts_published_html(reader, errors)

    return _posts_published_json(reader, errors, filename_pattern=filename_pattern)


def _posts_published_json(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename_pattern: str = r"(^|/)posts(?:_\d+)?\.json$",
) -> pd.DataFrame:
    out = pd.DataFrame()
    datapoints = []

    try:
        results = reader.json_all(filename_pattern)
        if not results:
            return pd.DataFrame()

        for result in results:
            data = result.data
            # Posts can be a list at root or nested under a key
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = [data]
            else:
                items = []

            for item in items:  # pyright: ignore
                dd = eh.dict_denester(item)
                datapoints.append((
                    eh.fix_latin1_string(eh.find_item(dd, "title")),
                    eh.epoch_to_datetime_string(eh.find_item(dd, "creation_timestamp"), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Title", "Timestamp"])  # pyright: ignore
        out = _sort_by_date(out, "Timestamp")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _posts_published_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    results = reader.raw_all(r"(^|/)posts(?:_\d+)?\.html$")
    if not results:
        return pd.DataFrame()

    datapoints = []

    try:
        for result in results:
            tree = etree.HTML(result.data.read())

            sections = eh.xpath_nodes(tree, "//main/div[contains(@class, '_a6-g')]")
            for section in sections:
                # The post title (caption) is the section's own heading; media
                # entries nested deeper carry their own headings.
                h2 = section.xpath("h2")
                title = h2[0].text.strip() if h2 and h2[0].text else ""

                ts = section.xpath(".//div[contains(@class, '_a6-o')]")
                timestamp = _html_timestamp(ts[0].text.strip() if ts and ts[0].text else "", errors)

                datapoints.append((title, timestamp))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Title", "Timestamp"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def subscription_for_no_ads_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "subscription_for_no_ads.json",
    validation=None,
) -> pd.DataFrame:
    """Extract ad-free subscription status into a DataFrame.

    Reads the ``label_values`` structure from the subscription file.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"subscription_for_no_ads.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Label``, ``Value``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a field from the participant's ad-free subscription status on Instagram.",
          "source_file": "subscription_for_no_ads.json / subscription_for_no_ads.html",
          "columns": {
            "Label": "Description label for the subscription field.",
            "Value": "Value of the subscription field."
          }
        }

    Table config::

        {
          "id": "instagram_subscription_for_no_ads",
          "title": {
            "en": "Ad-free subscription status",
            "nl": "Status advertentievrij abonnement"
          },
          "description": {
            "en": "Your ad-free subscription status on Instagram.",
            "nl": "Je status van het advertentievrije abonnement op Instagram."
          },
          "headers": {
            "Label": {"en": "Label", "nl": "Label"},
            "Value": {"en": "Value", "nl": "Waarde"}
          }
        }
    """
    if validation and validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        return _subscription_for_no_ads_html(reader, errors)

    return _subscription_for_no_ads_json(reader, errors, filename=filename)


def _subscription_for_no_ads_json(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "subscription_for_no_ads.json",
) -> pd.DataFrame:
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            label_values = data.get("label_values", [])
        elif isinstance(data, list):
            label_values = data
        else:
            label_values = []

        for item in label_values:
            datapoints.append((
                item.get("label", ""),
                item.get("value", ""),
            ))

        out = pd.DataFrame(datapoints, columns=["Label", "Value"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def _subscription_for_no_ads_html(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    result = reader.raw("subscription_for_no_ads.html")
    if not result.found:
        return pd.DataFrame()

    datapoints = []

    try:
        tree = etree.HTML(result.data.read())

        rows = eh.xpath_nodes(tree, "//tr[td[contains(@class, '_a6_q')] and td[contains(@class, '_a6_r')]]")
        for row in rows:
            label_td = row.xpath("td[contains(@class, '_a6_q')]")
            value_td = row.xpath("td[contains(@class, '_a6_r')]")
            label = label_td[0].text.strip() if label_td and label_td[0].text else ""
            value = value_td[0].text.strip() if value_td and value_td[0].text else ""
            if label:
                datapoints.append((eh.fix_latin1_string(label), eh.fix_latin1_string(value)))

        if datapoints:
            return pd.DataFrame(datapoints, columns=["Label", "Value"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return pd.DataFrame()


def followers_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "followers_1.json",
) -> pd.DataFrame:
    """Extract the list of followers into a DataFrame.

    Handles both the newer bare top-level list format and the older format
    where entries are wrapped under a ``"relationships_followers"`` key.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"followers_1.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Account``, ``URL``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one account that follows the participant on Instagram, including when they started following.",
          "source_file": "followers_1.json",
          "columns": {
            "Account": "Username or display name of the follower account.",
            "URL": "Direct URL to the follower's Instagram profile.",
            "Date": "Timestamp (YYYY-MM-DD HH:MM:SS, Europe/Amsterdam) of when the account started following the participant."
          }
        }

    Table config::

        {
          "id": "instagram_followers",
          "title": {"en": "Your Instagram followers", "nl": "Je Instagram-volgers"},
          "description": {
            "en": "List of accounts that follow you on Instagram.",
            "nl": "Lijst van accounts die jou op Instagram volgen."
          },
          "headers": {
            "Account": {"en": "Account", "nl": "Account"},
            "URL": {"en": "URL", "nl": "URL"},
            "Date": {"en": "Date", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data.get("relationships_followers", [])
        else:
            items = data  # pyright: ignore

        for item in items:
            d = eh.dict_denester(item)
            datapoints.append((
                eh.fix_latin1_string(eh.find_item(d, "value") or eh.find_item(d, "title")),
                eh.find_item(d, "href"),
                eh.epoch_to_datetime_string(eh.find_item(d, "timestamp"), errors=errors),
            ))
        out = pd.DataFrame(datapoints, columns=["Account", "URL", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


# ---------------------------------------------------------------------------
# Extractor registry & platform info
# ---------------------------------------------------------------------------

#: Mapping from the string names used in port_config.json to actual extractor functions.
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "following_to_df": following_to_df,
    "posts_viewed_to_df": posts_viewed_to_df,
    "videos_watched_to_df": videos_watched_to_df,
    "post_comments_to_df": post_comments_to_df,
    "liked_comments_to_df": liked_comments_to_df,
    "liked_posts_to_df": liked_posts_to_df,
    "story_likes_to_df": story_likes_to_df,
    "saved_posts_to_df": saved_posts_to_df,
    "word_or_phrase_searches_to_df": word_or_phrase_searches_to_df,
    "stories_published_to_df": stories_published_to_df,
    "advertisers_using_activity_to_df": advertisers_using_activity_to_df,
    "ads_viewed_to_df": ads_viewed_to_df,
    "profile_searches_to_df": profile_searches_to_df,
    "threads_viewed_to_df": threads_viewed_to_df,
    "ads_clicked_to_df": ads_clicked_to_df,
    "posts_published_to_df": posts_published_to_df,
    "subscription_for_no_ads_to_df": subscription_for_no_ads_to_df,
    "followers_to_df": followers_to_df,
}


# ---------------------------------------------------------------------------
# Main extraction & flow
# ---------------------------------------------------------------------------

def extraction(
    instagram_zip: SeekableBinaryReader,
    validation,
) -> ExtractionResult:
    """Extract data from an Instagram DDP zip and return consent-form tables.

    Parameters
    ----------
    instagram_zip:
        Seekable binary reader over the Instagram DDP zip — the upload
        adapter itself, never a path (ADR-0026).
    validation:
        Validation result object whose ``archive_members`` attribute is passed
        to ``ZipArchiveReader``.
    """
    config = load_port_config(EXTRACTOR_REGISTRY, "instagram")
    for table in config:
        table.extractor_kwargs = {'validation': validation}
    errors: Counter = Counter()
    reader = ZipArchiveReader(instagram_zip, validation.archive_members, errors)
    return run_extraction(reader, errors, config)


class InstagramFlow(FlowBuilder):
    """Flow implementation for the Instagram data donation study.

    Parameters
    ----------
    session_id:
        Unique identifier for the current participant session.
    """

    def __init__(self, session_id: str):
        super().__init__(session_id, "Instagram")

    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)

    def extract_data(self, file_value, validation):
        return extraction(file_value, validation)


def process(session_id):
    flow = InstagramFlow(session_id)
    return flow.start_flow()
