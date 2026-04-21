"""
TikTok

This module contains an example flow of a TikTok data donation study.

Assumptions:
It handles DDPs in the English language with filetype JSON (user_data.json).
TikTok changed their export format from .txt to .json. Several section names
also changed; both old and new names are tried when navigating the JSON.

Configuration
-------------
The ``extraction`` function is driven by ``port_config.json``.  Generate one with::

    pnpm generate-config tiktok

Each extractor function carries its own table config in a ``Table config::``
JSON block inside its docstring.  The generator reads those blocks and
assembles the JSON file.

Platform info::

    {
        "name": "TikTok",
        "filetypes": ["json"],
        "languages": ["en", "nl"],
        "description": "Handles DDPs in English. Both user_data.json and user_data_tiktok.json are tried automatically. These data donation flows have not been tested yet, if you find anything wrong with them report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "not yet implemented"
    }
"""

import logging
from collections import Counter
from typing import Callable

import pandas as pd

import port.helpers.extraction_helpers as eh
import port.helpers.port_helpers as ph
import port.helpers.validate as validate
from port.helpers.extraction_helpers import ZipArchiveReader
from port.helpers.flow_builder import FlowBuilder

from port.helpers.validate import (
    DDPCategory,
    DDPFiletype,
    Language,
)
from port.api.d3i_props import ExtractionResult
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
            "user_data.json",
            "user_data_tiktok.json",
        ],
    ),
]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _load_user_data(reader: ZipArchiveReader) -> dict:
    """Load the TikTok export root JSON from the DDP zip."""
    for filename in ("user_data_tiktok.json", "user_data.json"):
        result = reader.json(filename)
        if result.found and isinstance(result.data, dict) and result.data:
            return result.data
    return {}


def _get(d: dict, *keys: str | list[str]):
    """
    Navigate a nested dict, trying each key in order at each level.
    Accepts multiple variant names per level as a list or single string.
    """
    node = d
    for key in keys:
        if not isinstance(node, dict):
            return None
        if isinstance(key, (list, tuple)):
            for k in key:
                if k in node:
                    node = node[k]
                    break
            else:
                return None
        else:
            node = node.get(key)
    return node


def _get_first(d: dict, *paths: tuple[str | list[str], ...]):
    """Return the first non-None result across multiple candidate paths."""
    for path in paths:
        node = _get(d, *path)
        if node is not None:
            return node
    return None


def _item_get(item: dict, *keys: str):
    """Read the first present key from a record, handling case variants."""
    for key in keys:
        if key in item:
            return item.get(key)
        lower = key.lower()
        if lower in item:
            return item.get(lower)
    return ""


# ---------------------------------------------------------------------------
# Extractor functions
# ---------------------------------------------------------------------------

def activity_summary_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract TikTok activity summary counts.

    Reads ``Activity > Activity Summary > ActivitySummaryMap`` from the TikTok
    export JSON.

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
        Columns: ``Metric``, ``Count``.
        Empty DataFrame when the data is absent or parsing fails.

    Table documentation::

        {
          "summary": "Summary counts of TikTok activity since account registration, such as videos watched, commented on, and shared.",
          "source_file": "user_data_tiktok.json or user_data.json",
          "columns": {
            "Metric": "Name of the activity metric.",
            "Count": "Total count for that metric since account registration."
          }
        }

    Table config::

        {
          "id": "tiktok_activity_summary",
          "title": {
            "en": "Your TikTok activity summary",
            "nl": "Samenvatting van je TikTok-activiteit"
          },
          "description": {
            "en": "Summary counts of videos watched, commented on, and shared since account registration.",
            "nl": "Overzicht van het aantal bekeken, becommentarieerde en gedeelde video's sinds registratie."
          },
          "headers": {
            "Metric": {"en": "Metric", "nl": "Metriek"},
            "Count": {"en": "Count", "nl": "Aantal"}
          }
        }
    """
    data = _load_user_data(reader)
    out = pd.DataFrame()
    try:
        summary = _get(
            data,
            ["Activity", "Your Activity"],
            "Activity Summary",
            "ActivitySummaryMap",
        )
        if not isinstance(summary, dict):
            return out

        metric_priority = [
            ("Videos watched since registration", ["videoCount"]),
            ("Videos watched to the end since registration", ["videosWatchedToTheEndSinceAccountRegistration"]),
            ("Videos commented on since registration", ["videosCommentedOnSinceAccountRegistration", "commentVideoCount"]),
            ("Videos shared since registration", ["videosSharedSinceAccountRegistration", "sharedVideoCount"]),
        ]
        rows = []
        for label, keys in metric_priority:
            for key in keys:
                if key in summary:
                    rows.append((label, summary[key]))
                    break
        out = pd.DataFrame(rows, columns=["Metric", "Count"])  # pyright: ignore
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def settings_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract TikTok content preference keyword filters.

    Reads ``App Settings > Settings > SettingsMap`` from the TikTok export JSON.

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
        Columns: ``Setting``, ``Keywords``.
        Empty DataFrame when the data is absent or parsing fails.

    Table documentation::

        {
          "summary": "Keyword filters applied to the participant's TikTok feeds.",
          "source_file": "user_data_tiktok.json or user_data.json",
          "columns": {
            "Setting": "Name of the content preference setting.",
            "Keywords": "Comma-separated list of keywords configured for this setting."
          }
        }

    Table config::

        {
          "id": "tiktok_settings",
          "title": {
            "en": "Content preference keyword filters",
            "nl": "Zoekwoordfilters voor contentvoorkeuren"
          },
          "description": {
            "en": "Keyword filters applied to your Following and For You feeds.",
            "nl": "Zoekwoordfilters die worden toegepast op je Volgend- en Voor Jou-feeds."
          },
          "headers": {
            "Setting": {"en": "Setting", "nl": "Instelling"},
            "Keywords": {"en": "Keywords", "nl": "Trefwoorden"}
          }
        }
    """
    data = _load_user_data(reader)
    out = pd.DataFrame()
    try:
        settings_map = _get(
            data,
            ["App Settings", "Profile And Settings"],
            "Settings",
            "SettingsMap",
        )
        if not isinstance(settings_map, dict):
            return out

        rows = []
        content_preferences = settings_map.get("Content Preferences", {})
        if isinstance(content_preferences, dict):
            field_map = {
                "Keyword filters for videos in Following feed": "Keyword filter for videos in the following feed",
                "Keyword filters for videos in For You feed": "Keyword filters for videos in For You feed",
            }
            rows.extend(
                (label, ", ".join(content_preferences.get(key, [])))
                for key, label in field_map.items()
                if key in content_preferences
            )
        out = pd.DataFrame(rows, columns=["Setting", "Keywords"])  # pyright: ignore
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def watch_history_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract TikTok video watch history.

    Reads ``Activity > Video Browsing History > VideoList`` from the TikTok
    export JSON.

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
        Columns: ``Date``, ``Link``.
        Empty DataFrame when the data is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one TikTok video the participant watched, including the date and video link.",
          "source_file": "user_data_tiktok.json or user_data.json",
          "columns": {
            "Date": "Timestamp of when the video was watched.",
            "Link": "URL of the watched TikTok video."
          }
        }

    Table config::

        {
          "id": "tiktok_watch_history",
          "title": {"en": "Watch history", "nl": "Kijkgeschiedenis"},
          "description": {
            "en": "TikTok videos you have watched.",
            "nl": "TikTok-video's die je hebt bekeken."
          },
          "headers": {
            "Date": {"en": "Date", "nl": "Datum en tijd"},
            "Link": {"en": "Link", "nl": "URL"}
          }
        }
    """
    data = _load_user_data(reader)
    out = pd.DataFrame()
    try:
        items = _get(
            data,
            ["Activity", "Your Activity"],
            ["Video Browsing History", "Watch History"],
            "VideoList",
        )
        if not isinstance(items, list):
            return out
        rows = [(_item_get(item, "Date"), _item_get(item, "Link")) for item in items]
        out = pd.DataFrame(rows, columns=["Date", "Link"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def favorite_videos_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract TikTok favorite videos.

    Reads ``Activity > Favorite Videos > FavoriteVideoList`` from the TikTok
    export JSON.

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
        Columns: ``Date``, ``Link``.
        Empty DataFrame when the data is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one TikTok video the participant marked as a favorite.",
          "source_file": "user_data_tiktok.json or user_data.json",
          "columns": {
            "Date": "Timestamp of when the video was marked as favorite.",
            "Link": "URL of the favorited TikTok video."
          }
        }

    Table config::

        {
          "id": "tiktok_favorite_videos",
          "title": {"en": "Favorite videos", "nl": "Favoriete video's"},
          "description": {
            "en": "Videos you have marked as favorites on TikTok.",
            "nl": "Video's die je als favoriet hebt gemarkeerd op TikTok."
          },
          "headers": {
            "Date": {"en": "Date", "nl": "Datum en tijd"},
            "Link": {"en": "Link", "nl": "URL"}
          }
        }
    """
    data = _load_user_data(reader)
    out = pd.DataFrame()
    try:
        items = _get_first(
            data,
            (["Activity", "Your Activity"], "Favorite Videos", "FavoriteVideoList"),
            ("Likes and Favorites", "Favorite Videos", "FavoriteVideoList"),
        )
        if not isinstance(items, list):
            return out
        rows = [(_item_get(item, "Date"), _item_get(item, "Link")) for item in items]
        out = pd.DataFrame(rows, columns=["Date", "Link"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def follower_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract TikTok follower list.

    Reads ``Activity > Follower List > FansList`` from the TikTok export JSON.

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
        Columns: ``Date``, ``UserName``.
        Empty DataFrame when the data is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one account that follows the participant on TikTok.",
          "source_file": "user_data_tiktok.json or user_data.json",
          "columns": {
            "Date": "Timestamp of when the account started following.",
            "UserName": "Username of the follower account."
          }
        }

    Table config::

        {
          "id": "tiktok_follower",
          "title": {"en": "Your followers", "nl": "Je volgers"},
          "description": {
            "en": "Accounts that follow you on TikTok.",
            "nl": "Accounts die jou volgen op TikTok."
          },
          "headers": {
            "Date": {"en": "Date", "nl": "Datum en tijd"},
            "UserName": {"en": "UserName", "nl": "Gebruikersnaam"}
          }
        }
    """
    data = _load_user_data(reader)
    out = pd.DataFrame()
    try:
        items = _get_first(
            data,
            (["Activity", "Your Activity"], "Follower List", "FansList"),
            ("Profile And Settings", "Follower", "FansList"),
        )
        if not isinstance(items, list):
            return out
        rows = [(_item_get(item, "Date"), _item_get(item, "UserName")) for item in items]
        out = pd.DataFrame(rows, columns=["Date", "UserName"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def following_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract TikTok following list.

    Reads ``Activity > Following List > Following`` from the TikTok export JSON.

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
        Columns: ``Date``, ``UserName``.
        Empty DataFrame when the data is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one account that the participant follows on TikTok.",
          "source_file": "user_data_tiktok.json or user_data.json",
          "columns": {
            "Date": "Timestamp of when the participant started following this account.",
            "UserName": "Username of the followed account."
          }
        }

    Table config::

        {
          "id": "tiktok_following",
          "title": {"en": "Accounts you follow", "nl": "Accounts die je volgt"},
          "description": {
            "en": "Accounts you follow on TikTok.",
            "nl": "Accounts die je volgt op TikTok."
          },
          "headers": {
            "Date": {"en": "Date", "nl": "Datum en tijd"},
            "UserName": {"en": "UserName", "nl": "Gebruikersnaam"}
          }
        }
    """
    data = _load_user_data(reader)
    out = pd.DataFrame()
    try:
        items = _get_first(
            data,
            (["Activity", "Your Activity"], ["Following List", "Following"], "Following"),
            ("Profile And Settings", "Following", "Following"),
        )
        if not isinstance(items, list):
            return out
        rows = [(_item_get(item, "Date"), _item_get(item, "UserName")) for item in items]
        out = pd.DataFrame(rows, columns=["Date", "UserName"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def hashtag_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract TikTok hashtags associated with participant activity.

    Reads ``Activity > Hashtag > HashtagList`` from the TikTok export JSON.

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
        Columns: ``HashtagName``, ``HashtagLink``.
        Empty DataFrame when the data is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one hashtag associated with the participant's TikTok activity.",
          "source_file": "user_data_tiktok.json or user_data.json",
          "columns": {
            "HashtagName": "Name of the hashtag.",
            "HashtagLink": "URL link to the hashtag on TikTok."
          }
        }

    Table config::

        {
          "id": "tiktok_hashtag",
          "title": {"en": "Hashtags", "nl": "Hashtags"},
          "description": {
            "en": "Hashtags associated with your TikTok activity.",
            "nl": "Hashtags gekoppeld aan je TikTok-activiteit."
          },
          "headers": {
            "HashtagName": {"en": "HashtagName", "nl": "Hashtagnaam"},
            "HashtagLink": {"en": "HashtagLink", "nl": "Hashtag-link"}
          }
        }
    """
    data = _load_user_data(reader)
    out = pd.DataFrame()
    try:
        items = _get(
            data,
            ["Activity", "Your Activity"],
            "Hashtag",
            "HashtagList",
        )
        if not isinstance(items, list):
            return out
        rows = [
            (_item_get(item, "HashtagName"), _item_get(item, "HashtagLink"))
            for item in items
        ]
        out = pd.DataFrame(rows, columns=["HashtagName", "HashtagLink"])  # pyright: ignore
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def like_list_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract TikTok liked videos list.

    Reads ``Activity > Like List > ItemFavoriteList`` from the TikTok export JSON.

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
        Columns: ``Date``, ``Link``.
        Empty DataFrame when the data is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one TikTok video the participant liked.",
          "source_file": "user_data_tiktok.json or user_data.json",
          "columns": {
            "Date": "Timestamp of when the video was liked.",
            "Link": "URL of the liked TikTok video."
          }
        }

    Table config::

        {
          "id": "tiktok_like_list",
          "title": {"en": "Videos you liked", "nl": "Video's die je leuk vond"},
          "description": {
            "en": "Videos you have liked on TikTok.",
            "nl": "Video's die je leuk hebt gevonden op TikTok."
          },
          "headers": {
            "Date": {"en": "Date", "nl": "Datum en tijd"},
            "Link": {"en": "Link", "nl": "URL"}
          }
        }
    """
    data = _load_user_data(reader)
    out = pd.DataFrame()
    try:
        items = _get_first(
            data,
            (["Activity", "Your Activity"], "Like List", "ItemFavoriteList"),
            ("Likes and Favorites", "Like List", "ItemFavoriteList"),
        )
        if not isinstance(items, list):
            return out
        rows = [(_item_get(item, "Date"), _item_get(item, "Link")) for item in items]
        out = pd.DataFrame(rows, columns=["Date", "Link"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def searches_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract TikTok search history.

    Reads ``Activity > Search History > SearchList`` from the TikTok export JSON.

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
        Columns: ``Date``, ``SearchTerm``.
        Empty DataFrame when the data is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one search the participant performed on TikTok.",
          "source_file": "user_data_tiktok.json or user_data.json",
          "columns": {
            "Date": "Timestamp of when the search was performed.",
            "SearchTerm": "The search term entered by the participant."
          }
        }

    Table config::

        {
          "id": "tiktok_searches",
          "title": {"en": "Search history", "nl": "Zoekgeschiedenis"},
          "description": {
            "en": "Search terms you have used on TikTok.",
            "nl": "Zoektermen die je hebt gebruikt op TikTok."
          },
          "headers": {
            "Date": {"en": "Date", "nl": "Datum en tijd"},
            "SearchTerm": {"en": "SearchTerm", "nl": "Zoekterm"}
          },
          "visualizations": [
            {
              "title": {"en": "Most searched terms", "nl": "Meest gezochte termen"},
              "type": "wordcloud",
              "textColumn": "SearchTerm",
              "tokenize": false
            }
          ]
        }
    """
    data = _load_user_data(reader)
    out = pd.DataFrame()
    try:
        items = _get(
            data,
            ["Activity", "Your Activity"],
            ["Search History", "Searches"],
            "SearchList",
        )
        if not isinstance(items, list):
            return out
        rows = [(_item_get(item, "Date"), _item_get(item, "SearchTerm")) for item in items]
        out = pd.DataFrame(rows, columns=["Date", "SearchTerm"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def share_history_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract TikTok share history.

    Reads ``Activity > Share History > ShareHistoryList`` from the TikTok
    export JSON.

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
        Columns: ``Date``, ``SharedContent``, ``Link``, ``Method``.
        Empty DataFrame when the data is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one piece of content the participant shared on TikTok.",
          "source_file": "user_data_tiktok.json or user_data.json",
          "columns": {
            "Date": "Timestamp of when the content was shared.",
            "SharedContent": "Description of the shared content.",
            "Link": "URL of the shared content.",
            "Method": "Method used to share the content."
          }
        }

    Table config::

        {
          "id": "tiktok_share_history",
          "title": {"en": "Share history", "nl": "Deelgeschiedenis"},
          "description": {
            "en": "Content you have shared on TikTok, including when, what, and how.",
            "nl": "Inhoud die je hebt gedeeld op TikTok, inclusief wanneer, wat en hoe."
          },
          "headers": {
            "Date": {"en": "Date", "nl": "Datum en tijd"},
            "SharedContent": {"en": "SharedContent", "nl": "Gedeelde inhoud"},
            "Link": {"en": "Link", "nl": "URL"},
            "Method": {"en": "Method", "nl": "Methode"}
          }
        }
    """
    data = _load_user_data(reader)
    out = pd.DataFrame()
    try:
        items = _get(
            data,
            ["Activity", "Your Activity"],
            "Share History",
            "ShareHistoryList",
        )
        if not isinstance(items, list):
            return out
        rows = [
            (
                _item_get(item, "Date"),
                _item_get(item, "SharedContent"),
                _item_get(item, "Link"),
                _item_get(item, "Method"),
            )
            for item in items
        ]
        out = pd.DataFrame(rows, columns=["Date", "SharedContent", "Link", "Method"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def comments_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract TikTok comments.

    Reads ``Comment > Comments > CommentsList`` from the TikTok export JSON.

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
        Columns: ``Date``, ``Comment``, ``Photo``, ``Url``.
        Empty DataFrame when the data is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one comment the participant left on a TikTok video.",
          "source_file": "user_data_tiktok.json or user_data.json",
          "columns": {
            "Date": "Timestamp of when the comment was posted.",
            "Comment": "Text of the comment.",
            "Photo": "Photo associated with the comment, if any.",
            "Url": "URL of the video the comment was posted on."
          }
        }

    Table config::

        {
          "id": "tiktok_comments",
          "title": {"en": "Your comments", "nl": "Je reacties"},
          "description": {
            "en": "Comments you have left on TikTok videos.",
            "nl": "Reacties die je hebt achtergelaten op TikTok-video's."
          },
          "headers": {
            "Date": {"en": "Date", "nl": "Datum en tijd"},
            "Comment": {"en": "Comment", "nl": "Reactie"},
            "Photo": {"en": "Photo", "nl": "Foto"},
            "Url": {"en": "Url", "nl": "URL"}
          },
          "visualizations": [
            {
              "title": {
                "en": "Most common words in your comments",
                "nl": "Meest voorkomende woorden in je reacties"
              },
              "type": "wordcloud",
              "textColumn": "Comment",
              "tokenize": true
            }
          ]
        }
    """
    data = _load_user_data(reader)
    out = pd.DataFrame()
    try:
        items = _get(data, "Comment", "Comments", "CommentsList")
        if not isinstance(items, list):
            return out
        rows = [
            (
                _item_get(item, "Date"),
                _item_get(item, "Comment"),
                _item_get(item, "Photo"),
                _item_get(item, "Url"),
            )
            for item in items
        ]
        out = pd.DataFrame(rows, columns=["Date", "Comment", "Photo", "Url"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


# ---------------------------------------------------------------------------
# Extractor registry & platform info
# ---------------------------------------------------------------------------

#: Mapping from the string names used in port_config.json to actual extractor functions.
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "activity_summary_to_df": activity_summary_to_df,
    "settings_to_df": settings_to_df,
    "watch_history_to_df": watch_history_to_df,
    "favorite_videos_to_df": favorite_videos_to_df,
    "follower_to_df": follower_to_df,
    "following_to_df": following_to_df,
    "hashtag_to_df": hashtag_to_df,
    "like_list_to_df": like_list_to_df,
    "searches_to_df": searches_to_df,
    "share_history_to_df": share_history_to_df,
    "comments_to_df": comments_to_df,
}


# ---------------------------------------------------------------------------
# Main extraction & flow
# ---------------------------------------------------------------------------

def extraction(tiktok_zip: str, validation) -> ExtractionResult:
    """Extract data from a TikTok DDP zip and return consent-form tables.

    Parameters
    ----------
    tiktok_zip:
        Path to the TikTok DDP zip archive on disk.
    validation:
        Validation result object whose ``archive_members`` attribute is passed
        to ``ZipArchiveReader``.
    """
    config = load_port_config(EXTRACTOR_REGISTRY)
    errors: Counter = Counter()
    reader = ZipArchiveReader(tiktok_zip, validation.archive_members, errors)
    return run_extraction(reader, errors, config)


class TikTokFlow(FlowBuilder):
    """Flow implementation for the TikTok data donation study."""

    def __init__(self, session_id: str):
        super().__init__(session_id, "TikTok")

    def generate_file_prompt(self):
        return ph.generate_file_prompt("application/json, application/zip")

    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)

    def extract_data(self, file_value, validation):
        return extraction(file_value, validation)


def process(session_id):
    flow = TikTokFlow(session_id)
    return flow.start_flow()
