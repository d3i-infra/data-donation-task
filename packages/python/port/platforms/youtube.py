"""
YouTube

This module provides an example flow of a YouTube data donation study

Assumptions:
It handles DDPs in the Dutch and English language with filetype JSON.

Configuration
-------------
The ``extraction`` function is driven by ``port_config.json``.  Generate one with::

    pnpm generate-config youtube

Each extractor function carries its own table config in a ``Table config::``
JSON block inside its docstring.  The generator reads those blocks and
assembles the JSON file.

Platform info::

    {
        "name": "YouTube",
        "filetypes": ["json", "csv"],
        "languages": ["en", "nl"],
        "description": "Handles DDPs in both English and Dutch. English and Dutch filenames are tried automatically. These data donation flows have not been tested yet, if you find anything wrong with them report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "not yet implemented"
    }
"""
import json
import logging
from collections import Counter
from typing import Callable

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
            "search-history.json",
            "watch-history.json",
            "subscriptions.csv",
            "comments.csv",
        ],
    ),
    DDPCategory(
        id="json_nl",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.NL,
        known_files=[
            "abonnementen.csv",
            "kijkgeschiedenis.json",
            "zoekgeschiedenis.json",
            "reacties.csv",
        ],
    ),
]


def watch_history_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract watch history from the YouTube DDP.

    Tries the English filename ``watch-history.json`` first, then the Dutch
    ``kijkgeschiedenis.json``.

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
        Columns: ``Title``, ``URL``, ``Timestamp``.
        Empty DataFrame when no matching file is found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one video the participant watched on YouTube, including the video title, URL, and timestamp.",
          "source_file": "watch-history.json or kijkgeschiedenis.json",
          "columns": {
            "Title": "Title of the watched video.",
            "URL": "URL of the watched video.",
            "Timestamp": "ISO 8601 timestamp of when the video was watched."
          }
        }

    Table config::

        {
          "id": "youtube_watch_history",
          "title": {"en": "Your watch history", "nl": "Je kijkgeschiedenis"},
          "description": {
            "en": "Videos you have watched on YouTube, including timestamps.",
            "nl": "Video's die je op YouTube hebt bekeken, inclusief tijdstippen."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "URL": {"en": "URL", "nl": "URL"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          },
          "visualizations": [
            {
              "title": {
                "en": "Videos watched over time",
                "nl": "Bekeken video's in de loop van de tijd"
              },
              "type": "area",
              "group": {"column": "Timestamp", "dateFormat": "auto"},
              "values": [{"aggregate": "count", "label": "Count"}]
            },
            {
              "title": {
                "en": "Videos watched by hour of the day",
                "nl": "Bekeken video's per uur van de dag"
              },
              "type": "bar",
              "group": {"column": "Timestamp", "dateFormat": "hour_cycle", "label": "Hour of the day"},
              "values": [{"label": "Count"}]
            },
            {
              "title": {
                "en": "Words in video titles you watched",
                "nl": "Woorden in titels van bekeken video's"
              },
              "type": "wordcloud",
              "textColumn": "Title",
              "tokenize": true
            }
          ]
        }
    """
    result = None
    for filename in ("watch-history.json", "kijkgeschiedenis.json"):
        r = reader.json(filename)
        if r.found:
            result = r
            break

    if result is None or not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        for item in d:
            datapoints.append((
                item.get("title", ""),
                item.get("titleUrl", ""),
                item.get("time", ""),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "URL", "Timestamp"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def search_history_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract search history from the YouTube DDP.

    Tries the English filename ``search-history.json`` first, then the Dutch
    ``zoekgeschiedenis.json``.

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
        Columns: ``Title``, ``URL``, ``Timestamp``, ``Ad``.
        Empty DataFrame when no matching file is found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one search query or video watch registered in the YouTube search and watch history, including whether an ad was seen.",
          "source_file": "search-history.json or zoekgeschiedenis.json",
          "columns": {
            "Title": "Title of the search result or watched video.",
            "URL": "URL of the search result or watched video.",
            "Timestamp": "ISO 8601 timestamp of when the search was performed.",
            "Ad": "Boolean indicating whether an ad detail was associated with this entry."
          }
        }

    Table config::

        {
          "id": "youtube_search_history",
          "title": {
            "en": "Your search and watch history",
            "nl": "Je zoek- en kijkgeschiedenis"
          },
          "description": {
            "en": "Your search queries, videos watched, and ads seen on YouTube, with timestamps.",
            "nl": "Je zoekopdrachten, bekeken video's en geziene advertenties op YouTube, met tijdstippen."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "URL": {"en": "URL", "nl": "URL"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"},
            "Ad": {"en": "Ad", "nl": "Advertentie"}
          },
          "visualizations": [
            {
              "title": {
                "en": "Words in your search and watch history",
                "nl": "Woorden in je zoek- en kijkgeschiedenis"
              },
              "type": "wordcloud",
              "textColumn": "Title",
              "tokenize": true
            }
          ]
        }
    """
    result = None
    for filename in ("search-history.json", "zoekgeschiedenis.json"):
        r = reader.json(filename)
        if r.found:
            result = r
            break

    if result is None or not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        for item in d:
            datapoints.append((
                item.get("title", ""),
                item.get("titleUrl", ""),
                item.get("time", ""),
                bool(item.get("details") or []),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "URL", "Timestamp", "Ad"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def subscriptions_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract subscriptions from the YouTube DDP.

    Tries ``subscriptions.csv`` first, then the Dutch ``abonnementen.csv``.
    Normalises column names to English regardless of export language.

    Parameters
    ----------
    reader:
        Archive reader used to load CSV files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Channel Id``, ``Channel URL``, ``Channel Name``.
        Empty DataFrame when no matching file is found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one YouTube channel the participant is subscribed to.",
          "source_file": "subscriptions.csv or abonnementen.csv",
          "columns": {
            "Channel Id": "Unique identifier of the subscribed channel.",
            "Channel URL": "URL of the subscribed channel.",
            "Channel Name": "Display name of the subscribed channel."
          }
        }

    Table config::

        {
          "id": "youtube_subscriptions",
          "title": {"en": "Your subscriptions", "nl": "Je abonnementen"},
          "description": {
            "en": "YouTube channels you are subscribed to.",
            "nl": "YouTube-kanalen waarop je bent geabonneerd."
          },
          "headers": {
            "Channel Id": {"en": "Channel Id", "nl": "Kanaal-id"},
            "Channel URL": {"en": "Channel URL", "nl": "Kanaal-URL"},
            "Channel Name": {"en": "Channel Name", "nl": "Kanaalnaam"}
          }
        }
    """
    result = None
    for filename in ("subscriptions.csv", "abonnementen.csv"):
        r = reader.csv(filename)
        if r.found:
            result = r
            break

    if result is None or not result.found:
        return pd.DataFrame()
    df = result.data

    if not df.empty:
        df.columns = ["Channel Id", "Channel URL", "Channel Name"]  # pyright: ignore

    return df


def _parse_comment_text(raw: str) -> str:
    try:
        segments = json.loads(f"[{raw}]")
        return " ".join(s["text"] for s in segments if isinstance(s, dict) and s.get("text", "").strip())
    except Exception:
        return raw


def comments_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract comments from the YouTube DDP.

    Tries ``comments.csv`` first, then the Dutch ``reacties.csv``.
    Normalises column names to English and parses comment text segments.

    Parameters
    ----------
    reader:
        Archive reader used to load CSV files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Timestamp``, ``Channel ID``, ``Comment text``, ``Comment ID``,
        ``Video ID``, ``Price`` (subset available depends on export).
        Empty DataFrame when no matching file is found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one comment the participant posted on a YouTube video or post.",
          "source_file": "comments.csv or reacties.csv",
          "columns": {
            "Timestamp": "ISO 8601 timestamp of when the comment was created.",
            "Channel ID": "ID of the channel where the comment was posted.",
            "Comment text": "Full text of the comment.",
            "Comment ID": "Unique identifier for the comment.",
            "Video ID": "ID of the video the comment was posted on.",
            "Price": "Super Chat amount, if applicable."
          }
        }

    Table config::

        {
          "id": "youtube_comments",
          "title": {"en": "Your comments", "nl": "Je reacties"},
          "description": {
            "en": "Comments you posted on YouTube videos and posts.",
            "nl": "Reacties die je op YouTube-video's en -posts hebt geplaatst."
          },
          "headers": {
            "Comment ID": {"en": "Comment ID", "nl": "Reactie-ID"},
            "Channel ID": {"en": "Channel ID", "nl": "Kanaal-ID"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"},
            "Price": {"en": "Price", "nl": "Prijs"},
            "Video ID": {"en": "Video ID", "nl": "Video-ID"},
            "Comment text": {"en": "Comment text", "nl": "Reactietekst"}
          },
          "visualizations": [
            {
              "title": {
                "en": "Most common words in your comments",
                "nl": "Meest voorkomende woorden in je reacties"
              },
              "type": "wordcloud",
              "textColumn": "Comment text",
              "tokenize": true
            }
          ]
        }
    """
    result = None
    for filename in ("comments.csv", "reacties.csv"):
        r = reader.csv(filename)
        if r.found:
            result = r
            break

    if result is None or not result.found:
        return pd.DataFrame()
    df = result.data

    if not df.empty:
        df = df.rename(columns={
            "Reactie-ID": "Comment ID",
            "Kanaal-ID": "Channel ID",
            "Aanmaaktijdstempel reactie": "Timestamp",
            "Comment create timestamp": "Timestamp",
            "Prijs": "Price",
            "Video-ID": "Video ID",
            "Reactietekst": "Comment text",
        })
        keep = ["Timestamp", "Channel ID", "Comment text", "Comment ID", "Video ID", "Price"]
        df = df[[col for col in keep if col in df.columns]]  # pyright: ignore
        if "Comment text" in df.columns:
            df["Comment text"] = df["Comment text"].apply(_parse_comment_text)

    return df


# ---------------------------------------------------------------------------
# Extractor registry & platform info
# ---------------------------------------------------------------------------

#: Mapping from the string names used in port_config.json to actual extractor functions.
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "watch_history_to_df": watch_history_to_df,
    "search_history_to_df": search_history_to_df,
    "subscriptions_to_df": subscriptions_to_df,
    "comments_to_df": comments_to_df,
}


# ---------------------------------------------------------------------------
# Main extraction & flow
# ---------------------------------------------------------------------------

def extraction(youtube_zip: str, validation) -> ExtractionResult:
    """Extract data from a YouTube DDP zip and return consent-form tables.

    Parameters
    ----------
    youtube_zip:
        Path to the YouTube DDP zip archive on disk.
    validation:
        Validation result object whose ``archive_members`` attribute is passed
        to ``ZipArchiveReader``.
    """
    config = load_port_config(EXTRACTOR_REGISTRY)
    errors: Counter = Counter()
    reader = ZipArchiveReader(youtube_zip, validation.archive_members, errors)
    return run_extraction(reader, errors, config)


class YouTubeFlow(FlowBuilder):
    """Flow implementation for the YouTube data donation study."""

    def __init__(self, session_id: str):
        super().__init__(session_id, "YouTube")

    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)

    def extract_data(self, file, validation):
        return extraction(file, validation)


def process(session_id):
    flow = YouTubeFlow(session_id)
    return flow.start_flow()
