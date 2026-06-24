"""
YouTube

This module provides an example flow of a YouTube data donation study

Assumptions:
It handles DDPs in the Dutch and English language with filetype JSON or HTML for the watch and search histories and CSV for other files.

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
        "filetypes": ["json", "html", "csv"],
        "languages": ["en", "nl"],
        "description": "Handles DDPs in both English and Dutch. Both JSON and HTML formats are supported for watch and search histories. Comments and subscriptions are always extracted in CSV format. Tested for Dutch DDPs with both JSON and HTML formats. English DDPs have not yet been tested. If you find anything wrong with this script, report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "22-06-2026"
    }
"""
import json
import logging
from collections import Counter
from typing import Callable
import re
import io
from dateutil import parser

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
    DDPCategory(
        id="html_en",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.EN,
        known_files=[
            "subscriptions.csv",
            "watch-history.html",
            "search-history.html",
            "comments.csv",
        ],
    ),
    DDPCategory(
        id="html_nl",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.NL,
        known_files=[
            "abonnementen.csv",
            "kijkgeschiedenis.html",
            "zoekgeschiedenis.html",
            "reacties.csv",
        ],
    ),
]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _parse_watch_history_html(data: io.BytesIO) -> list[dict[str, str]] | None:
    """Reads structured YouTube data in html format and parses it into a list of dictionaries,
    extracting the title, url, channel url and name, and date of each watched item."""

    result = []

    lines = data.readlines()
    lines = [line.decode("utf-8") for line in lines]

    # Pattern to filter for watch history items by looking for specific url and capturing the 
    # following text until the end of the container
    div_pattern = re.compile(
        r'(<a href="https://www\.youtube\.com/watch\?v=.+?)</div>',
    )

    # Pattern to extract the relevant fields from watch history item
    content_pattern = re.compile(
        r'<a href="(.+?)">(.+?)</a>.+?<a href="(.+?)">(.+?)</a><br>(.+?)<br>',
    )

    for line in lines:
        for match in div_pattern.finditer(line):
            div_content = match.group(1)
            content = content_pattern.search(div_content)
            if content:
                result.append({
                    "titleUrl": content.group(1), 
                    "title": content.group(2), 
                    "channelUrl": content.group(3), 
                    "channelName": content.group(4), 
                    "time": _convert_to_iso8601(content.group(5)),
                })
    return result


def _parse_search_history_html(data: io.BytesIO) -> list[dict[str, str]] | None:
    """Reads structured YouTube data in html format and parses it into a list of dictionaries,
    extracting the title, url, and date of each seach item."""

    result = []

    lines = data.readlines()
    lines = [line.decode("utf-8") for line in lines]

    # Pattern to filter for search history items by looking for specific url and capturing the 
    # following text until the end of the container
    div_pattern = re.compile(
        r'(<a href="https://www\.youtube\.com/results\?search_query=.+?)</div>',
    )
    
    # Pattern to extract the relevant fields from container contents
    content_pattern = re.compile(
        r'<a href="(.+?)">(.+?)</a><br>(.+?)<br>',
    )

    # For each line in the html file extract all div containers with a specific set of classes. 
    # Then iterate over these containers and extract the relevant fields from their contents.
    for line in lines:
        for match in div_pattern.finditer(line):
            div_content = match.group(1)
            content = content_pattern.search(div_content)
            if content:
                result.append({
                    "titleUrl": content.group(1), 
                    "title": content.group(2), 
                    "time": _convert_to_iso8601(content.group(3))
                })
    return result


def _convert_to_iso8601(timestamp):
    """Converts a time string extracted from the HTML DDP (e.g. 15 jun 2026, 20:30:41 CEST) to
    ISO8601 format, ignoring timezone abbreviations and translating Dutch month abbreviations."""
    try:
        parts = timestamp.split(' ')

        # Ignore timezone abbreviation at the end as this is not included in json either
        # and cannot be automatically parsed
        if ':' not in parts[-1]:
            parts.pop()

        # Translate month abreviations to English
        nl_month_translations = {
            'mrt': 'mar',
            'mei': 'may',
            'okt': 'oct',
            }
        for i in range(len(parts)):
            if parts[i].lower() in nl_month_translations:
                parts[i] = nl_month_translations[parts[i].lower()]

        dt = parser.parse(' '.join(parts))
        return dt.isoformat()
    except (ValueError, TypeError) as e:
        return timestamp


# ---------------------------------------------------------------------------
# Extractor functions
# ---------------------------------------------------------------------------


def watch_history_to_df(reader: ZipArchiveReader, errors: Counter, validation) -> pd.DataFrame:
    """Extract watch history from the YouTube DDP.

    In case of a JSON DDP it tries the English filename ``watch-history.json``
    first, then the Dutch ``kijkgeschiedenis.json``. In case of a HTML DPP it 
    extracts information from ``watch-history.html`` or ``kijkgeschiedenis.html`` 
    depending on the detected language of the DDP.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation results for the extracted data used to determine ddp type 
        and language.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``URL``, ``Timestamp``.
        Empty DataFrame when no matching file is found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one video the participant watched on YouTube, including the video title, URL, and timestamp.",
          "source_file": "watch-history.json, kijkgeschiedenis.json", "watch-history.html or kijkgeschiedenis.html",
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
    out = pd.DataFrame()
    if validation.current_ddp_category.ddp_filetype == DDPFiletype.JSON:
        result = None
        for filename in ("watch-history.json", "kijkgeschiedenis.json"):
            r = reader.json(filename)
            if r.found:
                result = r
                break
        if result is None or not result.found:
            return out
        d = result.data
    elif validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        if validation.current_ddp_category.language == Language.NL:
            data = reader.raw("kijkgeschiedenis.html")
        elif validation.current_ddp_category.language == Language.EN:
            data = reader.raw("watch-history.html")
        else:
            return out
        if not data.found:
            return out    
        try:
            d = _parse_watch_history_html(data.data)
            if not isinstance(d, list):
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1

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


def search_history_to_df(reader: ZipArchiveReader, errors: Counter, validation) -> pd.DataFrame:
    """Extract search history from the YouTube DDP.

    Tries the English filename ``search-history.json`` first, then the Dutch
    ``zoekgeschiedenis.json``. In case of a HTML DPP it extract information from 
    ``search-history.html`` or ``zoekgeschiedenis.html`` depending on the detected 
    language of the DDP.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation results for the extracted data used to determine ddp type 
        and language.

    Returns
    -------
    pd.DataFrame
        Columns: ``Query``, ``URL``, ``Timestamp``.
        Empty DataFrame when no matching file is found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one search query in YouTube search history.",
          "source_file": "search-history.json, zoekgeschiedenis.json, search-history.html or zoekgeschiedenis.html",
          "columns": {
            "Query": "The searched query.",
            "URL": "URL of the search query.",
            "Timestamp": "ISO 8601 timestamp of when the search was performed.",
          }
        }

    Table config::

        {
          "id": "youtube_search_history",
          "title": {
            "en": "Your search history",
            "nl": "Je zoekgeschiedenis"
          },
          "description": {
            "en": "Your search queries on YouTube with timestamps.",
            "nl": "Je zoekopdrachten op YouTube met tijdstippen."
          },
          "headers": {
            "Query": {"en": "Search query", "nl": "Zoekopdracht"},
            "URL": {"en": "URL", "nl": "URL"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          },
          "visualizations": [
            {
              "title": {
                "en": "Words in your search history",
                "nl": "Woorden in je zoekgeschiedenis"
              },
              "type": "wordcloud",
              "textColumn": "Query",
              "tokenize": true
            }
          ]
        }
    """
    out = pd.DataFrame()
    if validation.current_ddp_category.ddp_filetype == DDPFiletype.JSON:
        result = None
        for filename in ("search-history.json", "zoekgeschiedenis.json"):
            r = reader.json(filename)
            if r.found:
                result = r
                break

        if result is None or not result.found:
            return pd.DataFrame()
        d = result.data
    elif validation.current_ddp_category.ddp_filetype == DDPFiletype.HTML:
        if validation.current_ddp_category.language == Language.NL:
            data = reader.raw("zoekgeschiedenis.html")
        elif validation.current_ddp_category.language == Language.EN:
            data = reader.raw("search-history.html")
        else:
            return out
        if not data.found:
            return out    
        try:
            d = _parse_search_history_html(data.data)
            if not isinstance(d, list):
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1


    datapoints = []
    try:
        for item in d:
            datapoints.append((
                item.get("title", ""),
                item.get("titleUrl", ""),
                item.get("time", ""),
            ))
        out = pd.DataFrame(datapoints, columns=["Query", "URL", "Timestamp"])  # pyright: ignore
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
        Validation result object that is passed on to the watch history and 
        search history extractor functions in ``EXTRACTOR_REGISTRY``, and whose 
         ``archive_members`` attribute is passed to ``ZipArchiveReader``.
    """
    config = load_port_config(EXTRACTOR_REGISTRY, "youtube")
    for table in config: # Pass validation results to determine ddp type and language
        if table.extractor in [watch_history_to_df, search_history_to_df]:
            table.extractor_kwargs = {'validation': validation}
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
