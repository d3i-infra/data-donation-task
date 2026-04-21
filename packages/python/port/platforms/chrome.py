"""
Chrome

This module contains a Chrome browser history data donation flow.

Assumptions:
It handles DDPs in English and Dutch with filetype JSON.

Configuration
-------------
The ``extraction`` function is driven by ``port_config.json``.  Generate one with::

    pnpm generate-config chrome

Each extractor function carries its own table config in a ``Table config::``
JSON block inside its docstring.  The generator reads those blocks and
assembles the JSON file.

Platform info::

    {
        "name": "Chrome",
        "filetypes": ["json"],
        "languages": ["en", "nl"],
        "description": "Handles DDPs in English and Dutch. Both language filenames are tried automatically. These data donation flows have not been tested yet, if you find anything wrong with them report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "not yet implemented"
    }
"""
from collections import Counter
from html.parser import HTMLParser
import logging
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
            "Autofill.json",
            "Bookmarks.html",
            "BrowserHistory.json",
            "History.json",
            "Device Information.json",
            "Dictionary.csv",
            "Extensions.json",
            "Omnibox.json",
            "OS Settings.json",
            "ReadingList.html",
            "SearchEngines.json",
            "SyncSettings.json",
        ],
    ),
    DDPCategory(
        id="json_nl",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.NL,
        known_files=[
            "Adressen en meer.json",
            "Bookmarks.html",
            "Geschiedenis.json",
            "Leeslijst.html",
            "Woordenboek.csv",
            "Apparaatgegevens.json",
            "Extensies.json",
            "Instellingen.json",
            "OS-instellingen.json",
        ],
    ),
]


class _BookmarkParser(HTMLParser):
    """Minimal HTML parser to extract <a> tags from bookmarks HTML."""

    def __init__(self):
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._current_href: str | None = None
        self._current_text = ""

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            attrs_dict = dict(attrs)
            self._current_href = attrs_dict.get("href", "")
            self._current_text = ""

    def handle_data(self, data):
        if self._current_href is not None:
            self._current_text = data

    def handle_endtag(self, tag):
        if tag == "a" and self._current_href is not None:
            self.links.append((self._current_text, self._current_href))
            self._current_href = None


def browser_history_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract browser history from the Chrome DDP.

    Tries ``History.json``, ``BrowserHistory.json``, and ``Geschiedenis.json``
    (NL) in order.

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
        Columns: ``Title``, ``URL``, ``Transition``, ``Date``.
        Capped at 10 000 most recent entries.
        Empty DataFrame when no matching file is found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one page visit in the participant's Chrome browser history, including the page title, URL, transition type, and visit date.",
          "source_file": "BrowserHistory.json, History.json, or Geschiedenis.json",
          "columns": {
            "Title": "Title of the visited web page.",
            "URL": "URL of the visited web page.",
            "Transition": "Page transition type (e.g. LINK, TYPED, RELOAD).",
            "Date": "ISO 8601 timestamp of the visit."
          }
        }

    Table config::

        {
          "id": "chrome_browser_history",
          "title": {
            "en": "Chrome browser history",
            "nl": "Chrome browsergeschiedenis"
          },
          "description": {
            "en": "The websites you have visited using Chrome",
            "nl": "De websites die u heeft bezocht met Chrome"
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "URL": {"en": "URL", "nl": "URL"},
            "Transition": {"en": "Transition type", "nl": "Transitietype"},
            "Date": {"en": "Date", "nl": "Datum"}
          },
          "visualizations": [
            {
              "title": {"en": "Most visited websites", "nl": "Meest bezochte websites"},
              "type": "wordcloud",
              "textColumn": "URL",
              "tokenize": false
            }
          ]
        }
    """
    d: dict | list = {}
    for filename in ("Geschiedenis.json", "BrowserHistory.json", "History.json"):
        result = reader.json(filename)
        if result.found:
            d = result.data
            break

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["Browser History"]  # type: ignore
        for item in items:
            datapoints.append((
                item.get("title", None),
                item.get("url", None),
                item.get("page_transition_qualifier") or item.get("page_transition"),
                eh.epoch_to_iso(item.get("time_usec", 0) / 1_000_000, errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "URL", "Transition", "Date"])
        out = out.sort_values("Date", ascending=False).head(10_000).reset_index(drop=True)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def bookmarks_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract bookmarks from the Chrome DDP.

    Reads ``Bookmarks.html`` and parses all ``<a>`` tags.

    Parameters
    ----------
    reader:
        Archive reader used to load files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Bookmark``, ``URL``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one bookmark saved in Chrome, including the bookmark label and URL.",
          "source_file": "Bookmarks.html",
          "columns": {
            "Bookmark": "Display name of the bookmarked page.",
            "URL": "URL of the bookmarked page."
          }
        }

    Table config::

        {
          "id": "chrome_bookmarks",
          "title": {
            "en": "Chrome bookmarks",
            "nl": "Chrome bladwijzers"
          },
          "description": {
            "en": "Websites you have bookmarked in Chrome",
            "nl": "Websites die u heeft opgeslagen als bladwijzer in Chrome"
          },
          "headers": {
            "Bookmark": {"en": "Bookmark", "nl": "Bladwijzer"},
            "URL": {"en": "URL", "nl": "URL"}
          }
        }
    """
    result = reader.raw("Bookmarks.html")
    if not result.found:
        return pd.DataFrame()
    out = pd.DataFrame()

    try:
        html_content = result.data.read().decode("utf-8", errors="replace")
        parser = _BookmarkParser()
        parser.feed(html_content)
        out = pd.DataFrame(parser.links, columns=["Bookmark", "URL"])
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def omnibox_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract omnibox (address bar) typed URL history from the Chrome DDP.

    Tries ``Omnibox.json`` then ``History.json``.

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
        Columns: ``Title``, ``Number of visits``, ``URL``.
        Sorted by visit count descending.
        Empty DataFrame when no matching file is found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one URL the participant typed directly into the Chrome address bar, including the visit count.",
          "source_file": "Omnibox.json or History.json",
          "columns": {
            "Title": "Title of the page at the typed URL.",
            "Number of visits": "Total number of times this URL was typed.",
            "URL": "The URL that was typed."
          }
        }

    Table config::

        {
          "id": "chrome_omnibox",
          "title": {
            "en": "Chrome address bar history",
            "nl": "Chrome adresbalk geschiedenis"
          },
          "description": {
            "en": "URLs you have typed directly into the Chrome address bar",
            "nl": "URLs die u direct in de Chrome adresbalk heeft ingevoerd"
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Number of visits": {"en": "Number of visits", "nl": "Aantal bezoeken"},
            "URL": {"en": "URL", "nl": "URL"}
          }
        }
    """
    d: dict | list = {}
    for filename in ("Omnibox.json", "History.json"):
        result = reader.json(filename)
        if result.found:
            d = result.data
            break

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["Typed Url"]  # type: ignore
        for item in items:
            datapoints.append((
                item.get("title", None),
                len(item.get("visits", [])),
                item.get("url", None),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Number of visits", "URL"])
        out = out.sort_values(by="Number of visits", ascending=False).reset_index(drop=True)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


# ---------------------------------------------------------------------------
# Extractor registry & platform info
# ---------------------------------------------------------------------------

#: Mapping from the string names used in port_config.json to actual extractor functions.
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "browser_history_to_df": browser_history_to_df,
    "bookmarks_to_df": bookmarks_to_df,
    "omnibox_to_df": omnibox_to_df,
}


# ---------------------------------------------------------------------------
# Main extraction & flow
# ---------------------------------------------------------------------------

def extraction(chrome_zip: str, validation) -> ExtractionResult:
    """Extract data from a Chrome DDP zip and return consent-form tables.

    Parameters
    ----------
    chrome_zip:
        Path to the Chrome DDP zip archive on disk.
    validation:
        Validation result object whose ``archive_members`` attribute is passed
        to ``ZipArchiveReader``.
    """
    config = load_port_config(EXTRACTOR_REGISTRY)
    errors: Counter = Counter()
    reader = ZipArchiveReader(chrome_zip, validation.archive_members, errors)
    return run_extraction(reader, errors, config)


class ChromeFlow(FlowBuilder):
    """Flow implementation for the Chrome data donation study."""

    def __init__(self, session_id: str):
        super().__init__(session_id, "Chrome")

    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)

    def extract_data(self, file_value, validation):
        return extraction(file_value, validation)


def process(session_id):
    flow = ChromeFlow(session_id)
    return flow.start_flow()
