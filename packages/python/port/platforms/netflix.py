"""
Netflix

This module provides an example flow of a Netflix data donation study.

Assumptions:
It handles DDPs in the English language with filetype CSV.
Netflix DDPs may have files nested under a numeric user ID prefix directory.

Configuration
-------------
The ``extraction`` function is driven by ``port_config.json``.  Generate one with::

    pnpm generate-config netflix

Each extractor function carries its own table config in a ``Table config::``
JSON block inside its docstring.  The generator reads those blocks and
assembles the JSON file.

Platform info::

    {
        "name": "Netflix",
        "filetypes": ["csv"],
        "languages": ["en", "nl"],
        "description": "Handles DDPs in English. Supports multi-profile DDPs; the participant selects their profile at runtime. These data donation flows have not been tested yet, if you find anything wrong with them report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "not yet implemented"
    }

Note: Netflix extractors receive the selected profile name via
``extractor_kwargs["selected_user"]``.  The ``extraction()`` function injects
the runtime-selected value into each ``TableConfig`` before calling
``run_extraction``.
"""
import logging
from collections import Counter
from typing import Callable

import pandas as pd

from port.api.props import Translatable
import port.helpers.extraction_helpers as eh
import port.helpers.validate as validate
import port.helpers.port_helpers as ph
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
        id="csv",
        ddp_filetype=DDPFiletype.CSV,
        language=Language.EN,
        known_files=[
            "MyList.csv", "ViewingActivity.csv", "SearchHistory.csv",
            "IndicatedPreferences.csv", "PlaybackRelatedEvents.csv",
            "InteractiveTitles.csv", "Ratings.csv", "GamePlaySession.csv",
            "IpAddressesLogin.csv", "IpAddressesAccountCreation.txt",
            "IpAddressesStreaming.csv", "Additional Information.pdf",
            "MessagesSentByNetflix.csv", "AccountDetails.csv",
            "ProductCancellationSurvey.txt", "CSContact.txt",
            "ChatTranscripts.txt", "Cover Sheet.pdf", "Devices.csv",
            "ParentalControlsRestrictedTitles.txt", "AvatarHistory.csv",
            "Profiles.csv", "Clickstream.csv", "BillingHistory.csv",
            "AccessAndDevices.csv", "ExtraMembers.txt",
        ]
    )
]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def extract_users(reader: ZipArchiveReader) -> list[str]:
    """Extract all profile names from Profiles.csv (first column).

    Falls back to ViewingActivity.csv if Profiles.csv is not available.
    Uses column position rather than name to handle different DDP languages.
    """
    out: list[str] = []

    result = reader.csv("Profiles.csv")
    df = result.data if result.found else pd.DataFrame()

    if df.empty:
        result = reader.csv("ViewingActivity.csv")
        df = result.data if result.found else pd.DataFrame()

    try:
        if not df.empty:
            if "Profile Name" in df.columns:
                out = df["Profile Name"].unique().tolist()
            else:
                out = df[df.columns[0]].unique().tolist()
            out.sort()
    except Exception as e:
        logger.error("Cannot extract users: %s", e)
        reader.errors[type(e).__name__] += 1
    return out


def keep_user(df: pd.DataFrame, selected_user: str) -> pd.DataFrame:
    """Keep only rows where the profile name column matches selected_user."""
    try:
        if "Profile Name" in df.columns:
            df = df.loc[df["Profile Name"] == selected_user].reset_index(drop=True)
        else:
            for col in df.columns:
                if selected_user in df[col].values:
                    df = df.loc[df[col] == selected_user].reset_index(drop=True)
                    break
    except Exception as e:
        logger.info(e)
    return df


def netflix_to_df(reader: ZipArchiveReader, file_name: str, selected_user: str) -> pd.DataFrame:
    """Load a Netflix CSV, filter to selected user."""
    result = reader.csv(file_name)
    if not result.found:
        return pd.DataFrame()
    return keep_user(result.data, selected_user)


# ---------------------------------------------------------------------------
# Per-table extraction functions
# ---------------------------------------------------------------------------

def ratings_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    selected_user: str = "",
) -> pd.DataFrame:
    """Extract Netflix ratings — title, thumbs value, timestamp.

    Parameters
    ----------
    reader:
        Archive reader used to load CSV files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    selected_user:
        Profile name to filter rows by.  Passed via ``extractor_kwargs`` from
        ``port_config.json`` and injected at runtime by ``extraction()``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title Name``, ``Thumbs Value``, ``Event Utc Ts``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one title the participant rated on Netflix, including the rating value and timestamp.",
          "source_file": "Ratings.csv",
          "columns": {
            "Title Name": "Name of the rated Netflix title.",
            "Thumbs Value": "Thumbs up or thumbs down value given by the participant.",
            "Event Utc Ts": "ISO 8601 timestamp of when the rating was given."
          }
        }

    Table config::

        {
          "id": "netflix_ratings",
          "title": {"en": "Your ratings on Netflix", "nl": "Uw beoordelingen op Netflix"},
          "description": {
            "en": "Titles you have rated on Netflix.",
            "nl": "Titels die u op Netflix heeft beoordeeld."
          },
          "headers": {
            "Title Name": {"en": "Title", "nl": "Titel"},
            "Thumbs Value": {"en": "Thumbs value", "nl": "Aantal duimpjes omhoog"},
            "Event Utc Ts": {"en": "Date", "nl": "Datum en tijd"}
          },
          "visualizations": [
            {
              "title": {
                "en": "Titles rated by thumbs value",
                "nl": "Beoordeelde titels op basis van duimpjes"
              },
              "type": "wordcloud",
              "textColumn": "Title Name",
              "valueColumn": "Thumbs Value"
            }
          ]
        }
    """
    columns_to_keep = ["Title Name", "Thumbs Value", "Event Utc Ts"]
    df = netflix_to_df(reader, "Ratings.csv", selected_user)
    out = pd.DataFrame()
    try:
        if not df.empty:
            out = pd.DataFrame(df[columns_to_keep])
    except Exception as e:
        logger.error("Data extraction error: %s", e)
        errors[type(e).__name__] += 1
    return out


def time_string_to_hours(time_str: str) -> float:
    try:
        hours, minutes, seconds = map(int, time_str.split(':'))
        total_hours = (hours * 3600 + minutes * 60 + seconds) / 3600
    except Exception:
        return 0.0
    return round(total_hours, 3)


def viewing_activity_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    selected_user: str = "",
) -> pd.DataFrame:
    """Extract Netflix viewing activity — start time, duration, title, type.

    Parameters
    ----------
    reader:
        Archive reader used to load CSV files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    selected_user:
        Profile name to filter rows by.  Passed via ``extractor_kwargs`` from
        ``port_config.json`` and injected at runtime by ``extraction()``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Start Time``, ``Duration``, ``Title``, ``Supplemental Video Type``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one viewing session on Netflix, including the title watched, start time, and duration in hours.",
          "source_file": "ViewingActivity.csv",
          "columns": {
            "Start Time": "ISO 8601 timestamp of when the viewing session started.",
            "Duration": "Duration of the viewing session in hours.",
            "Title": "Name of the Netflix title watched.",
            "Supplemental Video Type": "Type of supplemental video (e.g. trailer), if applicable."
          }
        }

    Table config::

        {
          "id": "netflix_viewing_activity",
          "title": {"en": "What you watched", "nl": "Wat u heeft gekeken"},
          "description": {
            "en": "This table shows what titles you watched, when, and for how long.",
            "nl": "Deze tabel toont welke titels u heeft gekeken, wanneer, en hoe lang."
          },
          "headers": {
            "Start Time": {"en": "Start time", "nl": "Starttijd"},
            "Duration": {"en": "Hours watched", "nl": "Aantal uur gekeken"},
            "Title": {"en": "Title", "nl": "Titel"},
            "Supplemental Video Type": {"en": "Type", "nl": "Aanvullende informatie"}
          },
          "visualizations": [
            {
              "title": {
                "en": "Total hours watched per month",
                "nl": "Totaal aantal uren gekeken per maand"
              },
              "type": "area",
              "group": {"column": "Start Time", "dateFormat": "month", "label": "Month"},
              "values": [{"column": "Duration", "aggregate": "sum"}]
            },
            {
              "title": {
                "en": "Total hours watched by hour of the day",
                "nl": "Totaal aantal uur gekeken per uur van de dag"
              },
              "type": "bar",
              "group": {"column": "Start Time", "dateFormat": "hour_cycle"},
              "values": [{"column": "Duration", "aggregate": "sum"}]
            }
          ]
        }
    """
    columns_to_keep = ["Start Time", "Duration", "Title", "Supplemental Video Type"]
    df = netflix_to_df(reader, "ViewingActivity.csv", selected_user)
    remove_values = ["TEASER_TRAILER", "HOOK", "TRAILER", "CINEMAGRAPH"]
    out = pd.DataFrame()
    try:
        if not df.empty:
            out = pd.DataFrame(df[columns_to_keep])
            mask = out["Supplemental Video Type"].isin(remove_values)
            out = out[~mask].reset_index(drop=True)
            out["Duration"] = out["Duration"].apply(time_string_to_hours)
            out = out.sort_values(by="Start Time", ascending=True).reset_index(drop=True)
    except Exception as e:
        logger.error("Data extraction error: %s", e)
        errors[type(e).__name__] += 1
    return out


def search_history_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    selected_user: str = "",
) -> pd.DataFrame:
    """Extract Netflix search history — query, displayed result, timestamp.

    Parameters
    ----------
    reader:
        Archive reader used to load CSV files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    selected_user:
        Profile name to filter rows by.  Passed via ``extractor_kwargs`` from
        ``port_config.json`` and injected at runtime by ``extraction()``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Query Typed``, ``Displayed Name``, ``Utc Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one search the participant performed on Netflix.",
          "source_file": "SearchHistory.csv",
          "columns": {
            "Query Typed": "The search query the participant typed.",
            "Displayed Name": "The result title that was displayed.",
            "Utc Timestamp": "ISO 8601 timestamp of when the search was performed."
          }
        }

    Table config::

        {
          "id": "netflix_search_history",
          "title": {
            "en": "Your search history on Netflix",
            "nl": "Uw zoekgeschiedenis op Netflix"
          },
          "description": {
            "en": "Searches you have performed on Netflix.",
            "nl": "Zoekopdrachten die u op Netflix heeft uitgevoerd."
          },
          "headers": {
            "Query Typed": {"en": "Search query", "nl": "Zoekterm"},
            "Displayed Name": {"en": "Result shown", "nl": "Weergegeven resultaat"},
            "Utc Timestamp": {"en": "Date", "nl": "Datum en tijd"}
          },
          "visualizations": [
            {
              "title": {
                "en": "Most searched terms",
                "nl": "Meest gezochte termen"
              },
              "type": "wordcloud",
              "textColumn": "Query Typed",
              "tokenize": false
            }
          ]
        }
    """
    df = netflix_to_df(reader, "SearchHistory.csv", selected_user)
    out = pd.DataFrame()
    try:
        if not df.empty:
            columns_to_keep = [c for c in ["Query Typed", "Displayed Name", "Utc Timestamp"] if c in df.columns]
            out = pd.DataFrame(df[columns_to_keep])
            if "Utc Timestamp" in out.columns:
                out = out.sort_values(by="Utc Timestamp", ascending=False).reset_index(drop=True)
    except Exception as e:
        logger.error("Data extraction error: %s", e)
        errors[type(e).__name__] += 1
    return out


# ---------------------------------------------------------------------------
# Extractor registry & platform info
# ---------------------------------------------------------------------------

#: Mapping from the string names used in port_config.json to actual extractor functions.
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "ratings_to_df": ratings_to_df,
    "viewing_activity_to_df": viewing_activity_to_df,
    "search_history_to_df": search_history_to_df,
}


# ---------------------------------------------------------------------------
# Main extraction & flow
# ---------------------------------------------------------------------------

def extraction(reader: ZipArchiveReader, selected_user: str) -> ExtractionResult:
    """Extract data from a Netflix DDP zip and return consent-form tables.

    Loads ``port_config.json``, injects the runtime-selected ``selected_user``
    into each table's ``extractor_kwargs``, then delegates to
    ``run_extraction``.

    Parameters
    ----------
    reader:
        Initialised archive reader for the Netflix DDP zip.
    selected_user:
        Profile name chosen by the participant during the flow.
    """
    config = load_port_config(EXTRACTOR_REGISTRY)
    for table_cfg in config:
        table_cfg.extractor_kwargs["selected_user"] = selected_user
    return run_extraction(reader, reader.errors, config)


class NetflixFlow(FlowBuilder):
    """Flow implementation for the Netflix data donation study."""

    def __init__(self, session_id: str):
        super().__init__(session_id, "Netflix")

    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)

    def extract_data(self, file, validation):
        errors: Counter = Counter()
        reader = ZipArchiveReader(file, validation.archive_members, errors)
        selected_user = ""
        users = extract_users(reader)

        if len(users) == 1:
            selected_user = users[0]
            return extraction(reader, selected_user)
        elif len(users) > 1:
            title = Translatable({
                "en": "Select your Netflix profile name",
                "nl": "Kies jouw Netflix profielnaam",
            })
            empty_text = Translatable({"en": "", "nl": ""})
            radio_prompt = ph.generate_radio_prompt(title, empty_text, users)
            selection = yield ph.render_page(empty_text, radio_prompt)
            selected_user = selection.value
            return extraction(reader, selected_user)


def process(session_id):
    flow = NetflixFlow(session_id)
    return flow.start_flow()
