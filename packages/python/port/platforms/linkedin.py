"""
LinkedIn

This module contains an example flow of a LinkedIn data donation study

Assumptions:
It handles DDPs in the english language with filetype CSV.

Configuration
-------------
The ``extraction`` function is driven by ``port_config.json``.  Generate one with::

    pnpm generate-config linkedin

Each extractor function carries its own table config in a ``Table config::``
JSON block inside its docstring.  The generator reads those blocks and
assembles the JSON file.

Platform info::

    {
        "name": "LinkedIn",
        "filetypes": ["csv"],
        "languages": ["en", "nl"],
        "description": "Handles DDPs in English. These data donation flows have not been tested yet, if you find anything wrong with them report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "not yet implemented"
    }
"""

import logging
from collections import Counter
import io
import re
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
        id="csv_en",
        ddp_filetype=DDPFiletype.CSV,
        language=Language.EN,
        known_files=[
            "Ad_Targeting.csv",
            "Endorsement_Given_Info.csv",
            "Member_Follows.csv",
            "Recommendations_Given.csv",
            "Company Follows.csv",
            "Endorsement_Received_Info.csv",
            "messages.csv",
            "Registration.csv",
            "Connections.csv",
            "Inferences_about_you.csv",
            "PhoneNumbers.csv",
            "Rich Media.csv",
            "Contacts.csv",
            "Invitations.csv",
            "Positions.csv",
            "Skills.csv",
            "Education.csv",
            "Profile.csv",
            "Votes.csv",
            "Email Addresses.csv",
            "Learning.csv",
            "Reactions.csv",
            "LAN Ads Engagement.csv",
        ]
    ),
]

def strip_notes(b: io.BytesIO) -> io.BytesIO:
    """
    Strip notes LinkedIn puts at the start of CSV files
    """

    try:
        pattern = re.compile(rb'^(.*?)\n\n', re.DOTALL)
        out = io.BytesIO(pattern.sub(b'', b.read()))
    except Exception:
        out = b

    return out


def company_follows_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the companies the participant follows on LinkedIn.

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
        Columns as returned by ``Company Follows.csv``: typically
        ``Organization``, ``Followed On``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one company the participant follows on LinkedIn.",
          "source_file": "Company Follows.csv",
          "columns": {
            "Organization": "Name of the followed company.",
            "Followed On": "Date on which the participant started following the company."
          }
        }

    Table config::

        {
          "id": "linked_in_company_follows",
          "title": {"en": "Companies you follow", "nl": "Bedrijven die je volgt"},
          "description": {
            "en": "List of companies you are following on LinkedIn",
            "nl": "Lijst van bedrijven die je volgt op LinkedIn"
          },
          "headers": {
            "Organization": {"en": "Organization", "nl": "Organisatie"},
            "Followed On": {"en": "Followed On", "nl": "Gevolgd op"}
          }
        }
    """
    result = reader.csv("Company Follows.csv")
    if not result.found:
        return pd.DataFrame()
    return result.data


def member_follows_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the LinkedIn members the participant follows.

    Strips the introductory notes block LinkedIn prepends to the CSV.

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
        Columns as returned by ``Member_Follows.csv`` after stripping notes.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one LinkedIn member the participant follows.",
          "source_file": "Member_Follows.csv",
          "columns": {
            "To": "Name or identifier of the followed member.",
            "To Name": "Display name of the followed member."
          }
        }

    Table config::

        {
          "id": "linkedin_member_follows",
          "title": {"en": "Members you follow", "nl": "Leden die je volgt"},
          "description": {
            "en": "List of LinkedIn members you are following",
            "nl": "Lijst van LinkedIn-leden die je volgt"
          },
          "headers": {
            "To": {"en": "To", "nl": "Aan"},
            "To Name": {"en": "To Name", "nl": "Naam"}
          }
        }
    """
    result = reader.raw("Member_Follows.csv")
    if not result.found:
        return pd.DataFrame()
    b = strip_notes(result.data)
    df = eh.read_csv_from_bytes_to_df(b)
    return df


def connections_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the participant's LinkedIn connections.

    Strips the introductory notes block LinkedIn prepends to the CSV.

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
        Columns: ``First Name``, ``Last Name``, ``Email Address``, ``Company``,
        ``Position``, ``Connected On``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one first-degree LinkedIn connection of the participant.",
          "source_file": "Connections.csv",
          "columns": {
            "First Name": "First name of the connection.",
            "Last Name": "Last name of the connection.",
            "Email Address": "Email address of the connection, if shared.",
            "Company": "Current employer of the connection.",
            "Position": "Current job title of the connection.",
            "Connected On": "Date on which the connection was established."
          }
        }

    Table config::

        {
          "id": "linkedin_connections",
          "title": {"en": "Your LinkedIn connections", "nl": "Je LinkedIn-connecties"},
          "description": {
            "en": "List of people you are connected with on LinkedIn",
            "nl": "Lijst van mensen met wie je verbonden bent op LinkedIn"
          },
          "headers": {
            "First Name": {"en": "First Name", "nl": "Voornaam"},
            "Last Name": {"en": "Last Name", "nl": "Achternaam"},
            "Email Address": {"en": "Email Address", "nl": "E-mailadres"},
            "Company": {"en": "Company", "nl": "Bedrijf"},
            "Position": {"en": "Position", "nl": "Functie"},
            "Connected On": {"en": "Connected On", "nl": "Verbonden op"}
          }
        }
    """
    result = reader.raw("Connections.csv")
    if not result.found:
        return pd.DataFrame()
    b = strip_notes(result.data)
    df = eh.read_csv_from_bytes_to_df(b)
    return df


def reactions_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the participant's reactions on LinkedIn.

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
        Columns as returned by ``Reactions.csv``: typically ``Date``, ``Type``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one reaction the participant gave to a post or content on LinkedIn.",
          "source_file": "Reactions.csv",
          "columns": {
            "Date": "Date of the reaction.",
            "Type": "Type of reaction (e.g. Like, Celebrate, Support)."
          }
        }

    Table config::

        {
          "id": "linkedin_reactions",
          "title": {"en": "Your reactions on LinkedIn", "nl": "Je reacties op LinkedIn"},
          "description": {
            "en": "Record of your reactions to posts and content on LinkedIn",
            "nl": "Overzicht van je reacties op berichten en content op LinkedIn"
          },
          "headers": {
            "Date": {"en": "Date", "nl": "Datum"},
            "Type": {"en": "Type", "nl": "Type"}
          },
          "visualizations": [
            {
              "title": {
                "en": "The type of reactions you put under posts on LinkedIn",
                "nl": "De soorten reacties die je plaatst onder berichten op LinkedIn"
              },
              "type": "wordcloud",
              "textColumn": "Type",
              "tokenize": true
            }
          ]
        }
    """
    result = reader.csv("Reactions.csv")
    if not result.found:
        return pd.DataFrame()
    return result.data


def ads_clicked_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the advertisements the participant clicked on LinkedIn.

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
        Columns as returned by ``Ads Clicked.csv``: typically
        ``Ad clicked Date``, ``Ad Title/Id``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one advertisement the participant clicked on LinkedIn.",
          "source_file": "Ads Clicked.csv",
          "columns": {
            "Ad clicked Date": "Date on which the ad was clicked.",
            "Ad Title/Id": "Title or numeric ID of the clicked advertisement."
          }
        }

    Table config::

        {
          "id": "linkedin_ads_clicked",
          "title": {"en": "Ads you clicked on", "nl": "Advertenties waarop je hebt geklikt"},
          "description": {
            "en": "Record of advertisements you have clicked on while using LinkedIn. Note: LinkedIn only provides numeric ad IDs, not ad titles or descriptions.",
            "nl": "Overzicht van advertenties waarop je hebt geklikt tijdens het gebruik van LinkedIn. Let op: LinkedIn geeft alleen numerieke advertentie-ID's, geen titels of beschrijvingen."
          },
          "headers": {
            "Ad clicked Date": {"en": "Ad clicked Date", "nl": "Advertentiedatum"},
            "Ad Title/Id": {"en": "Ad Title/Id", "nl": "Advertentietitel/id"}
          }
        }
    """
    result = reader.csv("Ads Clicked.csv")
    if not result.found:
        return pd.DataFrame()
    return result.data


def search_queries_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the participant's search queries on LinkedIn.

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
        Columns as returned by ``SearchQueries.csv``: typically
        ``Time``, ``Search Query``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one search query the participant performed on LinkedIn.",
          "source_file": "SearchQueries.csv",
          "columns": {
            "Time": "Timestamp of when the search was performed.",
            "Search Query": "The search term entered by the participant."
          }
        }

    Table config::

        {
          "id": "linkedin_search_queries",
          "title": {"en": "Your search queries on LinkedIn", "nl": "Je zoekopdrachten op LinkedIn"},
          "description": {
            "en": "Terms and phrases you've searched for on LinkedIn",
            "nl": "Termen en zinnen waarnaar je hebt gezocht op LinkedIn"
          },
          "headers": {
            "Time": {"en": "Time", "nl": "Tijd"},
            "Search Query": {"en": "Search Query", "nl": "Zoekterm"}
          },
          "visualizations": [
            {
              "title": {
                "en": "What you searched for on LinkedIn",
                "nl": "Waar je naar hebt gezocht op LinkedIn"
              },
              "type": "wordcloud",
              "textColumn": "Search Query",
              "tokenize": true
            }
          ]
        }
    """
    result = reader.csv("SearchQueries.csv")
    if not result.found:
        return pd.DataFrame()
    return result.data


def shares_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the posts the participant shared on LinkedIn.

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
        Columns as returned by ``Shares.csv``: typically ``Date``,
        ``ShareLink``, ``ShareCommentary``, ``SharedUrl``, ``MediaUrl``,
        ``Visibility``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one post the participant shared on LinkedIn.",
          "source_file": "Shares.csv",
          "columns": {
            "Date": "Date of the share.",
            "ShareLink": "Link to the share.",
            "ShareCommentary": "Text commentary added when sharing.",
            "SharedUrl": "URL of the content that was shared.",
            "MediaUrl": "URL of any media attached to the share.",
            "Visibility": "Visibility setting of the share (e.g. PUBLIC, CONNECTIONS)."
          }
        }

    Table config::

        {
          "id": "linkedin_shares",
          "title": {"en": "Posts you shared on LinkedIn", "nl": "Berichten die je hebt gedeeld op LinkedIn"},
          "description": {
            "en": "Content you've shared with your network on LinkedIn",
            "nl": "Content die je hebt gedeeld met je netwerk op LinkedIn"
          },
          "headers": {
            "Date": {"en": "Date", "nl": "Datum"},
            "ShareLink": {"en": "ShareLink", "nl": "Gedeelde link"},
            "ShareCommentary": {"en": "ShareCommentary", "nl": "Gedeelde tekst"},
            "SharedUrl": {"en": "SharedUrl", "nl": "Gedeelde URL"},
            "MediaUrl": {"en": "MediaUrl", "nl": "Media-URL"},
            "Visibility": {"en": "Visibility", "nl": "Zichtbaarheid"}
          }
        }
    """
    result = reader.csv("Shares.csv")
    if not result.found:
        return pd.DataFrame()
    return result.data


def comments_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the participant's comments on LinkedIn.

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
        Columns as returned by ``Comments.csv``: typically ``Date``,
        ``Message``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one comment the participant posted on LinkedIn content.",
          "source_file": "Comments.csv",
          "columns": {
            "Date": "Date of the comment.",
            "Message": "Text of the comment."
          }
        }

    Table config::

        {
          "id": "linkedin_comments",
          "title": {"en": "Your comments on LinkedIn", "nl": "Je reacties op LinkedIn"},
          "description": {
            "en": "Comments you've posted on LinkedIn content",
            "nl": "Reacties die je hebt geplaatst op LinkedIn-content"
          },
          "headers": {
            "Date": {"en": "Date", "nl": "Datum"},
            "Message": {"en": "Message", "nl": "Bericht"}
          },
          "visualizations": [
            {
              "title": {
                "en": "Words in your comments",
                "nl": "Woorden in je reacties"
              },
              "type": "wordcloud",
              "textColumn": "Message",
              "tokenize": true
            }
          ]
        }
    """
    result = reader.csv("Comments.csv")
    if not result.found:
        return pd.DataFrame()
    return result.data


# ---------------------------------------------------------------------------
# Extractor registry & platform info
# ---------------------------------------------------------------------------

#: Mapping from the string names used in port_config.json to actual extractor functions.
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "ads_clicked_to_df": ads_clicked_to_df,
    "comments_to_df": comments_to_df,
    "company_follows_to_df": company_follows_to_df,
    "shares_to_df": shares_to_df,
    "reactions_to_df": reactions_to_df,
    "connections_to_df": connections_to_df,
    "search_queries_to_df": search_queries_to_df,
    "member_follows_to_df": member_follows_to_df,
}


# ---------------------------------------------------------------------------
# Main extraction & flow
# ---------------------------------------------------------------------------

def extraction(linkedin_zip: str, validation: validate.ValidateInput) -> ExtractionResult:
    """Extract data from a LinkedIn DDP zip and return consent-form tables.

    Parameters
    ----------
    linkedin_zip:
        Path to the LinkedIn DDP zip archive on disk.
    validation:
        Validation result object whose ``archive_members`` attribute is passed
        to ``ZipArchiveReader``.
    """
    config = load_port_config(EXTRACTOR_REGISTRY)
    errors: Counter = Counter()
    reader = ZipArchiveReader(linkedin_zip, validation.archive_members, errors)
    return run_extraction(reader, errors, config)


class LinkedInFlow(FlowBuilder):
    """Flow implementation for the LinkedIn data donation study."""

    def __init__(self, session_id: str):
        super().__init__(session_id, "LinkedIn")

    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)

    def extract_data(self, file_value, validation):
        return extraction(file_value, validation)


def process(session_id):
    flow = LinkedInFlow(session_id)
    return flow.start_flow()
