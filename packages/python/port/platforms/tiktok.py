"""
TikTok

This module contains an example flow of a TikTok data donation study.

Assumptions:
It handles DDPs in the English or Dutch language, either with filetype JSON or a compressed folder with TXT files.

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
        "filetypes": ["json", "txt"],
        "languages": ["en", "nl"],
        "description": "Handles DDPs in English and Dutch. For the JSON format, both user_data.json and user_data_tiktok.json are tried automatically. English DDPs in TXT format have not yet been tested. If you find anything wrong with the data donation flows, please report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "15-06-2026"
    }
"""

from csv import reader
import io
import logging
from collections import Counter
from typing import Any, Callable

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
            "user_data.json",
            "user_data_tiktok.json",
        ],
    ),
    DDPCategory(
        id="txt_nl",
        ddp_filetype=DDPFiletype.TXT,
        language=Language.NL,
        known_files=[
            "Locatierecensies.txt","Instellingen voor LIVE bekijken.txt",
            "Geschiedenis van LIVE gaan.txt","Reactie op livestream.txt",
            "Geschiedenis van LIVE bekijken.txt","Instellingen voor LIVE gaan.txt",
            "Geschiedenis van Muntaankopen.txt","Transactiegeschiedenis.txt",
            "Reacties.txt","Informatie over huidige betaling.txt",
            "Geschiedenis van klantenservice.txt","Bestelgeschiedenis.txt",
            "Favoriet item.txt","Communicatie met winkels.txt","Productrecensies.txt",
            "Vouchers.txt","Geschiedenis van bladeren door producten.txt",
            "Geschiedenis van bestelkwesties.txt",
            "Geschiedenis van retourzendingen en terugbetalingen.txt",
            "Opgeslagen adresgegevens.txt","Winkelwagenlijst.txt",
            "Favoriete films en tv-programma's.txt","Favoriete video's.txt",
            "Favoriete hashtags.txt","Favoriete afspeellijsten.txt",
            "Favoriete effecten.txt","Likelijst.txt","Favoriete geluiden.txt",
            "Favoriete collecties.txt","Favoriete plaatsen.txt",
            "Favoriete reacties.txt","Volger.txt","Informatie van derden.txt",
            "Volgend.txt","Blokkeringslijst.txt","AI-moji.txt","Instellingen.txt",
            "Profielweergaven.txt","Automatisch invullen.txt","Profielinformatie.txt",
            "Inloggeschiedenis.txt","Activiteit buiten TikTok.txt",
            "Herplaatsingen.txt","Donatie.txt","Samenvatting van activiteit.txt",
            "Fondsenwerving.txt","Geschiedenis van advertentielinks.txt","Hashtag.txt",
            "Stickers.txt","Meest recente locatiegegevens.txt","Aankopen.txt",
            "Advertentie-interesses.txt","Reacties op direct formulier-advertenties.txt",
            "Geschiedenis delen.txt","Status.txt","Kijkgeschiedenis.txt",
            "Zoekopdrachten.txt","Groepschat.txt","Berichten.txt",
            "Onlangs verwijderde berichten.txt","Directe berichten.txt",
        ],
    ),
    DDPCategory(
        id="txt_en",
        ddp_filetype=DDPFiletype.TXT,
        language=Language.EN,
        known_files=[
            "Comments.txt","Recently Deleted Posts.txt","Posts.txt","Favorite Videos.txt",
            "Like List.txt","Favorite Sounds.txt","Favorite HashTags.txt",
            "Favorite Places.txt","Favorite Effects.txt","Favorite Comments.txt",
            "Favorite Collections.txt","Searches.txt","Ad Interests.txt",
            "Most Recent Location Data.txt","Activity Summary.txt","Watch History.txt",
            "Off TikTok Activity.txt","Donation.txt","Share History.txt","Hashtag.txt",
            "Stickers.txt","Purchases.txt","Login History.txt","Reposts.txt","Status.txt",
            "Instant Form Ads Responses.txt","Fundraiser.txt","Settings.txt","Follower.txt",
            "Following.txt",
        ],
    )   
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


def _parse_tiktok_txt(data: io.BytesIO) -> dict[str, Any] | list[dict[str, Any]] | None:
    """Read structured TikTok data in txt format and parse it into a 1) flat dictionary, 
    2) list of dictionaries, or 3) nested dictionary, depending on the file structure."""

    lines = data.readlines()
    lines = [line.decode("utf-8") for line in lines]

    # Strip trailing blank lines
    while lines and lines[-1].strip() == "":
        lines.pop()
    
    # Check if file is empty or only contains a line indicating it is empty
    if not lines or (len(lines) == 1 and _is_empty_sentinel(lines[0])):
        return None

    blocks = _split_into_blocks(lines)

    # Need at least one block with key-value pairs to continue
    if not blocks:
        return None

    # 1. Single block case: return a single flat dictionary
    if len(blocks) == 1 and _block_only_kv(blocks[0]):    
        return _parse_kv_block(blocks[0])

    # 2. List of records case: multiple blocks with identical keys should return a list of dictionaries
    if len(blocks) >= 2 and _block_only_kv(blocks[0]) and _block_only_kv(blocks[1]):
        keys_0 = {line.partition(":")[0].strip() for line in blocks[0]}
        keys_1 = {line.partition(":")[0].strip() for line in blocks[1]}
        if keys_0 == keys_1:
            records: list[dict[str, Any]] = [] # Every block with these same keys is a record
            for blk in blocks:
                if _block_only_kv(blk): # Only include blocks with key-value pairs
                    blk_keys = {line.partition(":")[0].strip() for line in blk}
                    if blk_keys == keys_0: # Only include blocks with the same keys as the first two
                        records.append(_parse_kv_block(blk))
                    else: # Keys diverged, fall back to generic parsing
                        break
                else: # Not a key-value block, fall back to generic parsing
                    break
            else:
                return records

    # 3. Other cases: generic (nested) parsing
    # Blocks with a first line without ':' or ending with ':' without a subsequent value 
    # are treated as section headers, opening a nested dictionary that is populated with
    # the key-value pairs in the following lines until the next section header or end 
    # of the block. All other key-value lines are added to the current context, while 
    # other non key-value lines are ignored.
    result: dict[str, Any] = {}
    for block in blocks:
        section_name = None
        start_line = 0
        if (':' not in block[0] or block[0].strip().endswith(":")) and len(block) > 1: # First line is a section header -> make a nested dict for this block
            if ':' in block[1] or _is_empty_sentinel(block[1]): # Only treat as section header if there is content left in this block
                section_name = block[0].strip()
                result[section_name] = {}
                start_line = 1
        for line in block[start_line:]:
            if ':' in line:
                key, _, raw_value = line.partition(":")
                key = key.strip()
                value = _parse_value(raw_value)
                if section_name is not None:
                    result[section_name][key] = value
                else:
                    result[key] = value
            elif len(line.strip()) > 0 and not _is_empty_sentinel(line): # New section header found, starting new nested dict for subsequent key-value pairs
                section_name = line.strip()
                result[section_name] = {}
            else:
                continue #ignore non key-value lines that are not section headers
    return result


def _split_into_blocks(lines: list[str]) -> list[list[str]]:
    """Split a list of lines into *blocks* separated by one or more blank lines.
    Trailing empty blocks are discarded."""
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in lines:
        if line.strip() == "":
            if current:
                blocks.append(current)
                current = []
        else:
            current.append(line)
    if current:
        blocks.append(current)
    return blocks


def _is_empty_sentinel(line: str) -> bool:
    """Return True if *line* is a known empty-data placeholder."""
    _EMPTY_SENTINELS = {
        "dit gedeelte bevat geen gegevens",
        "er staan geen gegevens in dit gedeelte",
        "je hebt geen informatie over platforms van derden",
        "You have no data in this section",        
    }
    return line.strip().lower() in _EMPTY_SENTINELS


def _block_only_kv(block: list[str]) -> bool:
    """Return True when every line looks like 'key: <value>'."""
    return all([':' in line for line in block])


def _parse_kv_block(block: list[str]) -> dict[str, Any]:
    """Parse a block of lines in 'key: value' format into a dictionary, coercing
    values to richer types where possible."""
    result = {}
    for line in block:
        if ":" not in line:
            continue
        key, _, raw_value = line.partition(":") #note that any ':' in the value (e.g. for time) are perserved in raw_value
        key = key.strip()
        value = _parse_value(raw_value)
        result[key] = value
    return result


def _parse_value(raw: str) -> Any:
    """Coerce a raw string value coming from a TXT key-value line into a
    richer Python type where appropriate."""
    s = raw.strip()
    # 1. Empty list literal
    if s == "[]":
        return []
    # 2. Non-empty bracketed list (e.g. "[a, b, c]")
    if s.startswith("[") and s.endswith("]"):
        inner = s[1:-1].strip()
        if inner == "":
            return []
        return [item.strip() for item in inner.split(",")]
    # 3. Null-like sentinels
    if s.lower() in {"n/a", "n.v.t.", "none"}:
        return None
    # 4. Integer
    try:
        return int(s)
    except ValueError:
        pass
    return s


# ---------------------------------------------------------------------------
# Extractor functions
# ---------------------------------------------------------------------------

def activity_summary_to_df(reader: ZipArchiveReader, errors: Counter, validation) -> pd.DataFrame:
    """Extract TikTok activity summary counts.

    Reads ``Activity > Activity Summary > ActivitySummaryMap`` from the TikTok
    export JSON or from ``Samenvatting van activiteit.txt`` or ``Activity Summary.txt``
    in case of a TXT export.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or TXT files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation results for the extracted data used to determine ddp type and language.

    Returns
    -------
    pd.DataFrame
        Columns: ``Metric``, ``Count``.
        Empty DataFrame when the data is absent or parsing fails.

    Table documentation::

        {
          "summary": "Summary counts of TikTok activity measures since account registration, such as the number of videos watched, commented on, and shared.",
          "source_file": "user_data_tiktok.json or user_data.json",
          "columns": {
            "Metric": "Name of the activity metric",
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
            "Metric": {"en": "Activity metric", "nl": "Activiteitsmaat"},
            "Count": {"en": "Count", "nl": "Aantal"}
          }
        }
    """
    out = pd.DataFrame()
    if validation.current_ddp_category.ddp_filetype == DDPFiletype.JSON:
        data = _load_user_data(reader)
        try:
            summary = _get(
                data,
                ["Activity", "Your Activity"],
                "Activity Summary",
                "ActivitySummaryMap",
            )
            if not isinstance(summary, dict):
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    elif validation.current_ddp_category.ddp_filetype == DDPFiletype.TXT:
        if validation.current_ddp_category.language == Language.NL:
            data = reader.raw("Samenvatting van activiteit.txt")
        elif validation.current_ddp_category.language == Language.EN:
            data = reader.raw("Activity Summary.txt")
        else:
            return out
        if not data.found:
            return out
        try:
            summary = _parse_tiktok_txt(data.data)
            # _parse_tiktok_txt() returns None for an empty file; len(None)
            # would raise instead of taking the intended "nothing here" exit.
            if not summary:
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    else:
        return out
    try:
        # Guard: the branches above assign `summary` inside a try/except, so an
        # error there can leave it unbound or holding a non-dict. Kept inside
        # this try so an unbound name is still counted, exactly as before.
        if not isinstance(summary, dict):
            return out
        metric_priority = [
            ("Videos watched since registration", ["videoCount"]),
            ("Videos watched to the end since registration", ["videosWatchedToTheEndSinceAccountRegistration", "Videos watched to the end since account registration", "Video's tot het einde bekeken sinds accountregistratie"]),
            ("Videos commented on since registration", ["videosCommentedOnSinceAccountRegistration", "commentVideoCount", "Videos commented on since account registration", "Video's waarop is gereageerd sinds accountregistratie"]),
            ("Videos shared since registration", ["videosSharedSinceAccountRegistration", "sharedVideoCount", "Videos shared since account registration", "Video's gedeeld sinds accountregistratie"]),
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


def settings_to_df(reader: ZipArchiveReader, errors: Counter, validation) -> pd.DataFrame:
    """Extract TikTok content preference keyword filters.

    Reads ``App Settings > Settings > SettingsMap`` from the TikTok export JSON 
    or from ``Instellingen.txt`` or ``Settings.txt`` in case of a TXT export.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or TXT files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation results for the extracted data used to determine ddp type and language.

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
    out = pd.DataFrame()
    if validation.current_ddp_category.ddp_filetype == DDPFiletype.JSON:
        data = _load_user_data(reader)
        try:
            settings_map = _get(
                data,
                ["App Settings", "Profile And Settings"],
                "Settings",
                "SettingsMap",
            )
            if not isinstance(settings_map, dict):
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    elif validation.current_ddp_category.ddp_filetype == DDPFiletype.TXT:
        if validation.current_ddp_category.language == Language.NL:
            data = reader.raw("Instellingen.txt")
        elif validation.current_ddp_category.language == Language.EN:
            data = reader.raw("Settings.txt")
        else:
            return out
        if not data.found:
            return out
        try:
            settings_map = _parse_tiktok_txt(data.data)
            # _parse_tiktok_txt() returns None for an empty file; len(None)
            # would raise instead of taking the intended "nothing here" exit.
            if not settings_map:
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    else:
        return out
    try:
        # Guard: the branches above assign `settings_map` inside a try/except,
        # so an error there can leave it unbound or holding a non-dict. Kept
        # inside this try so an unbound name is still counted, exactly as before.
        if not isinstance(settings_map, dict):
            return out
        rows = []
        content_section_labels = ["Content Preferences", "Contentvoorkeuren"]
        for label in content_section_labels:
            if label in settings_map:
                content_preferences = settings_map.get(label, {})
                if isinstance(content_preferences, dict):
                    break
        if not isinstance(content_preferences, dict):
            return out
        field_map = {
            "Keyword filters for videos in Following feed": "Keyword filter for videos in the following feed",
            "Keyword filters for videos in For You feed": "Keyword filters for videos in For You feed",
            "Trefwoordfilters voor video's in de 'Volgend'-feed": "Keyword filter for videos in the following feed",
            "Trefwoordfilters voor video's in de 'Voor jou'-feed": "Keyword filters for videos in For You feed",
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


def watch_history_to_df(reader: ZipArchiveReader, errors: Counter, validation) -> pd.DataFrame:
    """Extract TikTok video watch history.

    Reads ``Activity > Video Browsing History > VideoList`` from the TikTok 
    export JSON or from ``Kijkgeschiedenis.txt`` or ``Watch History.txt`` in 
    case of a TXT export.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or TXT files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation results for the extracted data used to determine ddp type and language.

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
    out = pd.DataFrame()
    if validation.current_ddp_category.ddp_filetype == DDPFiletype.JSON:
        data = _load_user_data(reader)
        try:
            items = _get(
                data,
                ["Activity", "Your Activity"],
                ["Video Browsing History", "Watch History"],
                "VideoList",
            )
            if not isinstance(items, list):
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    elif validation.current_ddp_category.ddp_filetype == DDPFiletype.TXT:
        if validation.current_ddp_category.language == Language.NL:
            data = reader.raw("Kijkgeschiedenis.txt")
        elif validation.current_ddp_category.language == Language.EN:
            data = reader.raw("Watch History.txt")
        else:
            return out
        if not data.found:
            return out    
        try:
            items = _parse_tiktok_txt(data.data)
            if not isinstance(items, list):
                # When only one record is present, this is not automatically recognized as a list of records.
                # Therefor the returned dict needs to be stored in a list to proceed.
                if isinstance(items, dict):
                    items = [items]
                else:
                    return out
                
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    else:
        return out    
    try:
        # Guard: the branches above assign `items` inside a try/except, so an
        # error there can leave it unbound or holding a non-list. Kept inside
        # this try so an unbound name is still counted, exactly as before.
        if not isinstance(items, list):
            return out
        rows = [(_item_get(item, "Date", "Datum"), _item_get(item, "Link")) for item in items]
        out = pd.DataFrame(rows, columns=["Date", "Link"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def favorite_videos_to_df(reader: ZipArchiveReader, errors: Counter, validation) -> pd.DataFrame:
    """Extract TikTok favorite videos.

    Reads ``Activity > Favorite Videos > FavoriteVideoList`` from the TikTok
    export JSON or from ``Favoriete video's.txt`` or ``Favorite Videos.txt`` in
    case of a TXT export.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or TXT files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation results for the extracted data used to determine ddp type and language.

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
    out = pd.DataFrame()
    if validation.current_ddp_category.ddp_filetype == DDPFiletype.JSON:
        data = _load_user_data(reader)
        try:
            items = _get_first(
                data,
                (["Activity", "Your Activity"], "Favorite Videos", "FavoriteVideoList"),
                ("Likes and Favorites", "Favorite Videos", "FavoriteVideoList"),
            )
            if not isinstance(items, list):
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    elif validation.current_ddp_category.ddp_filetype == DDPFiletype.TXT:
        if validation.current_ddp_category.language == Language.NL:
            data = reader.raw("Favoriete video's.txt")
        elif validation.current_ddp_category.language == Language.EN:
            data = reader.raw("Favorite Videos.txt")
        else:
            return out
        if not data.found:
            return out    
        try:
            items = _parse_tiktok_txt(data.data)
            if not isinstance(items, list):
                # When only one record is present, this is not automatically recognized as a list of records.
                # Therefor the returned dict needs to be stored in a list to proceed.
                if isinstance(items, dict):
                    items = [items]
                else:
                    return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    try:
        # Guard: the branches above assign `items` inside a try/except, so an
        # error there can leave it unbound or holding a non-list. Kept inside
        # this try so an unbound name is still counted, exactly as before.
        if not isinstance(items, list):
            return out
        rows = [(_item_get(item, "Date", "Datum"), _item_get(item, "Link")) for item in items]
        out = pd.DataFrame(rows, columns=["Date", "Link"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def follower_to_df(reader: ZipArchiveReader, errors: Counter, validation) -> pd.DataFrame:
    """Extract TikTok follower list.

    Reads ``Activity > Follower List > FansList`` from the TikTok export JSON
    or from ``Volger.txt`` or ``Follower.txt`` in case of a TXT export.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or TXT files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation results for the extracted data used to determine ddp type and language.

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
            "UserName": {"en": "Username", "nl": "Gebruikersnaam"}
          }
        }
    """
    out = pd.DataFrame()
    if validation.current_ddp_category.ddp_filetype == DDPFiletype.JSON:
        data = _load_user_data(reader)
        try:
            items = _get_first(
                data,
                (["Activity", "Your Activity"], "Follower List", "FansList"),
                ("Profile And Settings", "Follower", "FansList"),
            )
            if not isinstance(items, list):
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    elif validation.current_ddp_category.ddp_filetype == DDPFiletype.TXT:
        if validation.current_ddp_category.language == Language.NL:
            data = reader.raw("Volger.txt")
        elif validation.current_ddp_category.language == Language.EN:
            data = reader.raw("Follower.txt")
        else:
            return out
        if not data.found:
            return out    
        try:
            items = _parse_tiktok_txt(data.data)
            if not isinstance(items, list):
                # When only one record is present, this is not automatically recognized as a list of records.
                # Therefor the returned dict needs to be stored in a list to proceed.
                if isinstance(items, dict):
                    items = [items]
                else:
                    return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    try:
        # Guard: the branches above assign `items` inside a try/except, so an
        # error there can leave it unbound or holding a non-list. Kept inside
        # this try so an unbound name is still counted, exactly as before.
        if not isinstance(items, list):
            return out
        rows = [(_item_get(item, "Date", "Datum"), _item_get(item, "UserName", "User Name", "Gebruikersnaam")) for item in items]
        out = pd.DataFrame(rows, columns=["Date", "UserName"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def following_to_df(reader: ZipArchiveReader, errors: Counter, validation) -> pd.DataFrame:
    """Extract TikTok following list.

    Reads ``Activity > Following List > Following`` from the TikTok export JSON
    or from ``Volgend.txt`` or ``Following.txt`` in case of a TXT export.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or TXT files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation results for the extracted data used to determine ddp type and language.

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
            "UserName": {"en": "Username", "nl": "Gebruikersnaam"}
          }
        }
    """
    out = pd.DataFrame()
    if validation.current_ddp_category.ddp_filetype == DDPFiletype.JSON:
        data = _load_user_data(reader)
        try:
            items = _get_first(
                data,
                (["Activity", "Your Activity"], ["Following List", "Following"], "Following"),
                ("Profile And Settings", "Following", "Following"),
            )
            if not isinstance(items, list):
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    elif validation.current_ddp_category.ddp_filetype == DDPFiletype.TXT:
        if validation.current_ddp_category.language == Language.NL:
            data = reader.raw("Volgend.txt")
        elif validation.current_ddp_category.language == Language.EN:
            data = reader.raw("Following.txt")
        else:
            return out
        if not data.found:
            return out    
        try:
            items = _parse_tiktok_txt(data.data)
            if not isinstance(items, list):
                # When only one record is present, this is not automatically recognized as a list of records.
                # Therefor the returned dict needs to be stored in a list to proceed.
                if isinstance(items, dict):
                    items = [items]
                else:
                    return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    try:
        # Guard: the branches above assign `items` inside a try/except, so an
        # error there can leave it unbound or holding a non-list. Kept inside
        # this try so an unbound name is still counted, exactly as before.
        if not isinstance(items, list):
            return out
        rows = [(_item_get(item, "Date", "Datum"), _item_get(item, "UserName", "User Name", "Gebruikersnaam")) for item in items]
        out = pd.DataFrame(rows, columns=["Date", "UserName"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def hashtag_to_df(reader: ZipArchiveReader, errors: Counter, validation) -> pd.DataFrame:
    """Extract TikTok hashtags associated with participant activity.

    Reads ``Activity > Hashtag > HashtagList`` from the TikTok export JSON
    or from ``Hashtag.txt`` in case of a TXT export.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or TXT files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation results for the extracted data used to determine ddp type and language.

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
            "HashtagName": {"en": "Hashtag", "nl": "Hashtag"},
            "HashtagLink": {"en": "Link", "nl": "Link"}
          }
        }
    """
    out = pd.DataFrame()
    if validation.current_ddp_category.ddp_filetype == DDPFiletype.JSON:
        data = _load_user_data(reader)
        try:
            items = _get(
                data,
                ["Activity", "Your Activity"],
                "Hashtag",
                "HashtagList",
            )
            if not isinstance(items, list):
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    elif validation.current_ddp_category.ddp_filetype == DDPFiletype.TXT:
        if validation.current_ddp_category.language == Language.NL:
            data = reader.raw("Hashtag.txt")
        elif validation.current_ddp_category.language == Language.EN:
            data = reader.raw("Hashtag.txt")
        else:
            return out
        if not data.found:
            return out    
        try:
            items = _parse_tiktok_txt(data.data)
            if not isinstance(items, list):
                # When only one record is present, this is not automatically recognized as a list of records.
                # Therefor the returned dict needs to be stored in a list to proceed.
                if isinstance(items, dict):
                    items = [items]
                else:
                    return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    try:
        # Guard: the branches above assign `items` inside a try/except, so an
        # error there can leave it unbound or holding a non-list. Kept inside
        # this try so an unbound name is still counted, exactly as before.
        if not isinstance(items, list):
            return out
        rows = [
            (_item_get(item, "HashtagName", "Hashtag Name", "Hashtag naam"), _item_get(item, "HashtagLink", "Hashtag Link"))
            for item in items
        ]
        out = pd.DataFrame(rows, columns=["HashtagName", "HashtagLink"])  # pyright: ignore
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def like_list_to_df(reader: ZipArchiveReader, errors: Counter, validation) -> pd.DataFrame:
    """Extract TikTok liked videos list.

    Reads ``Activity > Like List > ItemFavoriteList`` from the TikTok export JSON
    or from ``Likelijst.txt`` or ``Like List.txt`` in case of a TXT export.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or TXT files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation results for the extracted data used to determine ddp type and language.

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
            "Link": {"en": "Link", "nl": "Link"}
          }
        }
    """
    out = pd.DataFrame()
    if validation.current_ddp_category.ddp_filetype == DDPFiletype.JSON:
        data = _load_user_data(reader)
        try:
            items = _get_first(
                data,
                (["Activity", "Your Activity"], "Like List", "ItemFavoriteList"),
                ("Likes and Favorites", "Like List", "ItemFavoriteList"),
            )
            if not isinstance(items, list):
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    elif validation.current_ddp_category.ddp_filetype == DDPFiletype.TXT:  
        if validation.current_ddp_category.language == Language.NL:
            data = reader.raw("Likelijst.txt")
        elif validation.current_ddp_category.language == Language.EN:
            data = reader.raw("Like List.txt")
        else:
            return out
        if not data.found:
            return out    
        try:
            items = _parse_tiktok_txt(data.data)
            if not isinstance(items, list):
                # When only one record is present, this is not automatically recognized as a list of records.
                # Therefor the returned dict needs to be stored in a list to proceed.
                if isinstance(items, dict):
                    items = [items]
                else:
                    return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    try:
        # Guard: the branches above assign `items` inside a try/except, so an
        # error there can leave it unbound or holding a non-list. Kept inside
        # this try so an unbound name is still counted, exactly as before.
        if not isinstance(items, list):
            return out
        rows = [(_item_get(item, "Date", "Datum"), _item_get(item, "Link")) for item in items]
        out = pd.DataFrame(rows, columns=["Date", "Link"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def searches_to_df(reader: ZipArchiveReader, errors: Counter, validation) -> pd.DataFrame:
    """Extract TikTok search history.

    Reads ``Activity > Search History > SearchList`` from the TikTok export JSON
    or from ``Zoekopdrachten.txt`` or ``Searches.txt`` in case of a TXT export.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or TXT files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation results for the extracted data used to determine ddp type and language.

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
            "SearchTerm": {"en": "Search term", "nl": "Zoekterm"}
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
    out = pd.DataFrame()
    if validation.current_ddp_category.ddp_filetype == DDPFiletype.JSON:
        data = _load_user_data(reader)
        try:
            items = _get(
                data,
                ["Activity", "Your Activity"],
                ["Search History", "Searches"],
                "SearchList",
            )
            if not isinstance(items, list):
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    elif validation.current_ddp_category.ddp_filetype == DDPFiletype.TXT:
        if validation.current_ddp_category.language == Language.NL:
            data = reader.raw("Zoekopdrachten.txt")
        elif validation.current_ddp_category.language == Language.EN:
            data = reader.raw("Searches.txt")
        else:
            return out
        if not data.found:
            return out    
        try:
            items = _parse_tiktok_txt(data.data)
            if not isinstance(items, list):
                # When only one record is present, this is not automatically recognized as a list of records.
                # Therefor the returned dict needs to be stored in a list to proceed.
                if isinstance(items, dict):
                    items = [items]
                else:
                    return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    try:       
        # Guard: the branches above assign `items` inside a try/except, so an
        # error there can leave it unbound or holding a non-list. Kept inside
        # this try so an unbound name is still counted, exactly as before.
        if not isinstance(items, list):
            return out
        rows = [(_item_get(item, "Date","Datum"), _item_get(item, "SearchTerm", "Search Term", "Zoekterm")) for item in items]
        out = pd.DataFrame(rows, columns=["Date", "SearchTerm"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def share_history_to_df(reader: ZipArchiveReader, errors: Counter, validation) -> pd.DataFrame:
    """Extract TikTok share history.

    Reads ``Activity > Share History > ShareHistoryList`` from the TikTok
    export JSON or from ``Geschiedenis delen.txt`` or ``Share History.txt`` 
    in case of a TXT export.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or TXT files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation results for the extracted data used to determine ddp type and language.

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
            "SharedContent": {"en": "Shared content", "nl": "Gedeelde inhoud"},
            "Link": {"en": "Link", "nl": "Link"},
            "Method": {"en": "Method", "nl": "Methode"}
          }
        }
    """
    out = pd.DataFrame()
    if validation.current_ddp_category.ddp_filetype == DDPFiletype.JSON:
        data = _load_user_data(reader)
        try:
            items = _get(
                data,
                ["Activity", "Your Activity"],
                "Share History",
                "ShareHistoryList",
            )
            if not isinstance(items, list):
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    elif validation.current_ddp_category.ddp_filetype == DDPFiletype.TXT:
        if validation.current_ddp_category.language == Language.NL:
            data = reader.raw("Geschiedenis delen.txt")
        elif validation.current_ddp_category.language == Language.EN:
            data = reader.raw("Share History.txt")
        else:
            return out
        if not data.found:
            return out    
        try:
            items = _parse_tiktok_txt(data.data)
            if not isinstance(items, list):
                # When only one record is present, this is not automatically recognized as a list of records.
                # Therefor the returned dict needs to be stored in a list to proceed.
                if isinstance(items, dict):
                    items = [items]
                else:
                    return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    try:
        # Guard: the branches above assign `items` inside a try/except, so an
        # error there can leave it unbound or holding a non-list. Kept inside
        # this try so an unbound name is still counted, exactly as before.
        if not isinstance(items, list):
            return out
        rows = [
            (
                _item_get(item, "Date", "Datum"),
                _item_get(item, "SharedContent", "SharedContent", "Gedeelde inhoud"),
                _item_get(item, "Link"),
                _item_get(item, "Method", "Methode"),
            )
            for item in items
        ]
        out = pd.DataFrame(rows, columns=["Date", "SharedContent", "Link", "Method"])  # pyright: ignore
        out = out.sort_values("Date", ascending=False)
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def comments_to_df(reader: ZipArchiveReader, errors: Counter, validation) -> pd.DataFrame:
    """Extract TikTok comments.

    Reads ``Comment > Comments > CommentsList`` from the TikTok export JSON or 
    from ``Reacties.txt`` or ``Comments.txt`` in case of a TXT export.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON or TXT files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    validation:
        Validation results for the extracted data used to determine ddp type and language.

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
            "Url": {"en": "Url", "nl": "Url"}
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
    out = pd.DataFrame()
    if validation.current_ddp_category.ddp_filetype == DDPFiletype.JSON:
        data = _load_user_data(reader)
        out = pd.DataFrame()
        try:
            items = _get(data, "Comment", "Comments", "CommentsList")
            if not isinstance(items, list):
                return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    elif validation.current_ddp_category.ddp_filetype == DDPFiletype.TXT:
        if validation.current_ddp_category.language == Language.NL:
            data = reader.raw("Reacties.txt")
        elif validation.current_ddp_category.language == Language.EN:
            data = reader.raw("Comments.txt")
        else:
            return out
        if not data.found:
            return out    
        try:
            items = _parse_tiktok_txt(data.data)
            if not isinstance(items, list):
                # When only one record is present, this is not automatically recognized as a list of records.
                # Therefor the returned dict needs to be stored in a list to proceed.
                if isinstance(items, dict):
                    items = [items]
                else:
                    return out
        except Exception as e:
            logger.error("Exception caught: %s", e)
            errors[type(e).__name__] += 1
    try:
        # Guard: the branches above assign `items` inside a try/except, so an
        # error there can leave it unbound or holding a non-list. Kept inside
        # this try so an unbound name is still counted, exactly as before.
        if not isinstance(items, list):
            return out
        rows = [
            (
                _item_get(item, "Date", "Datum"),
                _item_get(item, "Comment", "Reactie"),
                _item_get(item, "Photo", "Foto"),
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

def extraction(tiktok_zip: SeekableBinaryReader, validation) -> ExtractionResult:
    """Extract data from a TikTok DDP zip and return consent-form tables.

    Parameters
    ----------
    tiktok_zip:
        Seekable binary reader over the TikTok DDP zip — the upload
        adapter itself, never a path (ADR-0026).
    validation:
        Validation result object that is passed on to the extractor functions in 
        ``EXTRACTOR_REGISTRY``, and whose ``archive_members`` attribute is passed 
         to ``ZipArchiveReader``.
    """
    config = load_port_config(EXTRACTOR_REGISTRY, "tiktok")
    for table in config: # Pass validation results to determine ddp type and language
        table.extractor_kwargs = {'validation': validation}
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
