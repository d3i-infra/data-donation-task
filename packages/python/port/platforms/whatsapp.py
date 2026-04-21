"""
WhatsApp Group Chat

This module contains an example flow of a WhatsApp Group Chat data donation study

Assumptions:
It handles DDPs containing a group chat. This extraction is not perfect because the text file containg the group chat does not follow a structure, however it performs well enough.

Note: WhatsApp DDPs are plain-text chat exports rather than structured JSON/CSV
archives.  The extraction pipeline parses the chat file into a DataFrame before
any per-table function runs, so these functions cannot conform to the standard
``(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame`` extractor
signature used by other platforms.  Accordingly, WhatsApp does not use
``EXTRACTOR_REGISTRY`` or ``run_extraction``.

Platform info::

    {
        "name": "WhatsApp Group Chat",
        "filetypes": ["txt", "zip"],
        "languages": ["en", "nl"],
        "description": "Handles WhatsApp group chat exports in plain-text format. Multiple date/time formats and locales are supported. These data donation flows have not been tested yet, if you find anything wrong with them report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "not yet implemented"
    }
"""

from typing import Callable, Tuple, TypedDict
from collections import Counter
from dateutil import parser
import unicodedata
import logging
import zipfile
import re

import pandas as pd

from port.api.d3i_props import ExtractionResult
import port.helpers.validate as validate
from port.helpers.flow_builder import FlowBuilder
from port.helpers.emoji_pattern import EMOJI_PATTERN

logger = logging.getLogger(__name__)

SIMPLIFIED_REGEXES = [
    r"^%d/%m/%y, %H:%M - %name: %chat_message$",
    r"^\[%d/%m/%y, %H:%M:%S\] %name: %chat_message$",
    r"^%d-%m-%y %H:%M - %name: %chat_message$",
    r"^\[%d-%m-%y %H:%M:%S\] %name: %chat_message$",
    r"^%d/%m/%y, %H:%M – %name: %chat_message$",
    r"^%d/%m/%y, %H:%M - %name: %chat_message$",
    r"^%d.%m.%y, %H:%M – %name: %chat_message$",
    r"^%d.%m.%y, %H:%M - %name: %chat_message$",
    r"^\[%d/%m/%y, %H:%M:%S %P\] %name: %chat_message$",
    r"^\[%m/%d/%y, %H:%M:%S %P\] %name: %chat_message$",
    r"^\[%m/%d/%y, %H:%M:%S\] %name: %chat_message$",
    r"^\[%d.%m.%y, %H:%M:%S\] %name: %chat_message$",
    r"^\[%m/%d/%y %H:%M:%S\] %name: %chat_message$",
    r"^\[%m-%d-%y, %H:%M:%S\] %name: %chat_message$",
    r"^\[%m-%d-%y %H:%M:%S\] %name: %chat_message$",
    r"^%m.%d.%y, %H:%M - %name: %chat_message$",
    r"^%m.%d.%y %H:%M - %name: %chat_message$",
    r"^%m-%d-%y %H:%M - %name: %chat_message$",
    r"^%m-%d-%y, %H:%M - %name: %chat_message$",
    r"^%m-%d-%y, %H:%M , %name: %chat_message$",
    r"^%m/%d/%y, %H:%M , %name: %chat_message$",
    r"^%d-%m-%y, %H:%M , %name: %chat_message$",
    r"^%d/%m/%y, %H:%M , %name: %chat_message$",
    r"^%d.%m.%y %H:%M – %name: %chat_message$",
    r"^%m.%d.%y, %H:%M – %name: %chat_message$",
    r"^%m.%d.%y %H:%M – %name: %chat_message$",
    r"^\[%d.%m.%y %H:%M:%S\] %name: %chat_message$",
    r"^\[%m.%d.%y, %H:%M:%S\] %name: %chat_message$",
    r"^\[%m.%d.%y %H:%M:%S\] %name: %chat_message$",
    r"^%m/%d/%y, %H:%M - %name: %chat_message$",
    r"^(?P<year>.*?)(?:\] | - )%name: %chat_message$"  # Fallback catch all regex
]


REGEX_CODES = {
    "%Y": r"(?P<year>\d{2,4})",
    "%y": r"(?P<year>\d{2,4})",
    "%m": r"(?P<month>\d{1,2})",
    "%d": r"(?P<day>\d{1,2})",
    "%H": r"(?P<hour>\d{1,2})",
    "%I": r"(?P<hour>\d{1,2})",
    "%M": r"(?P<minutes>\d{2})",
    "%S": r"(?P<seconds>\d{2})",
    "%P": r"(?P<ampm>[AaPp].? ?[Mm].?)",
    "%p": r"(?P<ampm>[AaPp].? ?[Mm].?)",
    "%name": r"(?P<name>[^:]*)",
    "%chat_message": r"(?P<chat_message>.*)"
}


def generate_regexes(simplified_regexes):
    """
    Create the complete regular expression by substituting
    REGEX_CODES into SIMPLIFIED_REGEXES
    """
    final_regexes = []

    for simplified_regex in simplified_regexes:

        codes = re.findall(r"\%\w*", simplified_regex)
        for code in codes:
            try:
                simplified_regex = simplified_regex.replace(code, REGEX_CODES[code])
            except KeyError:
                logger.error(f"Could not find regular expression for: {code}")

        final_regexes.append(simplified_regex)

    return final_regexes


REGEXES =  generate_regexes(SIMPLIFIED_REGEXES)


def remove_unwanted_characters(s: str) -> str:
    """
    Cleans string from bytes using magic

    Keeps empjis intact
    """
    s = "".join(ch for ch in s if unicodedata.category(ch)[0]!="C")
    s = unicodedata.normalize("NFKD", s)
    return s


def convert_to_iso8601(timestamp):
    try:
        dt = parser.parse(timestamp)
        return dt.isoformat()
    except (ValueError, TypeError) as e:
        return timestamp


class Datapoint(TypedDict):
    date: str
    name: str
    chat_message: str


def create_data_point_from_chat(chat: str, regex) -> Datapoint:
    """
    Construct data point from chat messages
    """
    result = re.match(regex, chat)
    if result:
        result = result.groupdict()
    else:
        return Datapoint(date="", name="", chat_message="")

    # Construct date
    date = convert_to_iso8601(
        f"{result.get('year', '')}-{result.get('month', '')}-{result.get('day', '')} {result.get('hour', '')}:{result.get('minutes', '')}"
    )
    name = result.get("name", "")
    chat_message = result.get("chat_message", "")

    return Datapoint(date=date, name=name, chat_message=chat_message)


def remove_empty_chats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes all rows from the chat dataframe where no regex matched
    """
    df = df.drop(df[df.chat_message == ""].index) # pyright: ignore
    df = df.reset_index(drop=True)
    return df


def extract_users(df: pd.DataFrame) -> list[str]:
    """
    Extracts unique usernames from chat dataframe
    Because parsing chats through regex is unreliable

    Non users can be detected. This is an attempt to filter those out.
    consider the following case where non users are detected:

    timestamp - henk changed group name from bla to "blabla:
    everything from "- " up to ":" is detected as a username

    So assuming henk is also a user that is detected. It can be filtered out by checking whether 
    the a username occurs in another username, and to filter those out. 
    This could lead to problems if people have names that occur in other names.
    For example "john"" and "johnnie"
    """
    detected_users: list[str] = list(set(df["name"]))

    non_users = []
    for user in detected_users:
        for entry in detected_users:
            if bool(re.match(f"{re.escape(user)} ", f"{entry}")):
                non_users.append(entry)

                
    real_users = list(set(detected_users) - set(non_users))

    return real_users # pyright: ignore


def keep_users(df: pd.DataFrame, usernames: [str]) -> pd.DataFrame: # pyright: ignore
    """
    Extracts unique usersnames from chat dataframe
    """
    df = df[df.name.isin(usernames)] # pyright: ignore
    df = df.reset_index(drop=True)

    return df


def determine_regex_from_chat(lines: list[str]) -> str:
    """
    Read lines of chat return the first regex that matches
    That regex is used to process the chatfile
    """

    length_lines = len(lines)
    for index, line in enumerate(lines):
        for regex in REGEXES:
            if re.match(regex, line):
                logger.info(f"Matched regex: {regex}")
                return regex

        if index > length_lines:
            break

    logger.error(f"No matching regex found:")
    raise Exception(f"No matching regex found")


def construct_message(current_line: str, next_line: str, regex: str) -> Tuple[bool, str]:
    """
    Helper function: determines whether the next line in the chat matches the regex
    in case of no match it means that the line belongs to the message on the current line
    """

    match_next_line = re.match(regex, next_line)
    if match_next_line:
        return True, current_line
    else:
        current_line = current_line + " " + next_line
        current_line = current_line.replace("\n", " ")
        return False, current_line


def read_chat_file(path_to_chat_file: str) -> list[str]:

    out = []
    if zipfile.is_zipfile(path_to_chat_file):
      with zipfile.ZipFile(path_to_chat_file) as z:
        file_list = z.namelist()
        print(f"{file_list}")
        with z.open(file_list[0]) as f:
            lines = f.readlines()
            lines = [line.decode("utf-8") for line in lines]

    else:
        with open(path_to_chat_file, encoding="utf-8") as f:
            lines = f.readlines()

    out = [remove_unwanted_characters(line) for line in lines]

    return out


def parse_chat(path_to_chat: str) -> pd.DataFrame:
    """
    Read chat from file, parse, return df

    In case of error returns empty df
    """
    out = []

    try:
        lines = read_chat_file(path_to_chat)
        regex = determine_regex_from_chat(lines)

        current_line = lines.pop(0)
        next_line = lines.pop(0)

        while True:
            try:
                match_next_line, chat = construct_message(current_line, next_line, regex)

                while not match_next_line:
                    next_line = lines.pop(0)
                    match_next_line, chat = construct_message(chat, next_line, regex)

                data_point = create_data_point_from_chat(chat, regex)
                out.append(data_point)

                current_line = next_line
                next_line = lines.pop(0)

            # IndexError occurs when pop fails
            # Meaning we processed all chat messages
            except IndexError:
                data_point = create_data_point_from_chat(current_line, regex)
                out.append(data_point)
                break

    except Exception as e:
        logger.error(e)

    return pd.DataFrame(out)


def find_emojis(df):
    out = pd.DataFrame()
    try:

        emojis = []
        for text in df['chat_message']:
            chars = EMOJI_PATTERN.findall(text)
            emojis.extend(chars)

        emoji_counter = Counter(emojis)
        most_common_emojis = emoji_counter.most_common(100)
        out = pd.DataFrame(most_common_emojis, columns=['Emoji', 'Count']) # pyright: ignore

    except Exception as e:
        logger.error(e)

    return out


def who_reacted_to_you_the_most(df: pd.DataFrame, name_react: str) -> str:
    reacted = []
    names = df["name"]
    for i, name in enumerate(names):
        if i > 0:
            if name != name_react and names[i-1] == name_react:
                reacted.append(name)

    r = Counter(reacted).most_common(1)
    who_reacted_to_you_the_most = ""
    if len(r) > 0:
        who_reacted_to_you_the_most, _ = r[0]

    return who_reacted_to_you_the_most


def who_you_reacted_to_the_most(df: pd.DataFrame, name_react: str) -> str:
    reacted = []
    names = df["name"]
    for i, name in enumerate(names):
        if i > 0:
            if name == name_react and names[i-1] != name_react:
                reacted.append(names[i-1])

    r = Counter(reacted).most_common(1)
    who_you_reacted_to_the_most = ""
    if len(r) > 0:
        who_you_reacted_to_the_most, _ = r[0]

    return who_you_reacted_to_the_most


def total_number_of_messages(df: pd.DataFrame, name: str) -> int:
    messages = df[df["name"] == name]["chat_message"]
    return(len(messages))


def total_number_of_words(df: pd.DataFrame, name: str) -> int:
    messages = df[df["name"] == name]["chat_message"]
    total_number_of_words = 0

    for message in messages:
        total_number_of_words += len(message.split())

    return total_number_of_words


def favorite_emoji(df: pd.DataFrame, name: str) -> str:
    messages = df[df["name"] == name]["chat_message"]
    emojis = []

    for message in messages:
        emojis.extend(EMOJI_PATTERN.findall(message))

    emoji_counter_list = Counter(emojis).most_common(1)
    most_common_emoji = ""
    if len(emoji_counter_list) > 0:
        most_common_emoji, _ = emoji_counter_list[0]

    return most_common_emoji


def chat_messages_to_df(df: pd.DataFrame, errors: Counter) -> pd.DataFrame:
    """Return the full group chat as a DataFrame.

    Parameters
    ----------
    df:
        Pre-parsed chat DataFrame with columns ``date``, ``name``,
        ``chat_message``.
    errors:
        Mutable counter for error accumulation.  Unused here but required by
        the extractor protocol.

    Returns
    -------
    pd.DataFrame
        Columns: ``Timestamp``, ``Name``, ``Message``.

    Table documentation::

        {
          "summary": "Each row represents one message in the WhatsApp group chat, including the sender name, message text, and timestamp.",
          "source_file": "WhatsApp chat export (.txt or .zip)",
          "columns": {
            "Timestamp": "ISO 8601 timestamp of the message.",
            "Name": "Display name of the message sender.",
            "Message": "Text content of the message."
          }
        }

    Table config::

        {
          "id": "whatsapp_group_chat",
          "title": {
            "en": "Your group chat",
            "nl": "Je groepsgesprek"
          },
          "description": {
            "en": "The contents of your group chat. Timestamps (and therefore some tables) can be incorrect as it assumes the European format.",
            "nl": "De inhoud van je groepsgesprek. Tijdstempels (en dus sommige tabellen) kunnen onjuist zijn omdat het Europese formaat wordt aangenomen."
          },
          "headers": {
            "Timestamp": {"en": "Timestamp", "nl": "Tijdstempel"},
            "Name": {"en": "Name", "nl": "Naam"},
            "Message": {"en": "Message", "nl": "Bericht"}
          },
          "visualizations": [
            {
              "title": {"en": "Most common words in your chats", "nl": "Meest gebruikte woorden in je gesprekken"},
              "type": "wordcloud",
              "textColumn": "Message",
              "tokenize": true
            },
            {
              "title": {"en": "Total chats per month of the year", "nl": "Totaal chats per maand"},
              "type": "area",
              "group": {"column": "Timestamp", "dateFormat": "month"},
              "values": [{}]
            },
            {
              "title": {"en": "Total chats per hour of the day", "nl": "Totaal chats per uur van de dag"},
              "type": "bar",
              "group": {"column": "Timestamp", "dateFormat": "hour_cycle"},
              "values": [{}]
            }
          ]
        }
    """
    return df.rename(columns={"date": "Timestamp", "name": "Name", "chat_message": "Message"})


def emoji_usage_to_df(df: pd.DataFrame, errors: Counter) -> pd.DataFrame:
    """Return the 100 most used emojis across all chat members.

    Parameters
    ----------
    df:
        Pre-parsed chat DataFrame with a ``chat_message`` column.
    errors:
        Mutable counter for error accumulation.  Unused here but required by
        the extractor protocol.

    Returns
    -------
    pd.DataFrame
        Columns: ``Emoji``, ``Count``.

    Table documentation::

        {
          "summary": "Each row represents one emoji used in the group chat, ranked by frequency across all members.",
          "source_file": "WhatsApp chat export (.txt or .zip)",
          "columns": {
            "Emoji": "The emoji character.",
            "Count": "Total number of times this emoji was used."
          }
        }

    Table config::

        {
          "id": "whatsapp_emoji_usage",
          "title": {
            "en": "The 100 most used emojis in the group",
            "nl": "De 100 meest gebruikte emojis in de groep"
          },
          "description": {
            "en": "Analysis of emoji frequency used by all members in the chat.",
            "nl": "Analyse van emoji-frequentie gebruikt door alle leden in de chat."
          },
          "headers": {
            "Emoji": {"en": "Emoji", "nl": "Emoji"},
            "Count": {"en": "Count", "nl": "Aantal"}
          }
        }
    """
    return find_emojis(df)


def user_statistics_to_df(df: pd.DataFrame, errors: Counter) -> pd.DataFrame:
    """Return messaging statistics for every detected user in the chat.

    Parameters
    ----------
    df:
        Pre-parsed chat DataFrame with columns ``date``, ``name``,
        ``chat_message``.
    errors:
        Mutable counter for error accumulation.  Unused here but required by
        the extractor protocol.

    Returns
    -------
    pd.DataFrame
        Columns: ``User``, ``Description``, ``Statistic``.
        One row per (user, metric) combination.

    Table documentation::

        {
          "summary": "Each row represents a messaging statistic for one detected chat participant, including who they interact with most and their emoji usage.",
          "source_file": "WhatsApp chat export (.txt or .zip)",
          "columns": {
            "User": "Display name of the chat participant.",
            "Description": "Name of the statistic.",
            "Statistic": "Value of the statistic."
          }
        }

    Table config::

        {
          "id": "whatsapp_user_statistics",
          "title": {
            "en": "Chat statistics per user",
            "nl": "Chatstatistieken per gebruiker"
          },
          "description": {
            "en": "Detailed messaging patterns and activity metrics for each participant in the group chat.",
            "nl": "Gedetailleerde berichtpatronen en activiteitsgegevens voor elke deelnemer in het groepsgesprek."
          },
          "headers": {
            "User": {"en": "User", "nl": "Gebruiker"},
            "Description": {"en": "Description", "nl": "Beschrijving"},
            "Statistic": {"en": "Statistic", "nl": "Statistiek"}
          }
        }
    """
    users = extract_users(df)
    rows = []
    for user in users:
        rows.extend([
            (user, "who reacted to you the most", who_reacted_to_you_the_most(df, user)),
            (user, "who you reacted to the most", who_you_reacted_to_the_most(df, user)),
            (user, "total number of messages you send", total_number_of_messages(df, user)),
            (user, "total number of words you send", total_number_of_words(df, user)),
            (user, "the emoji you used most", favorite_emoji(df, user)),
        ])
    return pd.DataFrame(rows, columns=["User", "Description", "Statistic"])  # pyright: ignore


# ---------------------------------------------------------------------------
# Extractor registry & platform info
# ---------------------------------------------------------------------------

#: Mapping from the string names used in port_config.json to actual extractor functions.
#: WhatsApp extractors take ``(df: pd.DataFrame, errors: Counter)`` instead of
#: ``(reader: ZipArchiveReader, errors: Counter)`` because the chat file is
#: pre-parsed into a DataFrame before extraction runs.
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "chat_messages_to_df": chat_messages_to_df,
    "emoji_usage_to_df": emoji_usage_to_df,
    "user_statistics_to_df": user_statistics_to_df,
}


# ---------------------------------------------------------------------------
# Main extraction & flow
# ---------------------------------------------------------------------------

def extraction(df: pd.DataFrame) -> ExtractionResult:
    """Extract tables from a pre-parsed WhatsApp chat DataFrame.

    WhatsApp DDPs are plain-text exports rather than structured archives, so
    extraction pre-parses the chat into *df* before calling individual
    extractors.  Extractors therefore receive ``(df, errors)`` instead of the
    usual ``(reader, errors)``.

    Parameters
    ----------
    df:
        Pre-parsed, filtered chat DataFrame with columns ``date``, ``name``,
        ``chat_message``.
    """
    from port.helpers.table_extractor import load_port_config
    from port.api.d3i_props import PropsUIPromptConsentFormTableViz

    config = load_port_config(EXTRACTOR_REGISTRY)
    errors: Counter = Counter()
    tables = []
    for table_cfg in config:
        result_df = table_cfg.extractor(df, errors, **table_cfg.extractor_kwargs)
        if table_cfg.variables is not None:
            result_df = result_df[[c for c in table_cfg.variables if c in result_df.columns]]
        tables.append(PropsUIPromptConsentFormTableViz(
            id=table_cfg.id,
            data_frame=result_df,
            title=table_cfg.title,
            description=table_cfg.description,
            headers=table_cfg.headers,
            visualizations=table_cfg.visualizations if table_cfg.visualizations else None,
        ))
    return ExtractionResult(
        tables=[t for t in tables if not t.data_frame.empty],
        errors=errors,
    )


class WhatsAppFlow(FlowBuilder):
    def __init__(self, session_id: str):
        super().__init__(session_id, "WhatsApp Group Chat")
        
    def validate_file(self, file):
        df = parse_chat(file)
        if not df.empty:
            return validate.BaseValidation(status_code=0)
        else:
            return validate.BaseValidation(status_code=1)
        
    def extract_data(self, file, validation):
        df = parse_chat(file)
        df = remove_empty_chats(df)
        users = extract_users(df)
        df = keep_users(df, users)
        return extraction(df)


def process(session_id):
    flow = WhatsAppFlow(session_id)
    return flow.start_flow()
