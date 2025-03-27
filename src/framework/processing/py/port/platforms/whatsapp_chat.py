"""
Whatsapp

This module contains an example flow of a Whatsapp data donation study
"""
import logging

import pandas as pd
import re
import unicodedata
import zipfile

import port.api.props as props
import port.helpers.extraction_helpers as eh
import port.helpers.port_helpers as ph

from typing import Tuple, TypedDict

logger = logging.getLogger(__name__)

SIMPLIFIED_REGEXES = [
    r"^%m/%d/%y, %H:%M - %name: %chat_message$",
    r"^\[%d/%m/%y, %H:%M:%S\] %name: %chat_message$",
    r"^%d-%m-%y %H:%M - %name: %chat_message$",
    r"^\[%d-%m-%y %H:%M:%S\] %name: %chat_message$",
    r"^\[%m/%d/%y, %H:%M:%S\] %name: %chat_message$",
    r"^%d/%m/%y, %H:%M – %name: %chat_message$",
    r"^%d/%m/%y, %H:%M - %name: %chat_message$",
    r"^%d.%m.%y, %H:%M – %name: %chat_message$",
    r"^%d.%m.%y, %H:%M - %name: %chat_message$",
    r"^%m.%d.%y, %H:%M - %name: %chat_message$",
    r"^%m.%d.%y %H:%M - %name: %chat_message$",
    r"^\[%d/%m/%y, %H:%M:%S %P\] %name: %chat_message$",
    r"^\[%m/%d/%y, %H:%M:%S %P\] %name: %chat_message$",
    r"^\[%d.%m.%y, %H:%M:%S\] %name: %chat_message$",
    r"^\[%m/%d/%y %H:%M:%S\] %name: %chat_message$",
    r"^\[%m-%d-%y, %H:%M:%S\] %name: %chat_message$",
    r"^\[%m-%d-%y %H:%M:%S\] %name: %chat_message$",
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

    # Construct data
    date = '-'.join(
        [
            result.get("year", ""), 
            result.get("month", ""), 
            result.get("day", "")
        ]
    )
    name = result.get("name", "")
    chat_message = result.get("chat_message", "")

    return Datapoint(date=date, name=name, chat_message=chat_message)


def remove_empty_chats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes all rows from the chat dataframe where no regex matched
    """
    df = df.drop(df[df.chat_message == ""].index)
    df = df.reset_index(drop=True)
    return df


def filter_username(df: pd.DataFrame, username: str) -> pd.DataFrame:
    df = df.drop(df[df.name == username].index)
    df = df.reset_index(drop=True)

    return df


def split_dataframe(df: pd.DataFrame, row_count: int) -> list[pd.DataFrame]:
    """
    Split a pandas DataFrame into multiple based on a preset row_count.
    """
    # Calculate the number of splits needed.
    num_splits = int(len(df) / row_count) + (len(df) % row_count > 0)

    # Split the DataFrame into chunks of size row_count.
    df_splits = [df[i*row_count:(i+1)*row_count].reset_index(drop=True) for i in range(num_splits)]

    return df_splits



def extract_users(df: pd.DataFrame) -> list[str]:
    """
    Extracts unique usernames from chat dataframe
    Because parsing chats through regex is unreliable

    Non users can be detected This is an attempt to filter those out
    Non users are detected in the following case:

    timestamp - henk changed group name from bla to "blabla:
    everything from "- " up to ":" is detected as a username
    which in case of this system message is false
    one could go and account for all system messages in all languages. 
    But thats a fools errand
    """
    detected_users: list[str] = list(set(df["name"]))

    non_users = []
    for user in detected_users:
        for entry in detected_users:
            if bool(re.match(f"{re.escape(user)} ", f"{entry}")):
                non_users.append(entry)

    real_users = list(set(detected_users) - set(non_users))

    return real_users # pyright: ignore


def extract_groupname(df: pd.DataFrame) -> str:
    return df.loc[0, "name"]


def keep_users(df: pd.DataFrame, usernames: [str]) -> pd.DataFrame: # pyright: ignore
    df = df[df.name.isin(usernames)] # pyright: ignore
    df = df.reset_index(drop=True)

    return df


def anonymize_users(df: pd.DataFrame, list_with_users: list[str], user_name: str) -> pd.DataFrame:
    """
    Extracts unique usersnames from chat dataframe
    """

    users_not_you = list(set(list_with_users) - set([user_name]))
    mapping = {user_name : f"Member {i+2}" for i, user_name in enumerate(users_not_you)}
    mapping[user_name] = "Member 1"

    df['name'] = df['name'].replace(mapping)
    return df


def remove_name_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts unique usersnames from chat dataframe
    """
    df = df.drop(columns=["name"])
    return df


def reverse_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts unique usersnames from chat dataframe
    """
    df = df.sort_values('date',ascending=False)
    return df


def remove_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removing the date column from the chat dataframe
    """
    df = df.drop(columns=["date"])
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
                logger.debug(f"Matched regex: {regex}")
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
    #try:
    if zipfile.is_zipfile(path_to_chat_file):

      with zipfile.ZipFile(path_to_chat_file) as z:
        file_list = z.namelist()
        with z.open(file_list[0]) as f:
            lines = f.readlines()
            lines = [line.decode("utf-8") for line in lines]

    else:
        with open(path_to_chat_file, encoding="utf-8") as f:
            lines = f.readlines()

    out = [remove_unwanted_characters(line) for line in lines]
    out.pop(0) # remove first element containing system message

    #except Exception as e:
    #    raise e

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

    finally:
        return pd.DataFrame(out)


def deelnemer_statistics_to_df(df: pd.DataFrame, user_name: str) -> None | props.PropsUIPromptConsentFormTable:
    out = None

    try: 
        df["op_mij"] = df["name"].shift(-1)
        df["jij_op_wie"] = df["name"].shift(1)
        df["n_words"] = df["chat_message"].apply(lambda x: len(x.split()))

        # take into account self replies
        def ignore_self_reply(a, b):
            if a == b:
                return None
            else:
                return b

        df["op_mij"] = df[["name", "op_mij"]].apply(lambda x: ignore_self_reply(*x), axis=1)
        df["jij_op_wie"] = df[["name", "jij_op_wie"]].apply(lambda x: ignore_self_reply(*x), axis=1)

        df = df.drop(df[df.name != user_name].index) # pyright: ignore
        df = df.reset_index(drop=True)

        group_df = df.groupby("name").agg(
            {
                "date": ["count", "min", "max"],
                "op_mij": [ lambda x: x.value_counts().idxmax() ],
                "jij_op_wie": [ lambda x: x.value_counts().idxmax() ],
                "n_words": ["sum"]
            }
        ).reset_index(drop=True)

        group_df.columns = [
            "Number of messages", 
            "Date of first message",
            "Date of last message",
            "Who responds to you the most?",
            "Who do you respond to the most?",
            "Number of words"
        ]

        group_df = group_df[[
            "Number of words",
            "Number of messages", 
            "Date of first message",
            "Date of last message",
            "Who responds to you the most?",
            "Who do you respond to the most?"
        ]]

        df_out = pd.melt(
            group_df, # pyright: ignore
            var_name="Description",
            value_name="Value"
        )

        if user_name == "Member 1":
            title = "This is you (Member 1)"
            table_title = props.Translatable({ "en": title, "nl": title })
            description = props.Translatable({ 
                "en": """In this table you’ll see anonymized data of each member in the Whatsapp group chat, including the total number of messages you’ve sent, the dates of your first and last messages, who responds to you the most, who you respond to the most, and the total number of words you’ve used.""",    
                "nl": """In deze tabel zie je geanonimiseerde gegevens van elk lid in de Whatsapp-groepschat, waaronder het totale aantal berichten dat je hebt verstuurd, de data van je eerste en laatste bericht, wie het meest op jou reageert, op wie jij het meest reageert, en het totale aantal woorden dat je hebt gebruikt."""
            })
            out = props.PropsUIPromptConsentFormTable(f"table_id_{user_name.replace(' ', '_')}", table_title, df_out, description)
        else:
            title = user_name
            table_title = props.Translatable({ "en": title, "nl": title })
            out = props.PropsUIPromptConsentFormTable(f"table_id_{user_name.replace(' ', '_')}", table_title, df_out)

        return out

    except Exception as e:
        logger.error(e)

    finally:
        return out


# TEXTS
SUBMIT_FILE_HEADER = props.Translatable({
    "en": "Select your Whatsapp Group Chat file", 
    "nl": "Selecteer uw Whatsapp Group Chat bestand"
})

RADIO_HEADER = props.Translatable({
    "en": "Submit Whatsapp groupchat",
    "nl": "Submit Whatsapp groupchat"
})

RADIO_DESCRIPTION = props.Translatable({
    "en": "Please select your username", 
    "nl": "Selecteer uw gebruikersnaam"
})

REVIEW_DATA_HEADER = props.Translatable({
    "en": "Your Whatsapp Group Chat data", 
    "nl": "Uw Whatsapp Group Chat gegevens"
})

RETRY_HEADER = props.Translatable({
    "en": "Try again", 
    "nl": "Probeer opnieuw"
})

REVIEW_DATA_DESCRIPTION = props.Translatable({
   "en": "Below you will find a currated selection of Whatsapp Group Chat data.",
   "nl": "Below you will find a currated selection of Whatsapp Group Chat data.",
})


def process(session_id: int):
    platform_name = "Whatsapp Group Chat"
    list_with_consent_form_tables = []
    selected_username = ""

    while True:
        logger.info("Prompt for file for %s", platform_name)

        file_prompt = ph.generate_file_prompt("application/zip")
        file_result = yield ph.render_page(SUBMIT_FILE_HEADER, file_prompt)

        if file_result.__type__ == 'PayloadString':
            df = parse_chat(file_result.value)

            # Sad flow
            if df.empty:
                logger.info("Empty %s file; No payload; prompt retry_confirmation", platform_name)
                retry_prompt = ph.generate_retry_prompt(platform_name)
                retry_result = yield ph.render_page(RETRY_HEADER, retry_prompt)
                if retry_result.__type__ == "PayloadTrue":
                    continue
                else:
                    logger.info("Skipped during retry flow")
                    break

            # Happy flow
            else:
                logger.info("Payload for %s", platform_name)

                df = remove_empty_chats(df)
                users = extract_users(df)
                df = keep_users(df, users)

                if len(users) < 3:
                    logger.info("No group chat; prompt retry_confirmation")
                    retry_prompt = ph.generate_retry_prompt(platform_name)
                    retry_result = yield ph.render_page(RETRY_HEADER, retry_prompt)
                    if retry_result.__type__ == "PayloadTrue":
                        continue
                    else:
                        logger.info("Skipped during retry flow")
                        break

                if selected_username == "":
                    radio_prompt = ph.generate_radio_prompt(RADIO_DESCRIPTION, users)
                    selection = yield ph.render_page(RADIO_HEADER, radio_prompt)
                    # If user skips during this process, selectedUsername remains equal to ""
                    if selection.__type__ == "PayloadString":
                        selected_username = selection.value
                    else:
                        break
                    
                    df = anonymize_users(df, users, selected_username)
                    anonymized_users_list = [ f"Member {i + 1}" for i in range(len(users))]
                    for user_name in anonymized_users_list:
                        statistics_table = deelnemer_statistics_to_df(df, user_name)
                        if statistics_table != None:
                            list_with_consent_form_tables.append(statistics_table)

                    break

        else:
            logger.info("Skipped at file selection ending flow")
            break

    if len(list_with_consent_form_tables) > 0:
        logger.info("Prompt consent; %s", platform_name) 
        review_data_prompt = ph.generate_review_data_prompt(f"{session_id}-whatsapp-chat", REVIEW_DATA_DESCRIPTION, list_with_consent_form_tables)
        yield ph.render_page(REVIEW_DATA_HEADER, review_data_prompt)
    
    yield ph.exit(0, "Success")
    yield ph.render_end_page()
