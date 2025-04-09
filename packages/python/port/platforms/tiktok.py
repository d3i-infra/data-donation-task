"""
TikTok

This module contains an example flow of a TikTok data donation study
"""

from typing import Dict
import logging
import io
import re
import re

import pandas as pd

import port.api.props as props
import port.helpers.extraction_helpers as eh
import port.helpers.port_helpers as ph
import port.helpers.validate as validate

from port.helpers.validate import (
    DDPCategory,
    DDPFiletype,
    Language,
)

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "Transaction History.txt",
            "Most Recent Location Data.txt",
            "Comments.txt",
            "Purchases.txt",
            "Share History.txt",
            "Favorite Sounds.txt",
            "Searches.txt",
            "Login History.txt",
            "Favorite Videos.txt",
            "Favorite HashTags.txt",
            "Hashtag.txt",
            "Location Reviews.txt",
            "Favorite Effects.txt",
            "Following.txt",
            "Status.txt",
            "Browsing History.txt",
            "Like List.txt",
            "Follower.txt",
            "Watch Live settings.txt",
            "Go Live settings.txt",
            "Go Live History.txt",
            "Watch Live History.txt",
            "Profile Info.txt",
            "Autofill.txt",
            "Post.txt",
            "Block List.txt",
            "Settings.txt",
            "Customer support history.txt",
            "Communication with shops.txt",
            "Current Payment Information.txt",
            "Returns and Refunds History.txt",
            "Product Reviews.txt",
            "Order History.txt",
            "Vouchers.txt",
            "Saved Address Information.txt",
            "Order dispute history.txt",
            "Product Browsing History.txt",
            "Shopping Cart List.txt",
            "Direct Messages.txt",
            "Off TikTok Activity.txt",
            "Ad Interests.txt",
        ],
    ),
]



def browsing_history_to_df(tiktok_zip: str) -> Dict[str, Dict[str, str]] | None :

    out = None

    try:
        b = eh.extract_file_from_zip(tiktok_zip, "Browsing History.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nLink: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = {
            "Time and Date": { f"{i}": date for i, (date, _) in enumerate(matches) },
            "Video watched": { f"{i}": url for i, (_, url) in enumerate(matches) }
        }

    except Exception as e:
        logger.error(e)

    return out



def favorite_hashtag_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = eh.extract_file_from_zip(tiktok_zip, "Favorite HashTags.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nHashTag Link(?::|::) (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Tijdstip", "Hashtag url"]) # pyright: ignore

    except Exception as e:
        logger.error(e)

    return out



def favorite_videos_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = eh.extract_file_from_zip(tiktok_zip, "Favorite Videos.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nLink: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Tijdstip", "Video"]) # pyright: ignore

    except Exception as e:
        logger.error(e)

    return out



def follower_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = eh.extract_file_from_zip(tiktok_zip, "Follower.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Date"]) # pyright: ignore

    except Exception as e:
        logger.error(e)

    return out



def following_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = eh.extract_file_from_zip(tiktok_zip, "Following.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Date"]) # pyright: ignore

    except Exception as e:
        logger.error(e)

    return out


def hashtag_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = eh.extract_file_from_zip(tiktok_zip, "Hashtag.txt") # pyright: ignore
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Hashtag Name: (.*?)\nHashtag Link: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Hashtag naam", "Hashtag url"]) # pyright: ignore

    except Exception as e:
        logger.error(e)

    return out



def like_list_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = eh.extract_file_from_zip(tiktok_zip, "Like List.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nLink: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Tijdstip", "Video"]) # pyright: ignore

    except Exception as e:
        logger.error(e)

    return out


def searches_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = eh.extract_file_from_zip(tiktok_zip, "Searches.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nSearch Term: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Tijdstip", "Zoekterm"]) # pyright: ignore

    except Exception as e:
        logger.error(e)

    return out



def share_history_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = eh.extract_file_from_zip(tiktok_zip, "Share History.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nShared Content: (.*?)\nLink: (.*?)\nMethod: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Tijdstip", "Gedeelde inhoud", "Url", "Gedeeld via"]) # pyright: ignore

    except Exception as e:
        logger.error(e)

    return out


def settings_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = eh.extract_file_from_zip(tiktok_zip, "Settings.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Interests: (.*?)$", re.MULTILINE)
        match = re.search(pattern, text)
        if match:
            interests = match.group(1).split("|")
            out = pd.DataFrame(interests, columns=["Interesses"])  # pyright: ignore

    except Exception as e:
        logger.error(e)

    return out



def extraction(tiktok_zip: str) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    data = browsing_history_to_df(tiktok_zip)
    if data != None:
        df_name = f"tiktok_video_browsing_history"
        table_title = props.Translatable({
            "en": "Watch history", 
            "nl": "Kijkgeschiedenis"
        })
        table_description = props.Translatable({
            "en": "The table below indicates exactly which TikTok videos you have watched and when that was.",
            "nl": "De tabel hieronder geeft aan welke TikTok video's je precies hebt bekeken en wanneer dat was.",
        })
        table = props.PropsUIPromptConsentFormTable(df_name, table_title, data, table_description, []) 
        tables_to_render.append(table)


    df = favorite_videos_to_df(tiktok_zip)
    if not df.empty:
        df_name = "tiktok_favorite_videos"
        table_title = props.Translatable({
            "en": "Favorite video's", 
            "nl": "Favoriete video's", 
        })
        table_description = props.Translatable({
            "nl": "In de tabel hieronder vind je de video's die tot je favorieten behoren.", 
            "en": "In de tabel hieronder vind je de video's die tot je favorieten behoren.", 
         })
        table = props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description)
        tables_to_render.append(table)

    df = favorite_hashtag_to_df(tiktok_zip)
    if not df.empty:
        df_name = "tiktok_favorite_hashtags"
        table_title = props.Translatable({
            "en": "Favorite hashtags", 
            "nl": "Favoriete hashtags", 
        })
        table_description = props.Translatable({
            "en": "In the table below, you will find the hashtags that are among your favorites.",
            "nl": "In de tabel hieronder vind je de hashtags die tot je favorieten behoren.",
        })
        table = props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description)
        tables_to_render.append(table)

    df = hashtag_to_df(tiktok_zip)
    if not df.empty:
        df_name = "tiktok_hashtag"
        table_title = props.Translatable({
            "en": "Hashtags in video's die je hebt geplaatst", 
            "nl": "Hashtags in video's die je hebt geplaatst", 
        })
        table_description = props.Translatable({
            "nl": "In de tabel hieronder vind je de hashtags die je gebruikt hebt in een video die je hebt geplaats op TikTok.",
            "en": "In de tabel hieronder vind je de hashtags die je gebruikt hebt in een video die je hebt geplaats op TikTok.",
        })
        table = props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description)
        tables_to_render.append(table)

    df = like_list_to_df(tiktok_zip)
    if not df.empty:
        df_name = "tiktok_like_list"
        table_title = props.Translatable({
            "en": "Videos you have liked", 
            "nl": "Video's die je hebt geliket", 
        })
        table_description = props.Translatable({
            "nl": "In de tabel hieronder vind je de video's die je hebt geliket en wanneer dat was.",
            "en": "In the table below, you will find the videos you have liked and when that was.",
        })

        table =  props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description)
        tables_to_render.append(table)

    df = searches_to_df(tiktok_zip)
    if not df.empty:
        df_name = "tiktok_searches"
        wordcloud = {
            "title": {"en": "", "nl": ""},
            "type": "wordcloud",
            "textColumn": "Zoekterm",
        }
        table_title = props.Translatable({
            "en": "Search terms", 
            "nl": "Zoektermen", 
        })
        table_description = props.Translatable({
            "nl": "De tabel hieronder laat zien wat je hebt gezocht en wanneer dat was. De grootte van de woorden in de grafiek geeft aan hoe vaak de zoekterm voorkomt in jouw gegevens.",
            "en": "The table below shows what you have searched for and when. The size of the words in the chart indicates how often the search term appears in your data.",
        })
        table =  props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description, [wordcloud])
        tables_to_render.append(table)

    df = share_history_to_df(tiktok_zip)
    if not df.empty:
        df_name = "tiktok_share_history"
        table_title = props.Translatable({
            "en": "Shared videos", 
            "nl": "Gedeelde video's", 
        })
        table_description = props.Translatable({
            "nl": "In de tabel hieronder vind je wat je hebt gedeeld, op welk tijdstip en de manier waarop.",
            "en": "The table below shows what you have shared, at what time, and how.",
        })

        table =  props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description)
        tables_to_render.append(table)


    df = settings_to_df(tiktok_zip)
    if not df.empty:
        df_name = "tiktok_settings"
        table_title = props.Translatable({
            "en": "Interests on TikTok", 
            "nl": "Interesses op TikTok"
        })

        table_description = props.Translatable({
            "nl": "Hieronder vind je de interesses die je hebt aangevinkt bij het aanmaken van je TikTok account",
            "en": "Below you will find the interests you selected when creating your TikTok account",
        })

        table =  props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description)
        tables_to_render.append(table)

    return tables_to_render


# TEXTS
SUBMIT_FILE_HEADER = props.Translatable({
    "en": "Select your TikTok file", 
    "nl": "Selecteer uw TikTok bestand"
})

REVIEW_DATA_HEADER = props.Translatable({
    "en": "Your TikTok data", 
    "nl": "Uw TikTok gegevens"
})

RETRY_HEADER = props.Translatable({
    "en": "Try again", 
    "nl": "Probeer opnieuw"
})

REVIEW_DATA_DESCRIPTION = props.Translatable({
   "en": "Below you will find a selection of your TikTok data.",
   "nl": "Hieronder vindt u een geselecteerde weergave van uw TikTok-gegevens.",
})


def process(session_id: int):
    platform_name = "TikTok"

    table_list = None
    while True:
        logger.info("Prompt for file for %s", platform_name)

        file_prompt = ph.generate_file_prompt("application/zip")
        file_result = yield ph.render_page(SUBMIT_FILE_HEADER, file_prompt)

        if file_result.__type__ == "PayloadString":
            validation = validate.validate_zip(DDP_CATEGORIES, file_result.value)

            # Happy flow: Valid DDP
            if validation.get_status_code_id() == 0:
                logger.info("Payload for %s", platform_name)
                extraction_result = extraction(file_result.value)
                table_list = extraction_result
                break

            # Enter retry flow, reason: if DDP was not a Valid DDP
            if validation.get_status_code_id() != 0:
                logger.info("Not a valid %s zip; No payload; prompt retry_confirmation", platform_name)
                retry_prompt = ph.generate_retry_prompt(platform_name)
                retry_result = yield ph.render_page(RETRY_HEADER, retry_prompt)

                if retry_result.__type__ == "PayloadTrue":
                    continue
                else:
                    logger.info("Skipped during retry flow")
                    break

        else:
            logger.info("Skipped at file selection ending flow")
            break

    if table_list is not None:
        logger.info("Prompt consent; %s", platform_name)
        review_data_prompt = ph.generate_review_data_prompt(f"{session_id}-tiktok", REVIEW_DATA_DESCRIPTION, table_list)
        yield ph.render_page(REVIEW_DATA_HEADER, review_data_prompt)

    yield ph.exit(0, "Success")
    yield ph.render_end_page()
