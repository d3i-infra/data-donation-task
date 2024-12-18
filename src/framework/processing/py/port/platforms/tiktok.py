"""
TikTok

This module contains an example flow of a TikTok data donation study.

To see what type of DDPs from TikTok it is designed for check DDP_CATEGORIES
"""

from typing import Dict
import logging
import io
import re

import pandas as pd

import port.api.props as props
import port.helpers.extraction_helpers as eh
from port.platforms.flow_builder import DataDonationFlow
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



def extraction_fun(tiktok_zip: str) -> list[props.PropsUIPromptConsentFormTable]:
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


TEXTS = {
    "submit_file_header": props.Translatable({
        "en": "Select your TikTok file", 
        "nl": "Selecteer uw TikTok bestand"
    }),
    "review_data_header": props.Translatable({
        "en": "Your TikTok data", 
        "nl": "Uw TikTok gegevens"
    }),
    "retry_header": props.Translatable({
        "en": "Try again", 
        "nl": "Probeer opnieuw"
    }),
    "review_data_description": props.Translatable({
       "en": "Below you will find a selection of your TikTok data.",
       "nl": "Hieronder vindt u een geselecteerde weergave van uw TikTok-gegevens.",
    }),
}

FUNCTIONS = {
    "extraction": extraction_fun
}


def process(session_id: int):
    flow = DataDonationFlow(
        platform_name="TikTok", 
        ddp_categories=DDP_CATEGORIES,
        texts=TEXTS,
        functions=FUNCTIONS,
        session_id=session_id,
        is_donate_logs=False,
    )

    yield from flow.initialize_default_flow().run()
