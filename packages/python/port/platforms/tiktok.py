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
import port.api.d3i_props as d3i_props
import port.helpers.extraction_helpers as eh
import port.helpers.validate as validate
from port.platforms.flow_builder import FlowBuilder

from port.helpers.validate import (
    DDPCategory,
    DDPFiletype,
    Language,
)

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="txt_en",
        ddp_filetype=DDPFiletype.TXT,
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



def browsing_history_to_df(tiktok_zip: str) -> pd.DataFrame:

    out = pd.DataFrame()

    try:
        b = eh.extract_file_from_zip(tiktok_zip, "Browsing History.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nLink: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Time and Date", "Video watched"]) # pyright: ignore

    except Exception as e:
        logger.error(e)

    return out


def favorite_hashtag_to_df(tiktok_zip: str) -> pd.DataFrame:

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


def favorite_videos_to_df(tiktok_zip: str) -> pd.DataFrame:

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


def follower_to_df(tiktok_zip: str) -> pd.DataFrame:

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


def following_to_df(tiktok_zip: str) -> pd.DataFrame:

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


def hashtag_to_df(tiktok_zip: str) -> pd.DataFrame:

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



def like_list_to_df(tiktok_zip: str) -> pd.DataFrame:

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


def searches_to_df(tiktok_zip: str) -> pd.DataFrame:

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



def share_history_to_df(tiktok_zip: str) -> pd.DataFrame:

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


def settings_to_df(tiktok_zip: str) -> pd.DataFrame:

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


def extraction(tiktok_zip: str) -> list[d3i_props.PropsUIPromptConsentFormTableViz]:
    tables = [
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="tiktok_video_browsing_history",
            data_frame=browsing_history_to_df(tiktok_zip),
            title=props.Translatable({
                "en": "Watch history", 
                "nl": "Kijkgeschiedenis"
            }),
            description=props.Translatable({
                "en": "The table below indicates exactly which TikTok videos you have watched and when that was.",
                "nl": "De tabel hieronder geeft aan welke TikTok video's je precies hebt bekeken en wanneer dat was.",
            }),
            visualizations=[]
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="tiktok_favorite_videos",
            data_frame=favorite_videos_to_df(tiktok_zip),
            title=props.Translatable({
                "en": "Favorite video's", 
                "nl": "Favoriete video's", 
            }),
            description=props.Translatable({
                "en": "In de tabel hieronder vind je de video's die tot je favorieten behoren.",
                "nl": "In de tabel hieronder vind je de video's die tot je favorieten behoren.", 
            }),
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="tiktok_favorite_hashtags",
            data_frame=favorite_hashtag_to_df(tiktok_zip),
            title=props.Translatable({
                "en": "Favorite hashtags", 
                "nl": "Favoriete hashtags", 
            }),
            description=props.Translatable({
                "en": "In the table below, you will find the hashtags that are among your favorites.",
                "nl": "In de tabel hieronder vind je de hashtags die tot je favorieten behoren.",
            }),
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="tiktok_hashtag",
            data_frame=hashtag_to_df(tiktok_zip),
            title=props.Translatable({
                "en": "Hashtags in video's die je hebt geplaatst", 
                "nl": "Hashtags in video's die je hebt geplaatst", 
            }),
            description=props.Translatable({
                "en": "In de tabel hieronder vind je de hashtags die je gebruikt hebt in een video die je hebt geplaats op TikTok.",
                "nl": "In de tabel hieronder vind je de hashtags die je gebruikt hebt in een video die je hebt geplaats op TikTok.",
            }),
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="tiktok_like_list",
            data_frame=like_list_to_df(tiktok_zip),
            title=props.Translatable({
                "en": "Videos you have liked", 
                "nl": "Video's die je hebt geliket", 
            }),
            description=props.Translatable({
                "en": "In the table below, you will find the videos you have liked and when that was.",
                "nl": "In de tabel hieronder vind je de video's die je hebt geliket en wanneer dat was.",
            }),
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="tiktok_searches",
            data_frame=searches_to_df(tiktok_zip),
            title=props.Translatable({
                "en": "Search terms", 
                "nl": "Zoektermen", 
            }),
            description=props.Translatable({
                "en": "The table below shows what you have searched for and when. The size of the words in the chart indicates how often the search term appears in your data.",
                "nl": "De tabel hieronder laat zien wat je hebt gezocht en wanneer dat was. De grootte van de woorden in de grafiek geeft aan hoe vaak de zoekterm voorkomt in jouw gegevens.",
            }),
            visualizations=[
                {
                    "title": {"en": "", "nl": ""},
                    "type": "wordcloud",
                    "textColumn": "Zoekterm",
                }
            ]
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="tiktok_share_history",
            data_frame=share_history_to_df(tiktok_zip),
            title=props.Translatable({
                "en": "Shared videos", 
                "nl": "Gedeelde video's", 
            }),
            description=props.Translatable({
                "en": "The table below shows what you have shared, at what time, and how.",
                "nl": "In de tabel hieronder vind je wat je hebt gedeeld, op welk tijdstip en de manier waarop.",
            }),
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="tiktok_settings",
            data_frame=settings_to_df(tiktok_zip),
            title=props.Translatable({
                "en": "Interests on TikTok", 
                "nl": "Interesses op TikTok"
            }),
            description=props.Translatable({
                "en": "Below you will find the interests you selected when creating your TikTok account",
                "nl": "Hieronder vind je de interesses die je hebt aangevinkt bij het aanmaken van je TikTok account",
            }),
        ),
    ]

    tables_to_render = [table for table in tables if table.data_frame is not None and not table.data_frame.empty]
    return tables_to_render


class TikTokFlow(FlowBuilder):
    def __init__(self, session_id: int):
        super().__init__(session_id, "TikTok")
        
    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)
        
    def extract_data(self, file_value, validation):
        return extraction(file_value)


def process(session_id):
    flow = TikTokFlow(session_id)
    return flow.start_flow()
