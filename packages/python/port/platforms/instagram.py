"""
Instagram

This module contains an example flow of a Instagram data donation study

Assumptions:
It handles DDPs in the english language with filetype JSON.
"""
import logging

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
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "secret_conversations.json",
            "personal_information.json",
            "account_privacy_changes.json",
            "account_based_in.json",
            "recently_deleted_content.json",
            "liked_posts.json",
            "stories.json",
            "profile_photos.json",
            "followers.json",
            "signup_information.json",
            "comments_allowed_from.json",
            "login_activity.json",
            "your_topics.json",
            "camera_information.json",
            "recent_follow_requests.json",
            "devices.json",
            "professional_information.json",
            "follow_requests_you've_received.json",
            "eligibility.json",
            "pending_follow_requests.json",
            "videos_watched.json",
            "ads_interests.json",
            "account_searches.json",
            "following.json",
            "posts_viewed.json",
            "recently_unfollowed_accounts.json",
            "post_comments.json",
            "account_information.json",
            "accounts_you're_not_interested_in.json",
            "use_cross-app_messaging.json",
            "profile_changes.json",
            "reels.json",
        ],
    )
]



def accounts_not_interested_in_to_df(instagram_zip: str) -> pd.DataFrame:

    b = eh.extract_file_from_zip(instagram_zip, "accounts_you're_not_interested_in.json")
    d = eh.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["impressions_history_recs_hidden_authors"] # pyright: ignore
        for item in items:
            data = item.get("string_map_data", {})
            account_name = data.get("Username", {}).get("value", None),
            if "Time" in data:
                timestamp = data.get("Time", {}).get("timestamp", "")
            else:
                timestamp = data.get("Tijd", {}).get("timestamp", "")

            datapoints.append((
                account_name,
                eh.epoch_to_iso(timestamp)
            ))
        out = pd.DataFrame(datapoints, columns=["Account name", "Date"]) # pyright: ignore
        out = out.sort_values(by="Date", key=eh.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def ads_viewed_to_df(instagram_zip: str) -> pd.DataFrame:

    b = eh.extract_file_from_zip(instagram_zip, "ads_viewed.json")
    d = eh.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["impressions_history_ads_seen"] # pyright: ignore
        for item in items:
            data = item.get("string_map_data", {})
            account_name = data.get("Author", {}).get("value", None)
            if "Time" in data:
                timestamp = data.get("Time", {}).get("timestamp", "")
            else:
                timestamp = data.get("Tijd", {}).get("timestamp", "")

            datapoints.append((
                account_name,
                eh.epoch_to_iso(timestamp)
            ))
        out = pd.DataFrame(datapoints, columns=["Author of ad", "Date"]) # pyright: ignore
        out = out.sort_values(by="Date", key=eh.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def posts_viewed_to_df(instagram_zip: str) -> pd.DataFrame:

    b = eh.extract_file_from_zip(instagram_zip, "posts_viewed.json")
    d = eh.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["impressions_history_posts_seen"] # pyright: ignore
        for item in items:
            data = item.get("string_map_data", {})
            account_name = data.get("Author", {}).get("value", None)
            if "Time" in data:
                timestamp = data.get("Time", {}).get("timestamp", "")
            else:
                timestamp = data.get("Tijd", {}).get("timestamp", "")

            datapoints.append((
                account_name,
                eh.epoch_to_iso(timestamp)
            ))
        out = pd.DataFrame(datapoints, columns=["Author", "Date"]) # pyright: ignore
        out = out.sort_values(by="Date", key=eh.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def posts_not_interested_in_to_df(instagram_zip: str) -> pd.DataFrame:

    b = eh.extract_file_from_zip(instagram_zip, "posts_you're_not_interested_in.json")
    data = eh.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data["impressions_history_posts_not_interested"] # pyright: ignore
        for item in items:
            d = eh.dict_denester(item.get("string_list_data"))
            datapoints.append((
                eh.fix_latin1_string(eh.find_item(d, "value")),
                eh.find_item(d, "href"),
                eh.epoch_to_iso(eh.find_item(d, "timestamp"))
            ))
        out = pd.DataFrame(datapoints, columns=["Post", "Link", "Date"]) # pyright: ignore
        out = out.sort_values(by="Date", key=eh.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def videos_watched_to_df(instagram_zip: str) -> pd.DataFrame:

    b = eh.extract_file_from_zip(instagram_zip, "videos_watched.json")
    d = eh.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["impressions_history_videos_watched"] # pyright: ignore
        for item in items:
            data = item.get("string_map_data", {})
            account_name = data.get("Author", {}).get("value", None)
            if "Time" in data:
                timestamp = data.get("Time", {}).get("timestamp", "")
            else:
                timestamp = data.get("Tijd", {}).get("timestamp", "")

            datapoints.append((
                account_name,
                eh.epoch_to_iso(timestamp)
            ))
        out = pd.DataFrame(datapoints, columns=["Author", "Date"]) # pyright: ignore
        out = out.sort_values(by="Date", key=eh.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def post_comments_to_df(instagram_zip: str) -> pd.DataFrame:
    """
    You can have 1 to n files of post_comments_<x>.json
    """

    out = pd.DataFrame()
    datapoints = []
    i = 1

    while True:
        b = eh.extract_file_from_zip(instagram_zip, f"post_comments_{i}.json")
        d = eh.read_json_from_bytes(b)

        if not d:
            break

        try:
            for item in d:
                data = item.get("string_map_data", {})
                media_owner = data.get("Media Owner", {}).get("value", "")
                comment = data.get("Comment", {}).get("value", "")
                if "Time" in data:
                    timestamp = data.get("Time", {}).get("timestamp", "")
                else:
                    timestamp = data.get("Tijd", {}).get("timestamp", "")

                datapoints.append((
                    media_owner,
                    eh.fix_latin1_string(comment),
                    eh.epoch_to_iso(timestamp)
                ))
            i += 1

        except Exception as e:
            logger.error("Exception caught: %s", e)
            return pd.DataFrame()

    out = pd.DataFrame(datapoints, columns=["Media Owner", "Comment", "Date"]) # pyright: ignore

    return out



def following_to_df(instagram_zip: str) -> pd.DataFrame:

    b = eh.extract_file_from_zip(instagram_zip, "following.json")
    data = eh.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data["relationships_following"] # pyright: ignore
        for item in items:
            d = eh.dict_denester(item)
            datapoints.append((
                eh.fix_latin1_string(eh.find_item(d, "value")),
                eh.find_item(d, "href"),
                eh.epoch_to_iso(eh.find_item(d, "timestamp"))
            ))
        out = pd.DataFrame(datapoints, columns=["Account", "Link", "Date"]) # pyright: ignore
        out = out.sort_values(by="Date", key=eh.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def liked_comments_to_df(instagram_zip: str) -> pd.DataFrame:

    b = eh.extract_file_from_zip(instagram_zip, "liked_comments.json")
    data = eh.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data["likes_comment_likes"] #pyright: ignore
        for item in items:
            d = eh.dict_denester(item)
            datapoints.append((
                eh.fix_latin1_string(eh.find_item(d, "title")),
                eh.fix_latin1_string(eh.find_item(d, "value")),
                eh.find_items(d, "href"),
                eh.epoch_to_iso(eh.find_item(d, "timestamp"))
            ))
        out = pd.DataFrame(datapoints, columns=["Account name", "Value", "Link", "Date"]) # pyright: ignore
        out = out.sort_values(by="Date", key=eh.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def liked_posts_to_df(instagram_zip: str) -> pd.DataFrame:

    b = eh.extract_file_from_zip(instagram_zip, "liked_posts.json")
    data = eh.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data["likes_media_likes"] #pyright: ignore
        for item in items:
            d = eh.dict_denester(item)
            datapoints.append((
                eh.fix_latin1_string(eh.find_item(d, "title")),
                eh.fix_latin1_string(eh.find_item(d, "value")),
                eh.find_items(d, "href"),
                eh.epoch_to_iso(eh.find_item(d, "timestamp"))
            ))
        out = pd.DataFrame(datapoints, columns=["Account name", "Value", "Link", "Date"]) # pyright: ignore
        out = out.sort_values(by="Date", key=eh.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def extraction(instagram_zip: str) -> list[d3i_props.PropsUIPromptConsentFormTableViz]:
    tables = [
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="instagram_posts_viewed",
            data_frame=posts_viewed_to_df(instagram_zip),
            title=props.Translatable({
                "en": "Posts viewed on Instagram",
                "nl": "Berichten bekeken op Instagram"
            }),
            description=props.Translatable({
                "en": "In this table you find the accounts of posts you viewed on Instagram sorted over time. Below, you find visualizations of different parts of this table. First, you find a timeline showing you the number of posts you viewed over time. Next, you find a histogram indicating how many posts you have viewed per hour of the day.",
                "nl": "In deze tabel zie je de accounts van berichten die je op Instagram hebt bekeken, gesorteerd op tijd. Hieronder vind je visualisaties van verschillende onderdelen van deze tabel. Eerst zie je een tijdlijn met het aantal berichten dat je in de loop van de tijd hebt bekeken. Daarna zie je een histogram dat aangeeft hoeveel berichten je per uur van de dag hebt bekeken."
            }),
            visualizations=[
                {
                    "title": {
                        "en": "The total number of Instagram posts you viewed over time",
                        "nl": "Het totale aantal Instagram-berichten dat je in de loop van de tijd hebt bekeken"
                    },
                    "type": "area",
                    "group": {
                        "column": "Date",
                        "dateFormat": "auto",
                    },
                    "values": [{
                        "label": "Count",
                        "aggregate": "count",
                    }]
                },
                {
                    "title": {
                        "en": "The total number of Instagram posts you have viewed per hour of the day",
                        "nl": "Het totale aantal Instagram-berichten dat je per uur van de dag hebt bekeken"
                    },
                    "type": "bar",
                    "group": {
                        "column": "Date",
                        "dateFormat": "hour_cycle",
                        "label": "Hour of the day",
                    },
                    "values": [{
                        "label": "Count"
                    }]
                }
            ]
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="instagram_videos_watched",
            data_frame=videos_watched_to_df(instagram_zip),
            title=props.Translatable({
                "en": "Videos watched on Instagram",
                "nl": "Video's bekeken op Instagram"
            }),
            description=props.Translatable({
                "en": "In this table you find the accounts of videos you watched on Instagram sorted over time. Below, you find a timeline showing you the number of videos you watched over time.",
                "nl": "In deze tabel zie je de accounts van video's die je op Instagram hebt bekeken, gesorteerd op tijd. Hieronder zie je een tijdlijn met het aantal video's dat je in de loop van de tijd hebt bekeken."
            }),
            visualizations=[
                {
                    "title": {
                        "en": "The total number of videos watched on Instagram over time",
                        "nl": "Het totale aantal video's dat je op Instagram hebt bekeken in de loop van de tijd"
                    },
                    "type": "area",
                    "group": {
                        "column": "Date",
                        "dateFormat": "auto"
                    },
                    "values": [{
                        "aggregate": "count",
                        "label": "Count"
                    }]
                }
            ]
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="instagram_post_comments",
            data_frame=post_comments_to_df(instagram_zip),
            title=props.Translatable({
                "en": "Comments on Instagram posts",
                "nl": "Reacties op Instagram-berichten",
            }),
            description=props.Translatable({
                "en": "In this table, you find the comments that you left behind on Instagram posts sorted over time. Below, you find a wordcloud, where the size of the word indicates how frequently that word has been used in these comments.",
                "nl": "In deze tabel zie je de reacties die je hebt achtergelaten op Instagram-berichten, gesorteerd op tijd. Hieronder zie je een woordwolk waarin de grootte van een woord aangeeft hoe vaak het is gebruikt in deze reacties."
            }),
            visualizations=[
                {
                    "title": {
                        "en": "Most common words in comments on posts",
                        "nl": "Meest gebruikte woorden in reacties op berichten"
                    },
                    "type": "wordcloud",
                    "textColumn": "Comment",
                    "tokenize": True,
                }
            ]
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="instagram_accounts_not_interested_in",
            data_frame=accounts_not_interested_in_to_df(instagram_zip),
            title=props.Translatable({
                "en": "Instagram accounts not interested in",
                "nl": "Instagram-accounts waarin je geen interesse hebt"
            }),
            description=props.Translatable({
                "en": "",
                "nl": ""
            }),
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="instagram_ads_viewed",
            data_frame=ads_viewed_to_df(instagram_zip),
            title=props.Translatable({
                "en": "Ads you viewed on Instagram",
                "nl": "Advertenties die je op Instagram hebt bekeken"
            }),
            description=props.Translatable({
                "en": "In this table, you find the ads that you viewed on Instagram sorted over time.",
                "nl": "In deze tabel zie je de advertenties die je op Instagram hebt bekeken, gesorteerd op tijd."
            }),
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="instagram_posts_not_interested_in",
            data_frame=posts_not_interested_in_to_df(instagram_zip),
            title=props.Translatable({
                "en": "Instagram posts not interested in",
                "nl": "Instagram-berichten waarin je geen interesse hebt"
            }),
            description=props.Translatable({
                "en": "",
                "nl": ""
            }),
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="instagram_following",
            data_frame=following_to_df(instagram_zip),
            title=props.Translatable({
                "en": "Accounts that you follow on Instagram",
                "nl": "Accounts die je volgt op Instagram"
            }),
            description=props.Translatable({
                "en": "In this table, you find the accounts that you follow on Instagram.",
                "nl": "In deze tabel zie je de accounts die je volgt op Instagram."
            }),
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="instagram_liked_comments",
            data_frame=liked_comments_to_df(instagram_zip),
            title=props.Translatable({
                "en": "Instagram liked comments",
                "nl": "Instagram-reacties die je leuk vond"
            }),
            description=props.Translatable({
                "en": "",
                "nl": ""
            }),
            visualizations=[
                {
                    "title": {
                        "en": "Accounts who's comments you liked most",
                        "nl": "Accounts waarvan je de reacties het vaakst leuk vond"
                    },
                    "type": "wordcloud",
                    "textColumn": "Account name",
                    "tokenize": False,
                }
            ]
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="instagram_liked_posts",
            data_frame=liked_posts_to_df(instagram_zip),
            title=props.Translatable({
                "en": "Instagram liked posts",
                "nl": "Instagram-berichten die je leuk vond"
            }),
            description=props.Translatable({
                "en": "",
                "nl": ""
            }),
            visualizations=[
                {
                    "title": {
                        "en": "Most liked accounts",
                        "nl": "Meest gelikete accounts"
                    },
                    "type": "wordcloud",
                    "textColumn": "Account name",
                    "tokenize": False,
                }
            ]
        )
    ]

    return [table for table in tables if not table.data_frame.empty]


class InstagramFlow(FlowBuilder):
    def __init__(self, session_id: int):
        super().__init__(session_id, "Instagram")
        
    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)
        
    def extract_data(self, file_value, validation):
        return extraction(file_value)


def process(session_id):
    flow = InstagramFlow(session_id)
    return flow.start_flow()
