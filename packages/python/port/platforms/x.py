"""
X

This module contains an example flow of a X data donation study

Assumptions:
It handles DDPs in the english language with filetype js.
"""

import logging
import json
import io
import re
from typing import Any

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
            "account-creation-ip.js", "app.js", "community-tweet.js", "expanded-profile.js", "ni-devices.js", "professional-data.js", "tweet-headers.js", "account-label.js", "article-metadata.js", "connected-application.js", "follower.js", "note-tweet.js", "profile.js", "tweetdeck.js", "account-suspension.js", "article.js", "contact.js", "following.js", "periscope-account-information.js", "profile_media", "tweets.js", "account-timezone.js", "audio-video-calls-in-dm-recipient-sessions.js", "deleted-note-tweet.js", "grok-chat-item.js", "periscope-ban-information.js", "protected-history.js", "tweets_media", "account.js", "audio-video-calls-in-dm.js", "deleted-tweet-headers.js", "ip-audit.js", "periscope-broadcast-metadata.js", "README.txt", "twitter-shop.js", "ad-engagements.js", "block.js", "deleted-tweets.js", "key-registry.js", "periscope-comments-made-by-user.js", "reply-prompt.js", "user-link-clicks.js", "ad-impressions.js", "branch-links.js", "device-token.js", "like.js", "periscope-expired-broadcasts.js", "saved-search.js", "verified-organization.js", "ad-mobile-conversions-attributed.js", "catalog-item.js", "direct-message-group-headers.js", "lists-created.js", "periscope-followers.js", "screen-name-change.js", "verified.js", "ad-mobile-conversions-unattributed.js", "commerce-catalog.js", "direct-message-headers.js", "lists-member.js", "periscope-profile-description.js", "shop-module.js", "ad-online-conversions-attributed.js", "community-note-batsignal.js", "direct-message-mute.js", "lists-subscribed.js", "personalization.js", "shopify-account.js", "ad-online-conversions-unattributed.js", "community-note-rating.js", "direct-messages-group.js", "manifest.js", "phone-number.js", "smartblock.js", "ads-revenue-sharing.js", "community-note-tombstone.js", "direct-messages.js", "moment.js", "product-drop.js", "spaces-metadata.js", "ageinfo.js", "community-note.js", "email-address-change.js", "mute.js", "product-set.js", "sso.js",
        ],
    ),
]


def bytesio_to_listdict(bytes_to_read: io.BytesIO) -> list[dict[Any, Any]]:
    """
    Converts a io.BytesIO buffer containing a twitter.js file, to a list of dicts

    A list of dicts is the current structure of twitter.js files
    """

    out = []
    lines = []

    try:
        with io.TextIOWrapper(bytes_to_read, encoding="utf8") as f:
            lines = f.readlines()

        # change first line so its a valid json
        lines[0] = re.sub("^.*? = ", "", lines[0])

        # convert to a list of dicts
        out = json.loads("".join(lines))

    except json.decoder.JSONDecodeError as e:
        logger.error("The input buffer did not contain a valid JSON: %s", e)
    except IndexError as e:
        logger.error("No lines were read, could be empty input buffer: %s", e)
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    finally:
        return out


def ad_engagement_to_df(x_zip: str) -> pd.DataFrame:

    b = eh.extract_file_from_zip(x_zip, "ad-engagements.js")
    items = bytesio_to_listdict(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        for item in items:
            d = eh.dict_denester(item)
            datapoints.append((
                eh.find_item(d, "tweetText"),
                eh.find_item(d, "impressionTime"),
            ))
        out = pd.DataFrame(datapoints, columns=["Text", "Impression time"]) # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def personalization_to_df(x_zip: str) -> pd.DataFrame:

    b = eh.extract_file_from_zip(x_zip, "personalization.js")
    items = bytesio_to_listdict(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        l = items[0]["p13nData"]["interests"]["interests"]
        for item in l:
            d = eh.dict_denester(item)
            datapoints.append((
                eh.find_item(d, "name"),
                eh.find_item(d, "isDisabled"),
            ))
        out = pd.DataFrame(datapoints, columns=["Interest", "is disabled"]) # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def follower_to_df(x_zip: str) -> pd.DataFrame:
    """
    following.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = eh.extract_file_from_zip(x_zip, "follower.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("follower", {}).get("userLink", None)
            ))
        out = pd.DataFrame(datapoints, columns=["Link to user"]) # pyright: ignore
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out


def following_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    following.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = eh.extract_file_from_zip(twitter_zip, "following.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("following", {}).get("userLink", None)
            ))
        out = pd.DataFrame(datapoints, columns=["Link to user"]) # pyright: ignore
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out



def like_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    like.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = eh.extract_file_from_zip(twitter_zip, "like.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("like", {}).get("tweetId", None),
                item.get("like", {}).get("fullText", None)
            ))
        out = pd.DataFrame(datapoints, columns=["Tweet Id", "Tweet"]) #pyright: ignore
        out["Tweet Id"] = "https://twitter.com/a/status/" + out["Tweet Id"]
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out


def tweets_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    tweets.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = eh.extract_file_from_zip(twitter_zip, "/tweets.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("tweet", {}).get("created_at", None),
                item.get("tweet", {}).get("full_text", None),
                str(item.get("tweet", {}).get("retweeted", ""))
            ))
        out = pd.DataFrame(datapoints, columns=["Date", "Tweet", "Retweeted"]) #pyright: ignore
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out


def block_to_df(x_zip: str) -> pd.DataFrame:
    """
    block.js
    """

    b = eh.extract_file_from_zip(x_zip, "/block.js")
    ld = bytesio_to_listdict(b)

    datapoints = []
    out = pd.DataFrame()

    try:
        for item in ld:
            datapoints.append((
                item.get("blocking", {}).get("userLink", "")
            ))
        out = pd.DataFrame(datapoints, columns=["Blocked users"]) # pyright: ignore

    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out


def mute_to_df(twitter_zip: str) -> pd.DataFrame:
    """
    mute.js
    """

    datapoints = []
    out = pd.DataFrame()

    b = eh.extract_file_from_zip(twitter_zip, "mute.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            datapoints.append((
                item.get("muting", {}).get("userLink", "")
            ))
        out = pd.DataFrame(datapoints, columns=["Muted users"]) # pyright: ignore
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out


def tweet_headers_to_df(twitter_zip: str) -> pd.DataFrame:
    datapoints = []
    out = pd.DataFrame()

    b = eh.extract_file_from_zip(twitter_zip, "/tweet-headers.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            d = eh.dict_denester(item)
            datapoints.append((
                eh.find_item(d, "tweet_id"),
                eh.find_item(d, "user_id"),
                eh.find_item(d, "created_at"),
            ))

        out = pd.DataFrame(datapoints, columns=["Tweet id", "User id", "Created at"]) # pyright: ignore
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out


def user_link_clicks_to_df(twitter_zip: str) -> pd.DataFrame:
    datapoints = []
    out = pd.DataFrame()

    b = eh.extract_file_from_zip(twitter_zip, "/user-link-clicks.js")
    ld = bytesio_to_listdict(b)

    try:
        for item in ld:
            d = eh.dict_denester(item)
            datapoints.append((
                eh.find_item(d, "tweetId"),
                eh.find_item(d, "finalUrl"),
                eh.find_item(d, "timeStampOfInteraction"),
            ))

        out = pd.DataFrame(datapoints, columns=["Tweet id", "Link", "Datum en tijd"]) # pyright: ignore
    except Exception as e:
        logger.error("Exception was caught: %s", e)

    return out



def extraction(x_zip: str) -> list[d3i_props.PropsUIPromptConsentFormTableViz]:
    tables = [
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="x_ad_engagement",
            data_frame=ad_engagement_to_df(x_zip),
            title=props.Translatable({
                "en": "Your engagement with ads",
                "nl": "Ad engagement"
            }),
            description=props.Translatable({
                "en": "Shows data about your interactions with advertisements on the platform",
                "nl": "Toont gegevens over uw interacties met advertenties op het platform"
            })
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="x_follower",
            data_frame=follower_to_df(x_zip),
            title=props.Translatable({
                "en": "Your followers",
                "nl": "Follower"
            }),
            description=props.Translatable({
                "en": "List of accounts that follow your profile",
                "nl": "Lijst van accounts die jouw profiel volgen"
            })
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="x_following",
            data_frame=following_to_df(x_zip),
            title=props.Translatable({
                "en": "Accounts you follow",
                "nl": "Following"
            }),
            description=props.Translatable({
                "en": "List of accounts that you are following",
                "nl": "Lijst van accounts die je volgt"
            })
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="x_block",
            data_frame=block_to_df(x_zip),
            title=props.Translatable({
                "en": "Accounts you blocked",
                "nl": "Block"
            }),
            description=props.Translatable({
                "en": "List of accounts you have blocked",
                "nl": "Lijst van accounts die je hebt geblokkeerd"
            })
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="x_like",
            data_frame=like_to_df(x_zip),
            title=props.Translatable({
                "en": "Posts that you liked",
                "nl": "Like"
            }),
            description=props.Translatable({
                "en": "Posts that you've marked as liked",
                "nl": "Berichten die je hebt geliked"
            }),
            visualizations=[
                {
                    "title": {
                        "en": "Words in Tweets you liked, larger words mean they occur more often",
                        "nl": "Words in Tweets you liked, larger words mean they occur more often"
                    },
                    "type": "wordcloud",
                    "textColumn": "Tweet",
                    "tokenize": True,
                }
            ]
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="x_tweet",
            data_frame=tweets_to_df(x_zip),
            title=props.Translatable({
                "en": "Your tweets",
                "nl": "Jouw Tweets"
            }),
            description=props.Translatable({
                "en": "Posts you have created on the platform",
                "nl": "Berichten die je hebt geplaatst op het platform"
            }),
            visualizations=[
                {
                    "title": {
                        "en": "Words in your Tweets, larger words mean they occur more often in your Tweets",
                        "nl": "Words in your Tweets, larger words mean they occur more often in your Tweets"
                    },
                    "type": "wordcloud",
                    "textColumn": "Tweet",
                    "tokenize": True,
                }
            ]
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="x_personalization",
            data_frame=personalization_to_df(x_zip),
            title=props.Translatable({
                "en": "Your personalization",
                "nl": "Personalization"
            }),
            description=props.Translatable({
                "en": "Information about your personalization settings and preferences",
                "nl": "Informatie over uw personalisatie-instellingen en voorkeuren"
            })
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="x_mute",
            data_frame=mute_to_df(x_zip),
            title=props.Translatable({
                "en": "Accounts you muted",
                "nl": "Mute"
            }),
            description=props.Translatable({
                "en": "List of accounts you have muted",
                "nl": "Lijst van accounts die je hebt gedempt"
            })
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="x_tweet_headers",
            data_frame=tweet_headers_to_df(x_zip),
            title=props.Translatable({
                "en": "Tweet headers",
                "nl": "Tweet headers"
            }),
            description=props.Translatable({
                "en": "Metadata information about your tweets",
                "nl": "Metadata-informatie over uw tweets"
            })
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="x_user_link_clicks",
            data_frame=user_link_clicks_to_df(x_zip),
            title=props.Translatable({
                "en": "Links you clicked",
                "nl": "User link clicks"
            }),
            description=props.Translatable({
                "en": "Record of links you've clicked on while using the platform",
                "nl": "Overzicht van links waarop je hebt geklikt tijdens het gebruik van het platform"
            })
        )
    ]

    # Filter out tables with empty dataframes
    return [table for table in tables if not table.data_frame.empty]



class XFlow(FlowBuilder):
    def __init__(self, session_id: int):
        super().__init__(session_id, "X")
        
    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)
        
    def extract_data(self, file_value, validation):
        return extraction(file_value)


def process(session_id):
    flow = XFlow(session_id)
    return flow.start_flow()
