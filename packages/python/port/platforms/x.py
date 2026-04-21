"""
X

This module contains an example flow of a X data donation study

Assumptions:
It handles DDPs in the english language with filetype js.

Configuration
-------------
The ``extraction`` function is driven by ``port_config.json``.  Generate one with::

    pnpm generate-config x

Each extractor function carries its own table config in a ``Table config::``
JSON block inside its docstring.  The generator reads those blocks and
assembles the JSON file.

Platform info::

    {
        "name": "X",
        "filetypes": ["js"],
        "languages": ["en", "nl"],
        "description": "Handles DDPs in English. DDP files use the .js extension with a JavaScript variable assignment prefix. These data donation flows have not been tested yet, if you find anything wrong with them report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "not yet implemented"
    }
"""

import logging
from collections import Counter
import json
import io
import re
from typing import Any, Callable

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

    return out


def ad_engagement_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract ad engagement data from X (Twitter).

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
        Columns: ``Text``, ``Impression time``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one advertisement the participant engaged with on X (Twitter), including the tweet text and the time of the impression.",
          "source_file": "ad-engagements.js",
          "columns": {
            "Text": "Text of the promoted tweet shown to the participant.",
            "Impression time": "Timestamp of when the ad impression occurred."
          }
        }

    Table config::

        {
          "id": "x_ad_engagement",
          "title": {
            "en": "Your engagement with ads",
            "nl": "Je interactie met advertenties"
          },
          "description": {
            "en": "Shows data about your interactions with advertisements on the platform",
            "nl": "Toont gegevens over uw interacties met advertenties op het platform"
          },
          "headers": {
            "Text": {"en": "Text", "nl": "Tekst"},
            "Impression time": {"en": "Impression time", "nl": "Impressietijd"}
          }
        }
    """
    result = reader.raw("ad-engagements.js")
    if not result.found:
        return pd.DataFrame()
    items = bytesio_to_listdict(result.data)

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
        errors[type(e).__name__] += 1

    return out


def personalization_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract X (Twitter) personalization interests.

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
        Columns: ``Interest``, ``is disabled``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one interest category X (Twitter) has associated with the participant's account for personalization purposes.",
          "source_file": "personalization.js",
          "columns": {
            "Interest": "Name of the interest category.",
            "is disabled": "Whether this interest is disabled for personalization."
          }
        }

    Table config::

        {
          "id": "x_personalization",
          "title": {
            "en": "Your personalization",
            "nl": "Je personalisatie"
          },
          "description": {
            "en": "Information about your personalization settings and preferences",
            "nl": "Informatie over uw personalisatie-instellingen en voorkeuren"
          },
          "headers": {
            "Interest": {"en": "Interest", "nl": "Interesse"},
            "is disabled": {"en": "Is disabled", "nl": "Uitgeschakeld"}
          }
        }
    """
    result = reader.raw("personalization.js")
    if not result.found:
        return pd.DataFrame()
    items = bytesio_to_listdict(result.data)

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
        errors[type(e).__name__] += 1

    return out


def follower_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the participant's X (Twitter) followers.

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
        Columns: ``Link to user``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one account that follows the participant on X (Twitter).",
          "source_file": "follower.js",
          "columns": {
            "Link to user": "URL link to the follower's X profile."
          }
        }

    Table config::

        {
          "id": "x_follower",
          "title": {"en": "Your followers", "nl": "Je volgers"},
          "description": {
            "en": "List of accounts that follow your profile",
            "nl": "Lijst van accounts die jouw profiel volgen"
          },
          "headers": {
            "Link to user": {"en": "Link to user", "nl": "Link naar gebruiker"}
          }
        }
    """
    datapoints = []
    out = pd.DataFrame()

    result = reader.raw("follower.js")
    if not result.found:
        return pd.DataFrame()
    ld = bytesio_to_listdict(result.data)

    try:
        for item in ld:
            datapoints.append((
                item.get("follower", {}).get("userLink", None)
            ))
        out = pd.DataFrame(datapoints, columns=["Link to user"]) # pyright: ignore
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def following_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the accounts the participant follows on X (Twitter).

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
        Columns: ``Link to user``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one account the participant follows on X (Twitter).",
          "source_file": "following.js",
          "columns": {
            "Link to user": "URL link to the followed account's X profile."
          }
        }

    Table config::

        {
          "id": "x_following",
          "title": {"en": "Accounts you follow", "nl": "Accounts die je volgt"},
          "description": {
            "en": "List of accounts that you are following",
            "nl": "Lijst van accounts die je volgt"
          },
          "headers": {
            "Link to user": {"en": "Link to user", "nl": "Link naar gebruiker"}
          }
        }
    """
    datapoints = []
    out = pd.DataFrame()

    result = reader.raw("following.js")
    if not result.found:
        return pd.DataFrame()
    ld = bytesio_to_listdict(result.data)

    try:
        for item in ld:
            datapoints.append((
                item.get("following", {}).get("userLink", None)
            ))
        out = pd.DataFrame(datapoints, columns=["Link to user"]) # pyright: ignore
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def like_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the participant's liked posts on X (Twitter).

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
        Columns: ``Tweet Id``, ``Tweet``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one post the participant liked on X (Twitter), including a link to the tweet and its full text.",
          "source_file": "like.js",
          "columns": {
            "Tweet Id": "URL link to the liked tweet.",
            "Tweet": "Full text of the liked tweet."
          }
        }

    Table config::

        {
          "id": "x_like",
          "title": {"en": "Posts that you liked", "nl": "Berichten die je leuk vond"},
          "description": {
            "en": "Posts that you've marked as liked",
            "nl": "Berichten die je hebt geliked"
          },
          "headers": {
            "Tweet Id": {"en": "Tweet Id", "nl": "Tweet-id"},
            "Tweet": {"en": "Tweet", "nl": "Tweet"}
          },
          "visualizations": [
            {
              "title": {
                "en": "Words in Tweets you liked, larger words mean they occur more often",
                "nl": "Woorden in Tweets die je leuk vond, grotere woorden komen vaker voor"
              },
              "type": "wordcloud",
              "textColumn": "Tweet",
              "tokenize": true
            }
          ]
        }
    """
    datapoints = []
    out = pd.DataFrame()

    result = reader.raw("like.js")
    if not result.found:
        return pd.DataFrame()
    ld = bytesio_to_listdict(result.data)

    try:
        for item in ld:
            datapoints.append((
                item.get("like", {}).get("tweetId", None),
                item.get("like", {}).get("fullText", None)
            ))
        out = pd.DataFrame(datapoints, columns=["Tweet Id", "Tweet"]) #pyright: ignore
        out["Tweet Id"] = "https://twitter.com/a/status/" + out["Tweet Id"]
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def tweets_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the participant's tweets from X (Twitter).

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
        Columns: ``Date``, ``Tweet``, ``Retweeted``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one tweet posted by the participant on X (Twitter).",
          "source_file": "tweets.js",
          "columns": {
            "Date": "Timestamp of when the tweet was created.",
            "Tweet": "Full text of the tweet.",
            "Retweeted": "Whether the tweet was a retweet."
          }
        }

    Table config::

        {
          "id": "x_tweet",
          "title": {"en": "Your tweets", "nl": "Jouw Tweets"},
          "description": {
            "en": "Posts you have created on the platform",
            "nl": "Berichten die je hebt geplaatst op het platform"
          },
          "headers": {
            "Date": {"en": "Date", "nl": "Datum en tijd"},
            "Tweet": {"en": "Tweet", "nl": "Tweet"},
            "Retweeted": {"en": "Retweeted", "nl": "Geretweeted"}
          },
          "visualizations": [
            {
              "title": {
                "en": "Words in your Tweets, larger words mean they occur more often in your Tweets",
                "nl": "Woorden in je Tweets, grotere woorden komen vaker voor"
              },
              "type": "wordcloud",
              "textColumn": "Tweet",
              "tokenize": true
            }
          ]
        }
    """
    datapoints = []
    out = pd.DataFrame()

    result = reader.raw("tweets.js")
    if not result.found:
        return pd.DataFrame()
    ld = bytesio_to_listdict(result.data)

    try:
        for item in ld:
            datapoints.append((
                item.get("tweet", {}).get("created_at", None),
                item.get("tweet", {}).get("full_text", None),
                str(item.get("tweet", {}).get("retweeted", ""))
            ))
        out = pd.DataFrame(datapoints, columns=["Date", "Tweet", "Retweeted"]) #pyright: ignore
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def block_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the accounts the participant has blocked on X (Twitter).

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
        Columns: ``Blocked users``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one account the participant has blocked on X (Twitter).",
          "source_file": "block.js",
          "columns": {
            "Blocked users": "URL link to the blocked account's X profile."
          }
        }

    Table config::

        {
          "id": "x_block",
          "title": {"en": "Accounts you blocked", "nl": "Accounts die je hebt geblokkeerd"},
          "description": {
            "en": "List of accounts you have blocked",
            "nl": "Lijst van accounts die je hebt geblokkeerd"
          },
          "headers": {
            "Blocked users": {"en": "Blocked users", "nl": "Geblokkeerde gebruikers"}
          }
        }
    """
    result = reader.raw("block.js")
    if not result.found:
        return pd.DataFrame()
    ld = bytesio_to_listdict(result.data)

    datapoints = []
    out = pd.DataFrame()

    try:
        for item in ld:
            datapoints.append((
                item.get("blocking", {}).get("userLink", "")
            ))
        out = pd.DataFrame(datapoints, columns=["Blocked users"]) # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def mute_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the accounts the participant has muted on X (Twitter).

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
        Columns: ``Muted users``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one account the participant has muted on X (Twitter).",
          "source_file": "mute.js",
          "columns": {
            "Muted users": "URL link to the muted account's X profile."
          }
        }

    Table config::

        {
          "id": "x_mute",
          "title": {"en": "Accounts you muted", "nl": "Accounts die je hebt gedempt"},
          "description": {
            "en": "List of accounts you have muted",
            "nl": "Lijst van accounts die je hebt gedempt"
          },
          "headers": {
            "Muted users": {"en": "Muted users", "nl": "Gedempte gebruikers"}
          }
        }
    """
    datapoints = []
    out = pd.DataFrame()

    result = reader.raw("mute.js")
    if not result.found:
        return pd.DataFrame()
    ld = bytesio_to_listdict(result.data)

    try:
        for item in ld:
            datapoints.append((
                item.get("muting", {}).get("userLink", "")
            ))
        out = pd.DataFrame(datapoints, columns=["Muted users"]) # pyright: ignore
    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def tweet_headers_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract tweet header metadata from X (Twitter).

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
        Columns: ``Tweet id``, ``User id``, ``Created at``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents metadata for one tweet posted by the participant, without the full tweet text.",
          "source_file": "tweet-headers.js",
          "columns": {
            "Tweet id": "Unique identifier of the tweet.",
            "User id": "Unique identifier of the tweet author.",
            "Created at": "Timestamp of when the tweet was created."
          }
        }

    Table config::

        {
          "id": "x_tweet_headers",
          "title": {"en": "Tweet headers", "nl": "Tweet-headers"},
          "description": {
            "en": "Metadata information about your tweets",
            "nl": "Metadata-informatie over uw tweets"
          },
          "headers": {
            "Tweet id": {"en": "Tweet id", "nl": "Tweet-id"},
            "User id": {"en": "User id", "nl": "Gebruikers-id"},
            "Created at": {"en": "Created at", "nl": "Aangemaakt op"}
          }
        }
    """
    datapoints = []
    out = pd.DataFrame()

    result = reader.raw("tweet-headers.js")
    if not result.found:
        return pd.DataFrame()
    ld = bytesio_to_listdict(result.data)

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
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def user_link_clicks_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract links clicked by the participant on X (Twitter).

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
        Columns: ``Tweet id``, ``Link``, ``Datum en tijd``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one link the participant clicked while using X (Twitter).",
          "source_file": "user-link-clicks.js",
          "columns": {
            "Tweet id": "ID of the tweet containing the clicked link.",
            "Link": "The final URL that was clicked.",
            "Datum en tijd": "Timestamp of when the link was clicked."
          }
        }

    Table config::

        {
          "id": "x_user_link_clicks",
          "title": {"en": "Links you clicked", "nl": "Links die je hebt aangeklikt"},
          "description": {
            "en": "Record of links you've clicked on while using the platform",
            "nl": "Overzicht van links waarop je hebt geklikt tijdens het gebruik van het platform"
          },
          "headers": {
            "Tweet id": {"en": "Tweet id", "nl": "Tweet-id"},
            "Link": {"en": "Link", "nl": "Link"},
            "Datum en tijd": {"en": "Date", "nl": "Datum en tijd"}
          }
        }
    """
    datapoints = []
    out = pd.DataFrame()

    result = reader.raw("user-link-clicks.js")
    if not result.found:
        return pd.DataFrame()
    ld = bytesio_to_listdict(result.data)

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
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


# ---------------------------------------------------------------------------
# Extractor registry & platform info
# ---------------------------------------------------------------------------

#: Mapping from the string names used in port_config.json to actual extractor functions.
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "ad_engagement_to_df": ad_engagement_to_df,
    "follower_to_df": follower_to_df,
    "following_to_df": following_to_df,
    "block_to_df": block_to_df,
    "like_to_df": like_to_df,
    "tweets_to_df": tweets_to_df,
    "personalization_to_df": personalization_to_df,
    "mute_to_df": mute_to_df,
    "tweet_headers_to_df": tweet_headers_to_df,
    "user_link_clicks_to_df": user_link_clicks_to_df,
}


# ---------------------------------------------------------------------------
# Main extraction & flow
# ---------------------------------------------------------------------------

def extraction(x_zip: str, validation) -> ExtractionResult:
    """Extract data from an X (Twitter) DDP zip and return consent-form tables.

    Parameters
    ----------
    x_zip:
        Path to the X DDP zip archive on disk.
    validation:
        Validation result object whose ``archive_members`` attribute is passed
        to ``ZipArchiveReader``.
    """
    config = load_port_config(EXTRACTOR_REGISTRY)
    errors: Counter = Counter()
    reader = ZipArchiveReader(x_zip, validation.archive_members, errors)
    return run_extraction(reader, errors, config)


class XFlow(FlowBuilder):
    """Flow implementation for the X data donation study."""

    def __init__(self, session_id: str):
        super().__init__(session_id, "X")

    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)

    def extract_data(self, file_value, validation):
        return extraction(file_value, validation)


def process(session_id):
    flow = XFlow(session_id)
    return flow.start_flow()
