"""
Facebook

This module contains an example flow of a Facebook data donation study

Assumptions:
It handles DDPs in the english language with filetype JSON.

Configuration
-------------
The ``extraction`` function is driven by ``port_config.json``.  Generate one with::

    pnpm generate-config facebook

Each extractor function carries its own table config in a ``Table config::``
JSON block inside its docstring.  The generator reads those blocks and
assembles the JSON file.

Platform info::

    {
        "name": "Facebook",
        "filetypes": ["json"],
        "languages": ["en", "nl"],
        "description": "Handles DDPs in English. These data donation flows have not been tested yet, if you find anything wrong with them report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "not yet implemented"
    }
"""

import logging
from collections import Counter
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
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
"subscription_for_no_ads.json", "other_categories_used_to_reach_you.json", "ads_feedback_activity.json", "ads_personalization_consent.json", "advertisers_you've_interacted_with.json", "advertisers_using_your_activity_or_information.json", "story_views_in_past_7_days.json", "ad_preferences.json", "groups_you've_searched_for.json", "your_search_history.json", "primary_public_location.json", "timezone.json", "primary_location.json", "your_privacy_jurisdiction.json", "people_and_friends.json", "ads_interests.json", "notifications.json", "notification_of_meta_privacy_policy_update.json", "recently_viewed.json", "recently_visited.json", "your_avatar.json", "meta_avatars_post_backgrounds.json", "contacts_sync_settings.json", "timezone.json", "autofill_information.json", "profile_information.json", "profile_update_history.json", "your_transaction_survey_information.json", "your_recently_followed_history.json", "your_recently_used_emojis.json", "no-data.txt", "navigation_bar_activity.json", "pages_and_profiles_you_follow.json", "pages_you've_liked.json", "your_saved_items.json", "fundraiser_posts_you_likely_viewed.json", "your_fundraiser_donations_information.json", "your_event_responses.json", "event_invitations.json", "your_event_invitation_links.json", "likes_and_reactions_1.json", "your_uncategorized_photos.json", "payment_history.json", "no-data.txt", "your_answers_to_membership_questions.json", "your_group_membership_activity.json", "your_contributions.json", "group_posts_and_comments.json", "your_comments_in_groups.json", "instant_games.json", "your_page_or_groups_badges.json", "instant_games_usage_data.json", "no-data.txt", "who_you've_followed.json", "people_you_may_know.json", "received_friend_requests.json", "your_friends.json", "likes_and_reactions.json", "controls.json",
        ],
    ),
]


def who_youve_followed_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the list of profiles and pages you follow on Facebook.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Name``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook profile or page that the participant follows, including the name and the time they started following.",
          "source_file": "who_you_ve_followed.json",
          "columns": {
            "Name": "Name of the followed profile or page.",
            "Timestamp": "ISO 8601 timestamp of when the participant started following."
          }
        }

    Table config::

        {
          "id": "facebook_who_youve_followed",
          "title": {
            "en": "Who you follow",
            "nl": "Wie je volgt"
          },
          "description": {
            "en": "This table shows the Facebook profiles and pages you currently follow.",
            "nl": "Deze tabel toont de Facebook-profielen en -pagina's die je momenteel volgt."
          },
          "headers": {
            "Name": {"en": "Name", "nl": "Naam"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json("who_you_ve_followed.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["following_v3"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item.get("name", "")),
                eh.epoch_to_iso(item.get("timestamp", {}), errors=errors)
            ))

        out = pd.DataFrame(datapoints, columns=["Name", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def news_your_locations_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the locations Facebook News is configured to show.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Location``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a geographical location for which the participant's Facebook News feed is configured.",
          "source_file": "facebook_news/your_locations.json",
          "columns": {
            "Location": "Name of the configured location."
          }
        }

    Table config::

        {
          "id": "facebook_news_your_locations",
          "title": {
            "en": "The locations Facebook news is set to",
            "nl": "De locaties waar Facebook Nieuws op is ingesteld"
          },
          "description": {
            "en": "This table displays the geographical locations for which your Facebook News feed is configured.",
            "nl": "Deze tabel toont de geografische locaties waarvoor je Facebook Nieuwsfeed is geconfigureerd."
          },
          "headers": {
            "Location": {"en": "Location", "nl": "Locatie"}
          }
        }
    """
    result = reader.json("facebook_news/your_locations.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["news_your_locations_v2"]  # pyright: ignore
        for item in items:
            datapoints.append(
                item
            )
        out = pd.DataFrame(datapoints, columns=["Location"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def notifications_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract Facebook notifications history.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Text``, ``Link``, ``Read``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a notification the participant received from Facebook, including the text, link, read status, and timestamp.",
          "source_file": "notifications/notifications.json",
          "columns": {
            "Text": "Text content of the notification.",
            "Link": "URL the notification links to.",
            "Read": "Whether the notification was read.",
            "Date": "ISO 8601 timestamp of the notification."
          }
        }

    Table config::

        {
          "id": "facebook_notifications",
          "title": {
            "en": "Notifications Facebook sent you",
            "nl": "Notificaties die Facebook je stuurde"
          },
          "description": {
            "en": "This table contains a history of the notifications you've received from Facebook.",
            "nl": "Deze tabel bevat een overzicht van de notificaties die je van Facebook hebt ontvangen."
          },
          "headers": {
            "Text": {"en": "Text", "nl": "Tekst"},
            "Link": {"en": "Link", "nl": "Link"},
            "Read": {"en": "Read", "nl": "Gelezen"},
            "Date": {"en": "Date", "nl": "Datum"}
          }
        }
    """
    result = reader.json("notifications/notifications.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["notifications_v2"]  # pyright: ignore
        for item in items:
            denested_dict = eh.dict_denester(item)
            datapoints.append((
                eh.find_item(denested_dict, "text"),
                eh.find_item(denested_dict, "href"),
                eh.find_item(denested_dict, "unread"),
                eh.epoch_to_iso(eh.find_item(denested_dict, "timestamp"), errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Text", "Link", "Read", "Date"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def content_sharing_you_have_created_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract content sharing links you have created on Facebook.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Link``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents an external link the participant shared on Facebook, including the URL and date.",
          "source_file": "content_sharing_links_you_have_created.json",
          "columns": {
            "Link": "URL of the shared link.",
            "Date": "ISO 8601 timestamp of when the link was shared."
          }
        }

    Table config::

        {
          "id": "facebook_content_sharing_links_you_created",
          "title": {
            "en": "Links you shared",
            "nl": "Links die je hebt gedeeld"
          },
          "description": {
            "en": "This table displays the external links you have shared on Facebook.",
            "nl": "Deze tabel toont de externe links die je op Facebook hebt gedeeld."
          },
          "headers": {
            "Link": {"en": "Link", "nl": "Link"},
            "Date": {"en": "Date", "nl": "Datum en Tijd"}
          }
        }
    """
    result = reader.json("content_sharing_links_you_have_created.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        for item in d:
            denested_dict = eh.dict_denester(item)
            datapoints.append((
                eh.find_item(denested_dict, "href"),
                eh.epoch_to_iso(eh.find_item(denested_dict, "timestamp"), errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Link", "Date"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def facebook_reels_usage_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract Facebook Reels usage information.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Reel interaction``, ``Value``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a type of interaction the participant had with Facebook Reels and its associated value.",
          "source_file": "facebook_reels_usage_information.json",
          "columns": {
            "Reel interaction": "Type of interaction with Facebook Reels.",
            "Value": "Value associated with the interaction."
          }
        }

    Table config::

        {
          "id": "facebook_reels_usage",
          "title": {
            "en": "Interactions with Facebook Reels",
            "nl": "Interacties met Facebook Reels"
          },
          "description": {
            "en": "This table shows your interactions with Facebook Reels, such as videos you've watched or engaged with.",
            "nl": "Deze tabel toont je interacties met Facebook Reels, zoals video's die je hebt bekeken of waarmee je hebt gecommuniceerd."
          },
          "headers": {
            "Reel interaction": {"en": "Reel interaction", "nl": "Interactie met reels"},
            "Value": {"en": "Value", "nl": "Waarde"}
          }
        }
    """
    result = reader.json("facebook_reels_usage_information.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d.get("label_values", []) #pyright: ignore
        d = items[0]
        for item in d["dict"]:
            denested_dict = eh.dict_denester(item)
            datapoints.append((
                eh.find_item(denested_dict, "label"),
                eh.find_item(denested_dict, "value"),
            ))

        out = pd.DataFrame(datapoints, columns=["Reel interaction", "Value"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def last_28_days_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract how many videos you watched in the last 28 days on Facebook Watch.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Count``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Contains the number of videos the participant watched on Facebook in the past 28 days.",
          "source_file": "your_facebook_watch_activity_in_the_last_28_days.json",
          "columns": {
            "Count": "Number of videos watched in the last 28 days."
          }
        }

    Table config::

        {
          "id": "facebook_last_28",
          "title": {
            "en": "How many videos you watched in the last 28 days",
            "nl": "Hoeveel video's je de afgelopen 28 dagen hebt bekeken"
          },
          "description": {
            "en": "This table indicates the number of videos you have watched on Facebook in the past 28 days.",
            "nl": "Deze tabel geeft het aantal video's aan dat je de afgelopen 28 dagen op Facebook hebt bekeken."
          },
          "headers": {
            "Count": {"en": "Count", "nl": "Aantal"}
          }
        }
    """
    result = reader.json("your_facebook_watch_activity_in_the_last_28_days.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        denested_dict = eh.dict_denester(d)
        datapoints.append((
            eh.find_item(denested_dict, "-value"),
        ))

        out = pd.DataFrame(datapoints, columns=["Count"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def your_search_history_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract Facebook search history.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Search term``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a search query the participant made on Facebook, including the search term and date.",
          "source_file": "logged_information/search/your_search_history.json",
          "columns": {
            "Search term": "The search query entered by the participant.",
            "Date": "ISO 8601 timestamp of when the search was made."
          }
        }

    Table config::

        {
          "id": "facebook_search_history",
          "title": {
            "en": "Your search history",
            "nl": "Je zoekgeschiedenis"
          },
          "description": {
            "en": "This table contains a record of your search queries on Facebook.",
            "nl": "Deze tabel bevat een overzicht van je zoekopdrachten op Facebook."
          },
          "headers": {
            "Search term": {"en": "Search term", "nl": "Zoekterm"},
            "Date": {"en": "Date", "nl": "Datum"}
          },
          "visualizations": [
            {
              "title": {"en": "Terms you searched for", "nl": "Zoektermen waar je naar zocht"},
              "type": "wordcloud",
              "textColumn": "Search term",
              "tokenize": false
            }
          ]
        }
    """
    result = reader.json("logged_information/search/your_search_history.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["searches_v2"]  # pyright: ignore
        for item in items:
            denested_dict = eh.dict_denester(item)

            datapoints.append((
                eh.fix_latin1_string(eh.find_item(denested_dict, "text")),
                eh.epoch_to_iso(eh.find_item(denested_dict, "timestamp"), errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Search term", "Date"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def your_friends_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the number of Facebook friends.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Number of friends``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Contains the total number of friends the participant has on Facebook.",
          "source_file": "your_friends.json",
          "columns": {
            "Number of friends": "Total count of Facebook friends."
          }
        }

    Table config::

        {
          "id": "facebook_your_friends",
          "title": {
            "en": "Your friends on Facebook",
            "nl": "Je vrienden op Facebook"
          },
          "description": {
            "en": "This table lists your current friends on Facebook.",
            "nl": "Deze tabel toont je huidige vrienden op Facebook."
          },
          "headers": {
            "Number of friends": {"en": "Number of friends", "nl": "Aantal vrienden op facebook"}
          }
        }
    """
    result = reader.json("your_friends.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["friends_v2"]  # pyright: ignore
        datapoints.append((len(items)))

        out = pd.DataFrame(datapoints, columns=["Number of friends"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def ads_interests_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract Facebook ad interests.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Ad``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents an interest topic Facebook has associated with the participant for ad targeting purposes.",
          "source_file": "ads_interests.json",
          "columns": {
            "Ad": "Interest topic used for ad targeting."
          }
        }

    Table config::

        {
          "id": "facebook_ads_interests",
          "title": {
            "en": "Your ad interests",
            "nl": "Je advertentie-interesses"
          },
          "description": {
            "en": "This table shows the interests Facebook has identified for showing you personalized ads.",
            "nl": "Deze tabel toont de interesses die Facebook heeft geïdentificeerd om je gepersonaliseerde advertenties te tonen."
          },
          "headers": {
            "Ad": {"en": "Ad", "nl": "Advertentie"}
          }
        }
    """
    result = reader.json("ads_interests.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["topics_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item),
            ))
        out = pd.DataFrame(datapoints, columns=["Ad"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def recently_viewed_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract Facebook items recently viewed.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Category``, ``Name``, ``Link``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook post, video, or other item the participant recently viewed, including the category, name, link, and date.",
          "source_file": "recently_viewed.json",
          "columns": {
            "Category": "Content category (e.g. Videos, Marketplace).",
            "Name": "Name or title of the viewed item.",
            "Link": "URL of the viewed item.",
            "Date": "ISO 8601 timestamp of when the item was viewed."
          }
        }

    Table config::

        {
          "id": "facebook_recently_viewed",
          "title": {
            "en": "Facebook items you recently viewed",
            "nl": "Facebook items die je recentelijk hebt bekeken"
          },
          "description": {
            "en": "This table shows the Facebook posts, videos, and other items you have recently viewed.",
            "nl": "Deze tabel toont de Facebook-posts, video's en andere items die je recentelijk hebt bekeken."
          },
          "headers": {
            "Category": {"en": "Category", "nl": "Categorie"},
            "Name": {"en": "Name", "nl": "Naam"},
            "Link": {"en": "Link", "nl": "Link"},
            "Date": {"en": "Date", "nl": "Datum"}
          }
        }
    """
    result = reader.json("recently_viewed.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["recently_viewed"] # pyright: ignore
        for item in items:

            if "entries" in item:
                for entry in item["entries"]:
                    datapoints.append((
                        eh.fix_latin1_string(item.get("name", "")),
                        eh.fix_latin1_string(entry.get("data", {}).get("name", "")),
                        entry.get("data", {}).get("uri", ""),
                        eh.epoch_to_iso(entry.get("timestamp", ""), errors=errors)
                    ))

            # The nesting goes deeper
            if "children" in item:
                for child in item["children"]:
                    for entry in child["entries"]:
                        datapoints.append((
                            eh.fix_latin1_string(child.get("name", "")),
                            eh.fix_latin1_string(entry.get("data", {}).get("name", "")),
                            entry.get("data", {}).get("uri", ""),
                            eh.epoch_to_iso(entry.get("timestamp", ""), errors=errors)
                        ))

        out = pd.DataFrame(datapoints, columns=["Category", "Name", "Link", "Date"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def recently_visited_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract Facebook profiles recently visited.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Category``, ``Name``, ``Link``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook profile or page the participant recently visited, including the category, name, link, and date.",
          "source_file": "recently_visited.json",
          "columns": {
            "Category": "Category of the visited item.",
            "Name": "Name or title of the visited profile or page.",
            "Link": "URL of the visited profile or page.",
            "Date": "ISO 8601 timestamp of when the visit occurred."
          }
        }

    Table config::

        {
          "id": "facebook_recently_visited",
          "title": {
            "en": "Profiles you visited recently",
            "nl": "Profielen die je recentelijk hebt bezocht"
          },
          "description": {
            "en": "This table lists the Facebook profiles you have visited most recently.",
            "nl": "Deze tabel toont de Facebook-profielen die je recentelijk hebt bezocht."
          },
          "headers": {
            "Category": {"en": "Category", "nl": "Categorie"},
            "Name": {"en": "Name", "nl": "Naam"},
            "Link": {"en": "Link", "nl": "Link"},
            "Date": {"en": "Date", "nl": "Datum"}
          }
        }
    """
    result = reader.json("recently_visited.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["visited_things_v2"]  # pyright: ignore
        for item in items:
            if "entries" in item:
                for entry in item["entries"]:
                    datapoints.append((
                        item.get("name", ""),
                        eh.fix_latin1_string(entry.get("data", {}).get("name", "")),
                        entry.get("data", {}).get("uri", ""),
                        eh.epoch_to_iso(entry.get("timestamp", ""), errors=errors)
                    ))

        out = pd.DataFrame(datapoints, columns=["Category", "Name", "Link", "Date"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def profile_update_history_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract Facebook profile update history.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a change the participant made to their Facebook profile, including the title of the change and the timestamp.",
          "source_file": "profile_update_history.json",
          "columns": {
            "Title": "Description of the profile change.",
            "Timestamp": "ISO 8601 timestamp of when the change was made."
          }
        }

    Table config::

        {
          "id": "facebook_profile_update_history",
          "title": {
            "en": "History of your profile updates",
            "nl": "Geschiedenis van je profielupdates"
          },
          "description": {
            "en": "This table contains a log of changes you've made to your Facebook profile information.",
            "nl": "Deze tabel bevat een logboek van de wijzigingen die je in je Facebook-profielinformatie hebt aangebracht."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json("profile_update_history.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["profile_updates_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item.get("title", "")),
                eh.epoch_to_iso(item.get("timestamp", ""), errors=errors)
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
    return out


def your_event_responses_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract Facebook event responses.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Name``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook event the participant responded to (going, interested, or declined), including the event name and start time.",
          "source_file": "your_event_responses.json",
          "columns": {
            "Name": "Name of the Facebook event.",
            "Timestamp": "ISO 8601 timestamp of the event start time."
          }
        }

    Table config::

        {
          "id": "facebook_your_event_responses",
          "title": {
            "en": "Your event responses",
            "nl": "Je reacties op evenementen"
          },
          "description": {
            "en": "This table contains your responses (going, interested, declined) to Facebook events.",
            "nl": "Deze tabel bevat je reacties (gaat, geïnteresseerd, afgewezen) op Facebook-evenementen."
          },
          "headers": {
            "Name": {"en": "Name", "nl": "Naam"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json("your_event_responses.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["event_responses_v2"]["events_joined"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item.get("name", "")),
                eh.epoch_to_iso(item.get("start_timestamp", ""), errors=errors)
            ))

        out = pd.DataFrame(datapoints, columns=["Name", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def group_posts_and_comments_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract posts and comments you made in Facebook groups.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Post``, ``Date``, ``URL``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a post or comment the participant made in a Facebook group, including the title, post content, date, and URL.",
          "source_file": "group_posts_and_comments.json",
          "columns": {
            "Title": "Title of the group post.",
            "Post": "Text content of the post.",
            "Date": "ISO 8601 timestamp of when the post was made.",
            "URL": "URL of the group post."
          }
        }

    Table config::

        {
          "id": "facebook_group_posts_and_comments",
          "title": {
            "en": "Your posts and comments in groups",
            "nl": "Je berichten en commentaren in groepen"
          },
          "description": {
            "en": "This table shows your posts and comments within Facebook groups.",
            "nl": "Deze tabel toont je berichten en commentaren in Facebook-groepen."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Post": {"en": "Post", "nl": "Bericht"},
            "Date": {"en": "Date", "nl": "Datum"},
            "URL": {"en": "URL", "nl": "URL"}
          }
        }
    """
    result = reader.json("group_posts_and_comments.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        l = d["group_posts_v2"]  # pyright: ignore
        for item in l:
            denested_dict = eh.dict_denester(item)

            datapoints.append((
                eh.fix_latin1_string(eh.find_item(denested_dict, "title")),
                eh.fix_latin1_string(eh.find_item(denested_dict, "post")),
                eh.epoch_to_iso(eh.find_item(denested_dict, "timestamp"), errors=errors),
                eh.find_item(denested_dict, "url"),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Post", "Date", "URL"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def your_answers_to_membership_questions_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract your answers to Facebook group membership questions.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Group name``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook group the participant answered membership questions for when requesting to join.",
          "source_file": "your_answers_to_membership_questions.json",
          "columns": {
            "Group name": "Name of the Facebook group."
          }
        }

    Table config::

        {
          "id": "facebook_your_answers_to_membership_questions",
          "title": {
            "en": "Your answers to group membership questions",
            "nl": "Je antwoorden op vragen voor groepslidmaatschap"
          },
          "description": {
            "en": "This table contains the answers you provided when requesting to join Facebook groups.",
            "nl": "Deze tabel bevat de antwoorden die je hebt gegeven bij het aanvragen van lidmaatschap van Facebook-groepen."
          },
          "headers": {
            "Group name": {"en": "Group name", "nl": "Groepsnaam"}
          }
        }
    """
    result = reader.json("your_answers_to_membership_questions.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:

        items = d["group_membership_questions_answers_v2"]["group_answers"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item.get("group_name", "")),
            ))
        out = pd.DataFrame(datapoints, columns=["Group name"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def your_comments_in_groups_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract your comments in Facebook groups.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Comment``, ``Group``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a comment the participant made in a Facebook group, including the title, comment text, group name, and timestamp.",
          "source_file": "your_comments_in_groups.json",
          "columns": {
            "Title": "Title of the post the comment was made on.",
            "Comment": "Text content of the comment.",
            "Group": "Name of the Facebook group.",
            "Timestamp": "ISO 8601 timestamp of when the comment was made."
          }
        }

    Table config::

        {
          "id": "facebook_your_comments_in_groups",
          "title": {
            "en": "Your comments in groups",
            "nl": "Je commentaren in groepen"
          },
          "description": {
            "en": "This table specifically lists the comments you have made in Facebook groups.",
            "nl": "Deze tabel toont specifiek de commentaren die je in Facebook-groepen hebt geplaatst."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Comment": {"en": "Comment", "nl": "Reactie"},
            "Group": {"en": "Group", "nl": "Groep"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json("your_comments_in_groups.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        l = d["group_comments_v2"]  # pyright: ignore
        for item in l:
            denested_dict = eh.dict_denester(item)

            datapoints.append((
                eh.fix_latin1_string(eh.find_item(denested_dict, "title")),
                eh.fix_latin1_string(eh.find_item(denested_dict, "comment-comment")),
                eh.fix_latin1_string(eh.find_item(denested_dict, "group")),
                eh.epoch_to_iso(eh.find_item(denested_dict, "timestamp"), errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Comment", "Group", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def your_group_membership_activity_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract Facebook group membership activity.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Group name``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook group the participant joined, including the title, group name, and the time of joining.",
          "source_file": "your_group_membership_activity.json",
          "columns": {
            "Title": "Title or description of the membership activity.",
            "Group name": "Name of the Facebook group.",
            "Timestamp": "ISO 8601 timestamp of when the participant joined."
          }
        }

    Table config::

        {
          "id": "facebook_your_group_membership_activity",
          "title": {
            "en": "Facebook groups you are a member of",
            "nl": "Facebookgroepen waar je lid van bent"
          },
          "description": {
            "en": "This table lists the Facebook groups you are currently a member of.",
            "nl": "Deze tabel toont de Facebookgroepen waar je momenteel lid van bent."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Group name": {"en": "Group name", "nl": "Groepsnaam"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json("your_group_membership_activity.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["groups_joined_v2"]  # pyright: ignore
        for item in items:
            denested_dict = eh.dict_denester(item)

            datapoints.append((
                eh.fix_latin1_string(eh.find_item(denested_dict, "title")),
                eh.fix_latin1_string(eh.find_item(denested_dict, "name")),
                eh.epoch_to_iso(eh.find_item(denested_dict, "timestamp"), errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Group name", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def pages_and_profiles_you_follow_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract pages and profiles you follow on Facebook.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook Page or profile the participant follows, including the title and time they started following.",
          "source_file": "pages_and_profiles_you_follow.json",
          "columns": {
            "Title": "Title of the followed Page or profile.",
            "Timestamp": "ISO 8601 timestamp of when the participant started following."
          }
        }

    Table config::

        {
          "id": "facebook_pages_and_profiles_you_follow",
          "title": {
            "en": "Pages and profiles that you follow",
            "nl": "Pagina's en profielen die je volgt"
          },
          "description": {
            "en": "This table displays the Facebook Pages and profiles that you actively follow.",
            "nl": "Deze tabel toont de Facebookpagina's en -profielen die je actief volgt."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json("pages_and_profiles_you_follow.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["pages_followed_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item.get("title", "")),
                eh.epoch_to_iso(item.get("timestamp", ""), errors=errors)
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def pages_youve_liked_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract Facebook pages you have liked.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Name``, ``URL``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook Page the participant has liked, including the page name, URL, and timestamp.",
          "source_file": "pages_you_ve_liked.json",
          "columns": {
            "Name": "Name of the liked Facebook Page.",
            "URL": "URL of the liked Facebook Page.",
            "Timestamp": "ISO 8601 timestamp of when the page was liked."
          }
        }

    Table config::

        {
          "id": "facebook_pages_youve_liked",
          "title": {
            "en": "Pages that you have liked",
            "nl": "Pagina's die je leuk vindt"
          },
          "description": {
            "en": "This table contains a history of the Facebook Pages you have liked.",
            "nl": "Deze tabel bevat een overzicht van de Facebookpagina's die je leuk vindt."
          },
          "headers": {
            "Name": {"en": "Name", "nl": "Naam"},
            "URL": {"en": "URL", "nl": "URL"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json("pages_you_ve_liked.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["page_likes_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item.get("name", "")),
                item.get("url", ""),
                eh.epoch_to_iso(item.get("timestamp", ""), errors=errors)
            ))

        out = pd.DataFrame(datapoints, columns=["Name", "URL", "Timestamp"]) # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def your_saved_items_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract your saved items on Facebook.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a post, video, or other item the participant saved on Facebook, including the title and timestamp.",
          "source_file": "your_saved_items.json",
          "columns": {
            "Title": "Title of the saved item.",
            "Timestamp": "ISO 8601 timestamp of when the item was saved."
          }
        }

    Table config::

        {
          "id": "facebook_your_saved_items",
          "title": {
            "en": "Your saved items",
            "nl": "Je opgeslagen items"
          },
          "description": {
            "en": "This table contains the posts, videos, and other content you have saved on Facebook.",
            "nl": "Deze tabel bevat de berichten, video's en andere content die je op Facebook hebt opgeslagen."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json("your_saved_items.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["saves_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item.get("title", "")),
                eh.epoch_to_iso(item.get("timestamp", ""), errors=errors)
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def comments_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract all comments you made on Facebook.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Comment``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a comment the participant made on a Facebook post or other content, including the title, comment text, and timestamp.",
          "source_file": "comments_and_reactions/comments.json",
          "columns": {
            "Title": "Title of the post the comment was made on.",
            "Comment": "Text content of the comment.",
            "Timestamp": "ISO 8601 timestamp of when the comment was made."
          }
        }

    Table config::

        {
          "id": "facebook_comments",
          "title": {
            "en": "Your comments",
            "nl": "Je commentaren"
          },
          "description": {
            "en": "This table shows all the comments you have made on Facebook posts and other content.",
            "nl": "Deze tabel toont alle commentaren die je op Facebook-berichten en andere content hebt geplaatst."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Comment": {"en": "Comment", "nl": "Reactie"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json("comments_and_reactions/comments.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["comments_v2"]  # pyright: ignore
        for item in items:
            denested_dict = eh.dict_denester(item)

            datapoints.append((
                eh.fix_latin1_string(eh.find_item(denested_dict, "title")),
                eh.fix_latin1_string(eh.find_item(denested_dict, "comment-comment")),
                eh.epoch_to_iso(eh.find_item(denested_dict, "timestamp"), errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Comment", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def likes_and_reactions_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract likes and reactions with titles from Facebook.

    Reads ``likes_and_reactions_x`` numbered files.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Reaction``, ``Timestamp``.
        Empty DataFrame when no matching files are found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a post the participant liked or reacted to on Facebook, including the post title, reaction type, and timestamp.",
          "source_file": "likes_and_reactions_1.json (and numbered variants)",
          "columns": {
            "Title": "Title of the post that was liked or reacted to.",
            "Reaction": "Type of reaction (e.g. Like, Love, Haha).",
            "Timestamp": "ISO 8601 timestamp of when the reaction was made."
          }
        }

    Table config::

        {
          "id": "facebook_likes_and_reactions",
          "title": {
            "en": "Posts you liked (with title)",
            "nl": "Posts die je leuk vond (met titel)"
          },
          "description": {
            "en": "This table shows the titles of posts you liked on Facebook.",
            "nl": "Deze tabel toont de titels van posts die je leuk vond op Facebook."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Reaction": {"en": "Reaction", "nl": "Reactie"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    out = pd.DataFrame()
    datapoints = []

    results = reader.json_all(r"(^|/)likes_and_reactions_\d+\.json$")
    if not results:
        return pd.DataFrame()

    try:
        for result in results:
            for item in result.data:
                denested_dict = eh.dict_denester(item)

                datapoints.append((
                    eh.fix_latin1_string(eh.find_item(denested_dict, "title")),
                    eh.fix_latin1_string(eh.find_item(denested_dict, "reaction-reaction")),
                    eh.epoch_to_iso(eh.find_item(denested_dict, "timestamp"), errors=errors),
                ))

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1
        return pd.DataFrame()

    out = pd.DataFrame(datapoints, columns=["Title", "Reaction", "Timestamp"]) #pyright: ignore

    return out


def your_comment_active_days_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract days you actively commented on Facebook.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Label``, ``Value``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a label-value pair indicating the days on which the participant actively commented on Facebook.",
          "source_file": "your_comment_active_days.json",
          "columns": {
            "Label": "Label describing the activity metric.",
            "Value": "Value associated with the label."
          }
        }

    Table config::

        {
          "id": "facebook_your_comment_active_days",
          "title": {
            "en": "Days you actively commented",
            "nl": "Dagen waarop je actief commentaren hebt geplaatst"
          },
          "description": {
            "en": "This table indicates the days on which you made comments on Facebook.",
            "nl": "Deze tabel toont de dagen waarop je commentaren op Facebook hebt geplaatst."
          },
          "headers": {
            "Label": {"en": "Label", "nl": "Label"},
            "Value": {"en": "Value", "nl": "Waarde"}
          }
        }
    """
    result = reader.json("your_comment_active_days.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["label_values"]  # pyright: ignore
        for item in items:
            datapoints.append((
                item.get("label", ""),
                item.get("value", ""),
            ))

        out = pd.DataFrame(datapoints, columns=["Label", "Value"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def your_pages_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract the Facebook pages you manage.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Name``, ``URL``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook Page the participant administers, including the page name, URL, and creation timestamp.",
          "source_file": "your_pages.json",
          "columns": {
            "Name": "Name of the Facebook Page.",
            "URL": "URL of the Facebook Page.",
            "Timestamp": "ISO 8601 timestamp of when the page was created."
          }
        }

    Table config::

        {
          "id": "facebook_your_pages",
          "title": {
            "en": "Pages you manage",
            "nl": "Pagina's die je beheert"
          },
          "description": {
            "en": "This table lists the Facebook Pages that you administer.",
            "nl": "Deze tabel toont de Facebookpagina's die je beheert."
          },
          "headers": {
            "Name": {"en": "Name", "nl": "Naam"},
            "URL": {"en": "URL", "nl": "URL"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json("your_pages.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["pages_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item.get("name", "")),
                item.get("url", ""),
                eh.epoch_to_iso(item.get("timestamp", ""), errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Name", "URL", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def story_reactions_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract your reactions to Facebook Stories.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a Facebook Story the participant reacted to, identified by its title.",
          "source_file": "story_reactions.json",
          "columns": {
            "Title": "Title of the story that was reacted to."
          }
        }

    Table config::

        {
          "id": "facebook_story_reactions",
          "title": {
            "en": "Your story reactions",
            "nl": "Je story-reacties"
          },
          "description": {
            "en": "This table contains your reactions to Facebook Stories.",
            "nl": "Deze tabel bevat je reacties op Facebook Stories."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"}
          }
        }
    """
    result = reader.json("story_reactions.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["stories_feedback_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                eh.fix_latin1_string(item.get("title", "")),
            ))

        out = pd.DataFrame(datapoints, columns=["Title"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def your_posts_check_ins_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract your posts and check-ins on Facebook.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a post or check-in the participant made on Facebook, including the title and timestamp.",
          "source_file": "your_posts__check_ins__photos_and_videos_1.json",
          "columns": {
            "Title": "Title of the post or check-in.",
            "Timestamp": "ISO 8601 timestamp of when the post or check-in was made."
          }
        }

    Table config::

        {
          "id": "facebook_your_posts_and_check_ins",
          "title": {
            "en": "Your posts and check-ins",
            "nl": "Je posts en check-ins"
          },
          "description": {
            "en": "This table shows the posts and places you have checked into on Facebook.",
            "nl": "Deze tabel toont de berichten en plaatsen waar je op Facebook hebt ingecheckt."
          },
          "headers": {
            "Title": {"en": "Title", "nl": "Titel"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    result = reader.json("your_posts__check_ins__photos_and_videos_1.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        for item in d:
            datapoints.append((
                eh.fix_latin1_string(item.get("title", "")),
                eh.epoch_to_iso(item.get("timestamp", ""), errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Timestamp"]) #pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def likes_and_reactions_base_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract likes and reactions from Facebook (base format).

    Reads ``likes_and_reactions.json`` (no number suffix) or, if absent, the
    numbered variants ``likes_and_reactions_1.json``, ``_2.json``, etc.
    Each item is structured with ``label_values`` containing Reaction, Name,
    and URL.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Reaction``, ``Name``, ``URL``, ``Timestamp``.
        Empty DataFrame when no matching files are found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a like or reaction the participant gave on Facebook, including the reaction type, name, URL, and timestamp.",
          "source_file": "likes_and_reactions.json or likes_and_reactions_1.json (and numbered variants)",
          "columns": {
            "Reaction": "Type of reaction (e.g. Like, Love, Haha).",
            "Name": "Name of the content that was reacted to.",
            "URL": "URL of the content that was reacted to.",
            "Timestamp": "ISO 8601 timestamp of when the reaction was made."
          }
        }

    Table config::

        {
          "id": "facebook_likes_and_reactions_base",
          "title": {
            "en": "Likes and reactions on Facebook",
            "nl": "Likes en reacties op Facebook"
          },
          "description": {
            "en": "This table shows your likes and reactions to posts and other content on Facebook.",
            "nl": "Deze tabel toont je likes en reacties op berichten en andere content op Facebook."
          },
          "headers": {
            "Reaction": {"en": "Reaction", "nl": "Reactie"},
            "Name": {"en": "Name", "nl": "Naam"},
            "URL": {"en": "URL", "nl": "URL"},
            "Timestamp": {"en": "Timestamp", "nl": "Datum en tijd"}
          }
        }
    """
    datapoints = []

    def _parse_items(d: list) -> None:
        for item in d:
            lv = {x.get("label", ""): x.get("value", "") for x in item.get("label_values", [])}
            datapoints.append((
                lv.get("Reaction", ""),
                eh.fix_latin1_string(lv.get("Name", "")),
                lv.get("URL", ""),
                eh.epoch_to_iso(item.get("timestamp", ""), errors=errors),
            ))

    try:
        result = reader.json("likes_and_reactions.json")
        if result.found:
            _parse_items(result.data)  # pyright: ignore
        else:
            # Fall back to numbered files for DDPs that only export _1, _2, ...
            results = reader.json_all(r"(^|/)likes_and_reactions_\d+\.json$")
            for r in results:
                _parse_items(r.data)  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    out = pd.DataFrame(datapoints, columns=["Reaction", "Name", "URL", "Timestamp"]) if datapoints else pd.DataFrame()  # pyright: ignore
    return out


def controls_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract feed controls (show more / show less) from Facebook.

    Reads ``preferences/feed/controls.json``.  The top-level key ``controls``
    is a list of groups (e.g. "Show more", "Show less"), each with an
    ``entries`` list.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Action``, ``Content``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents an action the participant took to customise their Facebook feed (show more or show less of certain content), including the action type, content affected, and date.",
          "source_file": "preferences/feed/controls.json",
          "columns": {
            "Action": "Feed control action taken (e.g. Show more, Show less).",
            "Content": "Content or topic the action was applied to.",
            "Date": "ISO 8601 timestamp of when the action was taken."
          }
        }

    Table config::

        {
          "id": "facebook_feed_controls",
          "title": {
            "en": "Feed controls (show more / show less)",
            "nl": "Feed-voorkeuren (meer zien / minder zien)"
          },
          "description": {
            "en": "This table shows the actions you've taken to customise what content you see more or less of on Facebook.",
            "nl": "Deze tabel toont de acties die je hebt ondernomen om aan te passen welke content je meer of minder ziet op Facebook."
          },
          "headers": {
            "Action": {"en": "Action", "nl": "Actie"},
            "Content": {"en": "Content", "nl": "Inhoud"},
            "Date": {"en": "Date", "nl": "Datum"}
          }
        }
    """
    result = reader.json("preferences/feed/controls.json")
    if not result.found:
        return pd.DataFrame()
    d = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        groups = d["controls"]  # pyright: ignore
        for group in groups:
            action = group.get("name", "")
            for entry in group.get("entries", []):
                denested = eh.dict_denester(entry)
                datapoints.append((
                    action,
                    eh.fix_latin1_string(eh.find_item(denested, "value")),
                    eh.epoch_to_iso(eh.find_item(denested, "timestamp"), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Action", "Content", "Date"])  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


# ---------------------------------------------------------------------------
# Extractor registry & platform info
# ---------------------------------------------------------------------------

#: Mapping from the string names used in port_config.json to actual extractor functions.
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "who_youve_followed_to_df": who_youve_followed_to_df,
    "news_your_locations_to_df": news_your_locations_to_df,
    "notifications_to_df": notifications_to_df,
    "content_sharing_you_have_created_to_df": content_sharing_you_have_created_to_df,
    "facebook_reels_usage_to_df": facebook_reels_usage_to_df,
    "last_28_days_to_df": last_28_days_to_df,
    "your_search_history_to_df": your_search_history_to_df,
    "your_friends_to_df": your_friends_to_df,
    "ads_interests_to_df": ads_interests_to_df,
    "recently_viewed_to_df": recently_viewed_to_df,
    "recently_visited_to_df": recently_visited_to_df,
    "profile_update_history_to_df": profile_update_history_to_df,
    "your_event_responses_to_df": your_event_responses_to_df,
    "group_posts_and_comments_to_df": group_posts_and_comments_to_df,
    "your_answers_to_membership_questions_to_df": your_answers_to_membership_questions_to_df,
    "your_comments_in_groups_to_df": your_comments_in_groups_to_df,
    "your_group_membership_activity_to_df": your_group_membership_activity_to_df,
    "pages_and_profiles_you_follow_to_df": pages_and_profiles_you_follow_to_df,
    "pages_youve_liked_to_df": pages_youve_liked_to_df,
    "your_saved_items_to_df": your_saved_items_to_df,
    "comments_to_df": comments_to_df,
    "likes_and_reactions_to_df": likes_and_reactions_to_df,
    "your_comment_active_days_to_df": your_comment_active_days_to_df,
    "your_pages_to_df": your_pages_to_df,
    "story_reactions_to_df": story_reactions_to_df,
    "your_posts_check_ins_to_df": your_posts_check_ins_to_df,
    "likes_and_reactions_base_to_df": likes_and_reactions_base_to_df,
    "controls_to_df": controls_to_df,
}


# ---------------------------------------------------------------------------
# Main extraction & flow
# ---------------------------------------------------------------------------

def extraction(facebook_zip: str, validation) -> ExtractionResult:
    """Extract data from a Facebook DDP zip and return consent-form tables.

    Parameters
    ----------
    facebook_zip:
        Path to the Facebook DDP zip archive on disk.
    validation:
        Validation result object whose ``archive_members`` attribute is passed
        to ``ZipArchiveReader``.
    """
    config = load_port_config(EXTRACTOR_REGISTRY)
    errors: Counter = Counter()
    reader = ZipArchiveReader(facebook_zip, validation.archive_members, errors)
    return run_extraction(reader, errors, config)


class FacebookFlow(FlowBuilder):
    """Flow implementation for the Facebook data donation study."""

    def __init__(self, session_id: str):
        super().__init__(session_id, "Facebook")

    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)

    def extract_data(self, file_value, validation):
        return extraction(file_value, validation)


def process(session_id):
    flow = FacebookFlow(session_id)
    return flow.start_flow()
