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
        "languages": ["en", "nl", "de", "pl", "tr", "ar", "ru", "it", "ro", "es", "sq"],
        "description": "Handles DDPs in English. These data donation flows have not been tested yet, if you find anything wrong with them report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "not yet implemented"
    }
"""

import logging
import re
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
"subscription_for_no_ads.json", "other_categories_used_to_reach_you.json", "ads_feedback_activity.json", "ads_personalization_consent.json", "advertisers_you've_interacted_with.json", "advertisers_using_your_activity_or_information.json", "story_views_in_past_7_days.json", "ad_preferences.json", "groups_you've_searched_for.json", "your_search_history.json", "primary_public_location.json", "timezone.json", "primary_location.json", "your_privacy_jurisdiction.json", "people_and_friends.json", "ads_interests.json", "notifications.json", "notification_of_meta_privacy_policy_update.json", "recently_viewed.json", "recently_visited.json", "your_avatar.json", "meta_avatars_post_backgrounds.json", "contacts_sync_settings.json", "timezone.json", "autofill_information.json", "profile_information.json", "profile_update_history.json", "your_transaction_survey_information.json", "your_recently_followed_history.json", "your_recently_used_emojis.json", "navigation_bar_activity.json", "pages_and_profiles_you_follow.json", "pages_you've_liked.json", "your_saved_items.json", "fundraiser_posts_you_likely_viewed.json", "your_fundraiser_donations_information.json", "your_event_responses.json", "event_invitations.json", "your_event_invitation_links.json", "likes_and_reactions_1.json", "your_uncategorized_photos.json", "payment_history.json", "your_answers_to_membership_questions.json", "your_group_membership_activity.json", "your_contributions.json", "group_posts_and_comments.json", "your_comments_in_groups.json", "instant_games.json", "your_page_or_groups_badges.json", "instant_games_usage_data.json", "who_you've_followed.json", "people_you_may_know.json", "received_friend_requests.json", "your_friends.json", "likes_and_reactions.json", "controls.json",
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
            "nl": "Wie je volgt",
            "de": "Wem Sie folgen",
            "pl": "Kogo obserwujesz",
            "tr": "Takip ettiklerin",
            "ar": "من تتابعهم",
            "ru": "На кого вы подписаны",
            "it": "Chi segui",
            "ro": "Pe cine urmărești",
            "es": "A quién sigues",
            "sq": "Kë ndjek"
          },
          "description": {
            "en": "This table shows the Facebook profiles and pages you currently follow.",
            "nl": "Deze tabel toont de Facebook-profielen en -pagina's die je momenteel volgt.",
            "de": "Diese Tabelle zeigt die Facebook-Profile und -Seiten, denen Sie aktuell folgen.",
            "pl": "Ta tabela pokazuje profile i strony na Facebooku, które aktualnie obserwujesz.",
            "tr": "Bu tablo şu anda Facebook'ta takip ettiğin profilleri ve sayfaları gösterir.",
            "ar": "يعرض هذا الجدول ملفات الأشخاص وصفحات فيسبوك التي تتابعها حاليًا.",
            "ru": "В этой таблице показаны профили и страницы Facebook, на которые вы сейчас подписаны.",
            "it": "Questa tabella mostra i profili e le pagine di Facebook che segui attualmente.",
            "ro": "Acest tabel arată profilurile și paginile de Facebook pe care le urmărești în prezent.",
            "es": "Esta tabla muestra los perfiles y páginas de Facebook que sigues actualmente.",
            "sq": "Kjo tabelë tregon profilet dhe faqet e Facebook-ut që ndjek aktualisht."
          },
          "headers": {
            "Name": {
              "en": "Name",
              "nl": "Naam",
              "de": "Name",
              "pl": "Nazwa",
              "tr": "Ad",
              "ar": "الاسم",
              "ru": "Название",
              "it": "Nome",
              "ro": "Nume",
              "es": "Nombre",
              "sq": "Emri"
            },
            "Timestamp": {
              "en": "Timestamp",
              "nl": "Datum en tijd",
              "de": "Zeitstempel",
              "pl": "Znacznik czasu",
              "tr": "Zaman Damgası",
              "ar": "الطابع الزمني",
              "ru": "Отметка времени",
              "it": "Timestamp",
              "ro": "Marcaj temporal",
              "es": "Marca de tiempo",
              "sq": "Vula kohore"
            }
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


def facebook_reels_usage_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract Facebook Reels usage information.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction. Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Reel interaction``, ``Number of Reels``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a type of interaction the participant had with Facebook Reels and the corresponding number of Reels.",
          "source_file": "facebook_reels_usage_information.json",
          "columns": {
            "Reel interaction": "Type of interaction with Facebook Reels.",
            "Number of Reels": "Number of Reels associated with the interaction."
          }
        }

    Table config::

        {
          "id": "facebook_reels_usage",
          "title": {
            "en": "Interactions with Facebook Reels",
            "nl": "Interacties met Facebook Reels",
            "de": "Interaktionen mit Facebook Reels",
            "pl": "Interakcje z Reels na Facebooku",
            "tr": "Facebook Reels ile etkileşimlerin",
            "ar": "تفاعلاتك مع Reels على فيسبوك",
            "ru": "Взаимодействия с Reels на Facebook",
            "it": "Interazioni con i Reels di Facebook",
            "ro": "Interacțiunile tale cu Reels pe Facebook",
            "es": "Interacciones con Reels de Facebook",
            "sq": "Ndërveprimet e tua me Reels në Facebook"
          },
          "description": {
            "en": "This table shows your interactions with Facebook Reels, such as videos you've watched or engaged with.",
            "nl": "Deze tabel toont je interacties met Facebook Reels, zoals video's die je hebt bekeken of waarmee je hebt gecommuniceerd.",
            "de": "Diese Tabelle zeigt Ihre Interaktionen mit Facebook Reels, zum Beispiel Videos, die Sie sich angesehen oder mit denen Sie interagiert haben.",
            "pl": "Ta tabela pokazuje Twoje interakcje z Facebook Reels, na przykład filmy, które obejrzałeś/aś lub z którymi wchodziłeś/aś w interakcję.",
            "tr": "Bu tablo, izlediğin veya etkileşimde bulunduğun videolar gibi Facebook Reels ile olan etkileşimlerini gösterir.",
            "ar": "يعرض هذا الجدول تفاعلاتك مع Reels على فيسبوك، مثل مقاطع الفيديو التي شاهدتها أو تفاعلت معها.",
            "ru": "В этой таблице показаны ваши взаимодействия с Reels на Facebook, например просмотренные видео или видео, с которыми вы взаимодействовали.",
            "it": "Questa tabella mostra le tue interazioni con i Reels di Facebook, ad esempio i video che hai guardato o con cui hai interagito.",
            "ro": "Acest tabel arată interacțiunile tale cu Reels pe Facebook, cum ar fi videoclipurile pe care le-ai vizionat sau cu care ai interacționat.",
            "es": "Esta tabla muestra tus interacciones con los Reels de Facebook, como los videos que has visto o con los que has interactuado.",
            "sq": "Kjo tabelë tregon ndërveprimet e tua me Reels në Facebook, si videot që ke parë ose me të cilat ke ndërvepruar."
          },
          "headers": {
            "Reel interaction": {
              "en": "Reel interaction",
              "nl": "Interactie met reels",
              "de": "Reel-Interaktion",
              "pl": "Interakcja z Reels",
              "tr": "Reels Etkileşimi",
              "ar": "التفاعل مع Reels",
              "ru": "Взаимодействие с Reels",
              "it": "Interazione con i Reels",
              "ro": "Interacțiune cu Reels",
              "es": "Interacción con Reels",
              "sq": "Ndërveprim me Reels"
            },
            "Number of Reels": {
              "en": "Number of Reels",
              "nl": "Aantal Reels",
              "de": "Anzahl der Reels",
              "pl": "Liczba Reels",
              "tr": "Reels Sayısı",
              "ar": "عدد Reels",
              "ru": "Количество Reels",
              "it": "Numero di Reels",
              "ro": "Număr de Reels",
              "es": "Número de Reels",
              "sq": "Numri i Reels"
            }
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
        items = d.get("label_values", [])  # pyright: ignore
        d = items[0]

        for item in d["dict"]:
            denested_dict = eh.dict_denester(item)

            label = eh.fix_latin1_string(
                eh.find_item(denested_dict, "label")
            )
            value = eh.find_item(denested_dict, "value")

            # Shorter, participant-friendly German labels
            reel_label_translations = {
                "The number of Reels you have seen in the last 7 days":
                    "In den letzten 7 Tagen angesehen",

                "The number of Reels you have seen in the last 30 days":
                    "In den letzten 30 Tagen angesehen",

                "The number of Reels you have liked in the last 30 days":
                    "In den letzten 30 Tagen mit „Gefällt mir“ markiert",

                "The number of Reels you have seen in the horizontal Reels tray in the last 7 days":
                    "In den letzten 7 Tagen im horizontalen Reels-Bereich angesehen",

                "The number of Reels you have clicked from the horizontal Reels tray in the last 7 days":
                    "In den letzten 7 Tagen im horizontalen Reels-Bereich angeklickt",
            }

            label = reel_label_translations.get(label, label)

            label_lower = label.lower()

            # Fallback for slightly different German wording in Meta exports
            if (
                "horizontalen reels-bereich" in label_lower
                and "gesehen" in label_lower
                and "7" in label_lower
            ):
                label = (
                    "In den letzten 7 Tagen im horizontalen "
                    "Reels-Bereich angesehen"
                )

            datapoints.append((
                label,
                value,
            ))

        out = pd.DataFrame(
            datapoints,
            columns=["Reel interaction", "Number of Reels"]
        )  # pyright: ignore

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
            "nl": "Hoeveel video's je de afgelopen 28 dagen hebt bekeken",
            "de": "Wie viele Videos Sie in den letzten 28 Tagen angesehen haben",
            "pl": "Ile filmów obejrzałeś/aś w ciągu ostatnich 28 dni",
            "tr": "Son 28 günde izlediğin video sayısı",
            "ar": "عدد الفيديوهات التي شاهدتها خلال آخر 28 يومًا",
            "ru": "Сколько видео вы посмотрели за последние 28 дней",
            "it": "Quanti video hai guardato negli ultimi 28 giorni",
            "ro": "Câte videoclipuri ai vizionat în ultimele 28 de zile",
            "es": "Cuántos videos viste en los últimos 28 días",
            "sq": "Sa video ke parë në 28 ditët e fundit"
          },
          "description": {
            "en": "This table indicates the number of videos you have watched on Facebook in the past 28 days.",
            "nl": "Deze tabel geeft het aantal video's aan dat je de afgelopen 28 dagen op Facebook hebt bekeken.",
            "de": "Diese Tabelle zeigt, wie viele Videos Sie in den letzten 28 Tagen auf Facebook angesehen haben.",
            "pl": "Ta tabela pokazuje liczbę filmów, które obejrzałeś/aś na Facebooku w ciągu ostatnich 28 dni.",
            "tr": "Bu tablo, son 28 günde Facebook'ta izlediğin video sayısını gösterir.",
            "ar": "يوضح هذا الجدول عدد مقاطع الفيديو التي شاهدتها على فيسبوك خلال آخر 28 يومًا.",
            "ru": "В этой таблице указано количество видео, которые вы посмотрели на Facebook за последние 28 дней.",
            "it": "Questa tabella indica il numero di video che hai guardato su Facebook negli ultimi 28 giorni.",
            "ro": "Acest tabel indică numărul de videoclipuri pe care le-ai vizionat pe Facebook în ultimele 28 de zile.",
            "es": "Esta tabla indica el número de videos que has visto en Facebook en los últimos 28 días.",
            "sq": "Kjo tabelë tregon numrin e videove që ke parë në Facebook gjatë 28 ditëve të fundit."
          },
          "headers": {
            "Count": {
              "en": "Count",
              "nl": "Aantal",
              "de": "Anzahl",
              "pl": "Liczba",
              "tr": "Sayı",
              "ar": "العدد",
              "ru": "Количество",
              "it": "Numero",
              "ro": "Număr",
              "es": "Número",
              "sq": "Numri"
            }
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
            "nl": "Je zoekgeschiedenis",
            "de": "Ihr Suchverlauf",
            "pl": "Historia wyszukiwania",
            "tr": "Arama geçmişin",
            "ar": "سجل بحثك",
            "ru": "История ваших поисковых запросов",
            "it": "La tua cronologia di ricerca",
            "ro": "Istoricul căutărilor tale",
            "es": "Tu historial de búsqueda",
            "sq": "Historiku i kërkimeve të tua"
          },
          "description": {
            "en": "This table contains a record of your search queries on Facebook.",
            "nl": "Deze tabel bevat een overzicht van je zoekopdrachten op Facebook.",
            "de": "Diese Tabelle enthält eine Übersicht Ihrer Suchanfragen auf Facebook.",
            "pl": "Ta tabela zawiera zapis Twoich zapytań wyszukiwania na Facebooku.",
            "tr": "Bu tablo, Facebook'ta yaptığın arama sorgularının bir kaydını içerir.",
            "ar": "يحتوي هذا الجدول على سجل لعمليات البحث التي أجريتها على فيسبوك.",
            "ru": "В этой таблице содержится история ваших поисковых запросов на Facebook.",
            "it": "Questa tabella contiene un registro delle tue ricerche su Facebook.",
            "ro": "Acest tabel conține o evidență a căutărilor pe care le-ai făcut pe Facebook.",
            "es": "Esta tabla contiene un registro de tus búsquedas en Facebook.",
            "sq": "Kjo tabelë përmban një regjistër të kërkimeve që ke bërë në Facebook."
          },
          "headers": {
            "Search term": {
              "en": "Search term",
              "nl": "Zoekterm",
              "de": "Suchbegriff",
              "pl": "Wyszukiwane hasło",
              "tr": "Arama Terimi",
              "ar": "مصطلح البحث",
              "ru": "Поисковый запрос",
              "it": "Termine di ricerca",
              "ro": "Termen de căutare",
              "es": "Término de búsqueda",
              "sq": "Termi i kërkimit"
            },
            "Date": {
              "en": "Date",
              "nl": "Datum",
              "de": "Datum",
              "pl": "Data",
              "tr": "Tarih",
              "ar": "التاريخ",
              "ru": "Дата",
              "it": "Data",
              "ro": "Data",
              "es": "Fecha",
              "sq": "Data"
            }
          },
          "visualizations": [
            {
              "title": {
                "en": "Terms you searched for",
                "nl": "Zoektermen waar je naar zocht",
                "de": "Begriffe, nach denen Sie gesucht haben",
                "pl": "Hasła, których szukałeś/aś",
                "tr": "Aradığın terimler",
                "ar": "الكلمات التي بحثت عنها",
                "ru": "Термины, которые вы искали",
                "it": "Termini che hai cercato",
                "ro": "Termeni pe care i-ai căutat",
                "es": "Términos que buscaste",
                "sq": "Termat që ke kërkuar"
              },
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
            "nl": "Je vrienden op Facebook",
            "de": "Ihre Freunde auf Facebook",
            "pl": "Twoi znajomi na Facebooku",
            "tr": "Facebook'taki arkadaşların",
            "ar": "أصدقاؤك على فيسبوك",
            "ru": "Ваши друзья на Facebook",
            "it": "I tuoi amici su Facebook",
            "ro": "Prietenii tăi de pe Facebook",
            "es": "Tus amigos en Facebook",
            "sq": "Miqtë e tu në Facebook"
          },
          "description": {
            "en": "This table lists your current friends on Facebook.",
            "nl": "Deze tabel toont je huidige vrienden op Facebook.",
            "de": "Diese Tabelle zeigt Ihre aktuellen Freunde auf Facebook.",
            "pl": "Ta tabela zawiera listę Twoich obecnych znajomych na Facebooku.",
            "tr": "Bu tablo, Facebook'taki mevcut arkadaşlarını listeler.",
            "ar": "يعرض هذا الجدول أصدقاءك الحاليين على فيسبوك.",
            "ru": "В этой таблице перечислены ваши текущие друзья на Facebook.",
            "it": "Questa tabella elenca i tuoi amici attuali su Facebook.",
            "ro": "Acest tabel listează prietenii tăi actuali de pe Facebook.",
            "es": "Esta tabla enumera tus amigos actuales en Facebook.",
            "sq": "Kjo tabelë liston miqtë e tu aktualë në Facebook."
          },
          "headers": {
            "Number of friends": {
              "en": "Number of friends",
              "nl": "Aantal vrienden op facebook",
              "de": "Anzahl der Freunde",
              "pl": "Liczba znajomych",
              "tr": "Arkadaş Sayısı",
              "ar": "عدد الأصدقاء",
              "ru": "Количество друзей",
              "it": "Numero di amici",
              "ro": "Numărul de prieteni",
              "es": "Número de amigos",
              "sq": "Numri i miqve"
            }
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
            "nl": "Je advertentie-interesses",
            "de": "Ihre Werbeinteressen",
            "pl": "Twoje zainteresowania reklamowe",
            "tr": "Reklam ilgi alanların",
            "ar": "اهتماماتك الإعلانية",
            "ru": "Ваши рекламные интересы",
            "it": "I tuoi interessi pubblicitari",
            "ro": "Interesele tale de publicitate",
            "es": "Tus intereses publicitarios",
            "sq": "Interesat e tua reklamuese"
          },
          "description": {
            "en": "This table shows the interests Facebook has identified for showing you personalized ads.",
            "nl": "Deze tabel toont de interesses die Facebook heeft geïdentificeerd om je gepersonaliseerde advertenties te tonen.",
            "de": "Diese Tabelle zeigt die Interessen, die Facebook für Sie ermittelt hat, um Ihnen personalisierte Werbung zu zeigen.",
            "pl": "Ta tabela pokazuje zainteresowania, które Facebook zidentyfikował, aby wyświetlać Ci spersonalizowane reklamy.",
            "tr": "Bu tablo, sana kişiselleştirilmiş reklamlar göstermek için Facebook'un belirlediği ilgi alanlarını gösterir.",
            "ar": "يعرض هذا الجدول الاهتمامات التي حددها فيسبوك لعرض إعلانات مخصصة لك.",
            "ru": "В этой таблице показаны интересы, которые Facebook определил для показа вам персонализированной рекламы.",
            "it": "Questa tabella mostra gli interessi che Facebook ha identificato per mostrarti annunci personalizzati.",
            "ro": "Acest tabel arată interesele pe care Facebook le-a identificat pentru a-ți afișa reclame personalizate.",
            "es": "Esta tabla muestra los intereses que Facebook ha identificado para mostrarte anuncios personalizados.",
            "sq": "Kjo tabelë tregon interesat që Facebook ka identifikuar për të të shfaqur reklama të personalizuara."
          },
          "headers": {
            "Ad": {
              "en": "Ad",
              "nl": "Advertentie",
              "de": "Interesse",
              "pl": "Zainteresowanie",
              "tr": "İlgi Alanı",
              "ar": "الاهتمام",
              "ru": "Интерес",
              "it": "Interesse",
              "ro": "Interes",
              "es": "Interés",
              "sq": "Interesi"
            }
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
            "nl": "Facebook items die je recentelijk hebt bekeken",
            "de": "Facebook-Elemente, die Sie kürzlich angesehen haben",
            "pl": "Elementy na Facebooku, które ostatnio wyświetliłeś/aś",
            "tr": "Yakın zamanda görüntülediğin Facebook öğeleri",
            "ar": "عناصر فيسبوك التي شاهدتها مؤخرًا",
            "ru": "Элементы Facebook, которые вы недавно просматривали",
            "it": "Elementi di Facebook visualizzati di recente",
            "ro": "Elemente Facebook pe care le-ai vizualizat recent",
            "es": "Elementos de Facebook que viste recientemente",
            "sq": "Elementet e Facebook-ut që ke parë kohët e fundit"
          },
          "description": {
            "en": "This table shows the Facebook posts, videos, and other items you have recently viewed.",
            "nl": "Deze tabel toont de Facebook-posts, video's en andere items die je recentelijk hebt bekeken.",
            "de": "Diese Tabelle zeigt die Facebook-Beiträge, Videos und anderen Elemente, die Sie kürzlich angesehen haben.",
            "pl": "Ta tabela pokazuje posty, filmy i inne elementy na Facebooku, które ostatnio wyświetliłeś/aś.",
            "tr": "Bu tablo, yakın zamanda görüntülediğin Facebook gönderilerini, videolarını ve diğer öğeleri gösterir.",
            "ar": "يعرض هذا الجدول منشورات فيسبوك ومقاطع الفيديو والعناصر الأخرى التي شاهدتها مؤخرًا.",
            "ru": "В этой таблице показаны публикации, видео и другие элементы Facebook, которые вы недавно просматривали.",
            "it": "Questa tabella mostra i post, i video e altri elementi di Facebook che hai visualizzato di recente.",
            "ro": "Acest tabel arată postările, videoclipurile și alte elemente de Facebook pe care le-ai vizualizat recent.",
            "es": "Esta tabla muestra las publicaciones, videos y otros elementos de Facebook que has visto recientemente.",
            "sq": "Kjo tabelë tregon postimet, videot dhe elementet e tjera të Facebook-ut që ke parë kohët e fundit."
          },
          "headers": {
            "Category": {
              "en": "Category",
              "nl": "Categorie",
              "de": "Kategorie",
              "pl": "Kategoria",
              "tr": "Kategori",
              "ar": "الفئة",
              "ru": "Категория",
              "it": "Categoria",
              "ro": "Categorie",
              "es": "Categoría",
              "sq": "Kategoria"
            },
            "Name": {
              "en": "Name",
              "nl": "Naam",
              "de": "Name",
              "pl": "Nazwa",
              "tr": "Ad",
              "ar": "الاسم",
              "ru": "Название",
              "it": "Nome",
              "ro": "Nume",
              "es": "Nombre",
              "sq": "Emri"
            },
            "Link": {
              "en": "Link",
              "nl": "Link",
              "de": "Link",
              "pl": "Link",
              "tr": "Bağlantı",
              "ar": "الرابط",
              "ru": "Ссылка",
              "it": "Link",
              "ro": "Link",
              "es": "Enlace",
              "sq": "Lidhja"
            },
            "Date": {
              "en": "Date",
              "nl": "Datum",
              "de": "Datum",
              "pl": "Data",
              "tr": "Tarih",
              "ar": "التاريخ",
              "ru": "Дата",
              "it": "Data",
              "ro": "Data",
              "es": "Fecha",
              "sq": "Data"
            }
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
            "nl": "Profielen die je recentelijk hebt bezocht",
            "de": "Profile, die Sie kürzlich besucht haben",
            "pl": "Profile, które ostatnio odwiedziłeś/aś",
            "tr": "Yakın zamanda ziyaret ettiğin profiller",
            "ar": "الملفات الشخصية التي زرتها مؤخرًا",
            "ru": "Профили, которые вы недавно посещали",
            "it": "Profili visitati di recente",
            "ro": "Profiluri pe care le-ai vizitat recent",
            "es": "Perfiles que visitaste recientemente",
            "sq": "Profilet që ke vizituar kohët e fundit"
          },
          "description": {
            "en": "This table lists the Facebook profiles you have visited most recently.",
            "nl": "Deze tabel toont de Facebook-profielen die je recentelijk hebt bezocht.",
            "de": "Diese Tabelle zeigt die Facebook-Profile, die Sie zuletzt besucht haben.",
            "pl": "Ta tabela zawiera listę profili na Facebooku, które ostatnio odwiedziłeś/aś.",
            "tr": "Bu tablo, en son ziyaret ettiğin Facebook profillerini listeler.",
            "ar": "يسرد هذا الجدول ملفات فيسبوك الشخصية التي زرتها مؤخرًا.",
            "ru": "В этой таблице перечислены профили Facebook, которые вы посещали в последнее время.",
            "it": "Questa tabella elenca i profili di Facebook che hai visitato più di recente.",
            "ro": "Acest tabel listează profilurile de Facebook pe care le-ai vizitat cel mai recent.",
            "es": "Esta tabla enumera los perfiles de Facebook que has visitado más recientemente.",
            "sq": "Kjo tabelë liston profilet e Facebook-ut që ke vizituar më së fundmi."
          },
          "headers": {
            "Category": {
              "en": "Category",
              "nl": "Categorie",
              "de": "Kategorie",
              "pl": "Kategoria",
              "tr": "Kategori",
              "ar": "الفئة",
              "ru": "Категория",
              "it": "Categoria",
              "ro": "Categorie",
              "es": "Categoría",
              "sq": "Kategoria"
            },
            "Name": {
              "en": "Name",
              "nl": "Naam",
              "de": "Name",
              "pl": "Nazwa",
              "tr": "Ad",
              "ar": "الاسم",
              "ru": "Название",
              "it": "Nome",
              "ro": "Nume",
              "es": "Nombre",
              "sq": "Emri"
            },
            "Link": {
              "en": "Link",
              "nl": "Link",
              "de": "Link",
              "pl": "Link",
              "tr": "Bağlantı",
              "ar": "الرابط",
              "ru": "Ссылка",
              "it": "Link",
              "ro": "Link",
              "es": "Enlace",
              "sq": "Lidhja"
            },
            "Date": {
              "en": "Date",
              "nl": "Datum",
              "de": "Datum",
              "pl": "Data",
              "tr": "Tarih",
              "ar": "التاريخ",
              "ru": "Дата",
              "it": "Data",
              "ro": "Data",
              "es": "Fecha",
              "sq": "Data"
            }
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
                        eh.fix_latin1_string(item.get("name", "")),
                        eh.fix_latin1_string(entry.get("data", {}).get("name", "")),
                        entry.get("data", {}).get("uri", ""),
                        eh.epoch_to_iso(entry.get("timestamp", ""), errors=errors)
                    ))

        out = pd.DataFrame(datapoints, columns=["Category", "Name", "Link", "Date"]) #pyright: ignore

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
            "nl": "Pagina's en profielen die je volgt",
            "de": "Seiten und Profile, denen Sie folgen",
            "pl": "Strony i profile, które obserwujesz",
            "tr": "Takip ettiğin sayfalar ve profiller",
            "ar": "الصفحات والملفات الشخصية التي تتابعها",
            "ru": "Страницы и профили, на которые вы подписаны",
            "it": "Pagine e profili che segui",
            "ro": "Pagini și profiluri pe care le urmărești",
            "es": "Páginas y perfiles que sigues",
            "sq": "Faqet dhe profilet që ndjek"
          },
          "description": {
            "en": "This table displays the Facebook Pages and profiles that you actively follow.",
            "nl": "Deze tabel toont de Facebookpagina's en -profielen die je actief volgt.",
            "de": "Diese Tabelle zeigt die Facebook-Seiten und -Profile, denen Sie aktiv folgen.",
            "pl": "Ta tabela pokazuje strony i profile na Facebooku, które aktywnie obserwujesz.",
            "tr": "Bu tablo, aktif olarak takip ettiğin Facebook Sayfalarını ve profillerini gösterir.",
            "ar": "يعرض هذا الجدول صفحات وملفات فيسبوك الشخصية التي تتابعها بنشاط.",
            "ru": "В этой таблице показаны страницы и профили Facebook, на которые вы активно подписаны.",
            "it": "Questa tabella mostra le Pagine e i profili di Facebook che segui attivamente.",
            "ro": "Acest tabel arată Paginile și profilurile de Facebook pe care le urmărești activ.",
            "es": "Esta tabla muestra las Páginas y perfiles de Facebook que sigues activamente.",
            "sq": "Kjo tabelë tregon Faqet dhe profilet e Facebook-ut që ndjek në mënyrë aktive."
          },
          "headers": {
            "Title": {
              "en": "Title",
              "nl": "Titel",
              "de": "Titel",
              "pl": "Tytuł",
              "tr": "Başlık",
              "ar": "العنوان",
              "ru": "Заголовок",
              "it": "Titolo",
              "ro": "Titlu",
              "es": "Título",
              "sq": "Titulli"
            },
            "Timestamp": {
              "en": "Timestamp",
              "nl": "Datum en tijd",
              "de": "Zeitstempel",
              "pl": "Znacznik czasu",
              "tr": "Zaman Damgası",
              "ar": "الطابع الزمني",
              "ru": "Отметка времени",
              "it": "Timestamp",
              "ro": "Marcaj temporal",
              "es": "Marca de tiempo",
              "sq": "Vula kohore"
            }
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
            "nl": "Pagina's die je leuk vindt",
            "de": "Seiten, die Ihnen gefallen",
            "pl": "Strony, które polubiłeś/aś",
            "tr": "Beğendiğin sayfalar",
            "ar": "الصفحات التي أعجبت بها",
            "ru": "Страницы, которые вам понравились",
            "it": "Pagine a cui hai messo mi piace",
            "ro": "Pagini pe care le-ai apreciat",
            "es": "Páginas que te han gustado",
            "sq": "Faqet që ke pëlqyer"
          },
          "description": {
            "en": "This table contains a history of the Facebook Pages you have liked.",
            "nl": "Deze tabel bevat een overzicht van de Facebookpagina's die je leuk vindt.",
            "de": "Diese Tabelle enthält eine Übersicht der Facebook-Seiten, die Ihnen gefallen.",
            "pl": "Ta tabela zawiera historię stron na Facebooku, które polubiłeś/aś.",
            "tr": "Bu tablo, beğendiğin Facebook Sayfalarının geçmişini içerir.",
            "ar": "يحتوي هذا الجدول على سجل صفحات فيسبوك التي أعجبت بها.",
            "ru": "В этой таблице содержится история страниц Facebook, которые вам понравились.",
            "it": "Questa tabella contiene la cronologia delle Pagine Facebook a cui hai messo mi piace.",
            "ro": "Acest tabel conține istoricul Paginilor de Facebook pe care le-ai apreciat.",
            "es": "Esta tabla contiene un historial de las Páginas de Facebook que te han gustado.",
            "sq": "Kjo tabelë përmban historikun e Faqeve të Facebook-ut që ke pëlqyer."
          },
          "headers": {
            "Name": {
              "en": "Name",
              "nl": "Naam",
              "de": "Name",
              "pl": "Nazwa",
              "tr": "Ad",
              "ar": "الاسم",
              "ru": "Название",
              "it": "Nome",
              "ro": "Nume",
              "es": "Nombre",
              "sq": "Emri"
            },
            "URL": {
              "en": "URL",
              "nl": "URL",
              "de": "URL",
              "pl": "URL",
              "tr": "URL",
              "ar": "الرابط (URL)",
              "ru": "URL-адрес",
              "it": "URL",
              "ro": "URL",
              "es": "URL",
              "sq": "URL"
            },
            "Timestamp": {
              "en": "Timestamp",
              "nl": "Datum en tijd",
              "de": "Zeitstempel",
              "pl": "Znacznik czasu",
              "tr": "Zaman Damgası",
              "ar": "الطابع الزمني",
              "ru": "Отметка времени",
              "it": "Timestamp",
              "ro": "Marcaj temporal",
              "es": "Marca de tiempo",
              "sq": "Vula kohore"
            }
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
            "nl": "Je opgeslagen items",
            "de": "Ihre gespeicherten Objekte",
            "pl": "Twoje zapisane elementy",
            "tr": "Kaydettiğin öğeler",
            "ar": "العناصر المحفوظة لديك",
            "ru": "Ваши сохранённые материалы",
            "it": "I tuoi elementi salvati",
            "ro": "Elementele tale salvate",
            "es": "Tus elementos guardados",
            "sq": "Elementet e tua të ruajtura"
          },
          "description": {
            "en": "This table contains the posts, videos, and other content you have saved on Facebook.",
            "nl": "Deze tabel bevat de berichten, video's en andere content die je op Facebook hebt opgeslagen.",
            "de": "Diese Tabelle enthält die Beiträge, Videos und anderen Inhalte, die Sie auf Facebook gespeichert haben.",
            "pl": "Ta tabela zawiera posty, filmy i inne treści, które zapisałeś/aś na Facebooku.",
            "tr": "Bu tablo, Facebook'ta kaydettiğin gönderileri, videoları ve diğer içerikleri içerir.",
            "ar": "يحتوي هذا الجدول على المنشورات ومقاطع الفيديو والمحتويات الأخرى التي حفظتها على فيسبوك.",
            "ru": "В этой таблице содержатся публикации, видео и другой контент, который вы сохранили на Facebook.",
            "it": "Questa tabella contiene i post, i video e altri contenuti che hai salvato su Facebook.",
            "ro": "Acest tabel conține postările, videoclipurile și alt conținut pe care le-ai salvat pe Facebook.",
            "es": "Esta tabla contiene las publicaciones, videos y otro contenido que has guardado en Facebook.",
            "sq": "Kjo tabelë përmban postimet, videot dhe përmbajtjet e tjera që ke ruajtur në Facebook."
          },
          "headers": {
            "Title": {
              "en": "Title",
              "nl": "Titel",
              "de": "Titel",
              "pl": "Tytuł",
              "tr": "Başlık",
              "ar": "العنوان",
              "ru": "Заголовок",
              "it": "Titolo",
              "ro": "Titlu",
              "es": "Título",
              "sq": "Titulli"
            },
            "Timestamp": {
              "en": "Timestamp",
              "nl": "Datum en tijd",
              "de": "Zeitstempel",
              "pl": "Znacznik czasu",
              "tr": "Zaman Damgası",
              "ar": "الطابع الزمني",
              "ru": "Отметка времени",
              "it": "Timestamp",
              "ro": "Marcaj temporal",
              "es": "Marca de tiempo",
              "sq": "Vula kohore"
            }
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


def _extract_commented_post_author(title: str) -> str:
    """Extract the content author from an English Facebook comment title."""
    title = title.strip()

    own_match = re.fullmatch(
        r"(.+?) commented on (?:his|her|their) own\s+.+",
        title,
        re.IGNORECASE,
    )
    if own_match:
        return own_match.group(1).strip()

    author_match = re.fullmatch(
        r".+? commented on (.+)['’]s?\s+.+",
        title,
        re.IGNORECASE,
    )
    if author_match:
        return author_match.group(1).strip()

    return ""


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
        Columns: ``Author``, ``Comment``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a comment the participant made on Facebook, including the author of the commented content, comment text, and timestamp.",
          "source_file": "comments_and_reactions/comments.json",
          "columns": {
            "Author": "Author of the post or other content the comment was made on, parsed from Facebook's English activity description.",
            "Comment": "Text content of the comment.",
            "Timestamp": "ISO 8601 timestamp of when the comment was made."
          }
        }

    Table config::

        {
          "id": "facebook_comments",
          "title": {
            "en": "Your comments",
            "nl": "Je commentaren",
            "de": "Ihre Kommentare",
            "pl": "Twoje komentarze",
            "tr": "Yorumların",
            "ar": "تعليقاتك",
            "ru": "Ваши комментарии",
            "it": "I tuoi commenti",
            "ro": "Comentariile tale",
            "es": "Tus comentarios",
            "sq": "Komentet e tua"
          },
          "description": {
            "en": "This table shows all the comments you have made on Facebook posts and other content.",
            "nl": "Deze tabel toont alle commentaren die je op Facebook-berichten en andere content hebt geplaatst.",
            "de": "Diese Tabelle zeigt alle Kommentare, die Sie zu Facebook-Beiträgen und anderen Inhalten verfasst haben.",
            "pl": "Ta tabela pokazuje wszystkie komentarze, które dodałeś/aś do postów i innych treści na Facebooku.",
            "tr": "Bu tablo, Facebook gönderilerine ve diğer içeriklere yaptığın tüm yorumları gösterir.",
            "ar": "يعرض هذا الجدول جميع التعليقات التي أضفتها على منشورات فيسبوك والمحتويات الأخرى.",
            "ru": "В этой таблице показаны все комментарии, которые вы оставили к публикациям Facebook и другому контенту.",
            "it": "Questa tabella mostra tutti i commenti che hai fatto a post di Facebook e altri contenuti.",
            "ro": "Acest tabel arată toate comentariile pe care le-ai făcut la postările de Facebook și la alt conținut.",
            "es": "Esta tabla muestra todos los comentarios que has hecho en publicaciones de Facebook y otro contenido.",
            "sq": "Kjo tabelë tregon të gjitha komentet që ke bërë në postimet e Facebook-ut dhe përmbajtje të tjera."
          },
          "headers": {
            "Author": {
              "en": "Author of the commented post",
              "nl": "Auteur van het bericht",
              "de": "Autor*in des kommentierten Beitrags",
              "pl": "Autor/ka komentowanego posta",
              "tr": "Yorum yapılan gönderinin yazarı",
              "ar": "مؤلف المنشور المعلّق عليه",
              "ru": "Автор прокомментированной публикации",
              "it": "Autore del post commentato",
              "ro": "Autorul postării comentate",
              "es": "Autor de la publicación comentada",
              "sq": "Autori i postimit të komentuar"
            },
            "Comment": {
              "en": "Comment",
              "nl": "Reactie",
              "de": "Kommentar",
              "pl": "Komentarz",
              "tr": "Yorum",
              "ar": "التعليق",
              "ru": "Комментарий",
              "it": "Commento",
              "ro": "Comentariu",
              "es": "Comentario",
              "sq": "Komenti"
            },
            "Timestamp": {
              "en": "Timestamp",
              "nl": "Datum en tijd",
              "de": "Zeitstempel",
              "pl": "Znacznik czasu",
              "tr": "Zaman Damgası",
              "ar": "الطابع الزمني",
              "ru": "Отметка времени",
              "it": "Timestamp",
              "ro": "Marcaj temporal",
              "es": "Marca de tiempo",
              "sq": "Vula kohore"
            }
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
                _extract_commented_post_author(
                    eh.fix_latin1_string(eh.find_item(denested_dict, "title"))
                ),
                eh.fix_latin1_string(eh.find_item(denested_dict, "comment-comment")),
                eh.epoch_to_iso(eh.find_item(denested_dict, "timestamp"), errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Author", "Comment", "Timestamp"]) #pyright: ignore

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
            "nl": "Posts die je leuk vond (met titel)",
            "de": "Beiträge, die Ihnen gefallen haben (mit Titel)",
            "pl": "Posty, które polubiłeś/aś (z tytułem)",
            "tr": "Beğendiğin gönderiler (başlıklı)",
            "ar": "المنشورات التي أعجبت بها (مع العنوان)",
            "ru": "Публикации, которые вам понравились (с названием)",
            "it": "Post a cui hai messo mi piace (con titolo)",
            "ro": "Postări care ți-au plăcut (cu titlu)",
            "es": "Publicaciones que te gustaron (con título)",
            "sq": "Postimet që ke pëlqyer (me titull)"
          },
          "description": {
            "en": "This table shows the titles of posts you liked on Facebook.",
            "nl": "Deze tabel toont de titels van posts die je leuk vond op Facebook.",
            "de": "Diese Tabelle zeigt die Titel der Beiträge, die Ihnen auf Facebook gefallen haben.",
            "pl": "Ta tabela pokazuje tytuły postów, które polubiłeś/aś na Facebooku.",
            "tr": "Bu tablo, Facebook'ta beğendiğin gönderilerin başlıklarını gösterir.",
            "ar": "يعرض هذا الجدول عناوين المنشورات التي أعجبت بها على فيسبوك.",
            "ru": "В этой таблице показаны названия публикаций, которые вам понравились на Facebook.",
            "it": "Questa tabella mostra i titoli dei post a cui hai messo mi piace su Facebook.",
            "ro": "Acest tabel arată titlurile postărilor care ți-au plăcut pe Facebook.",
            "es": "Esta tabla muestra los títulos de las publicaciones que te gustaron en Facebook.",
            "sq": "Kjo tabelë tregon titujt e postimeve që ke pëlqyer në Facebook."
          },
          "headers": {
            "Title": {
              "en": "Title",
              "nl": "Titel",
              "de": "Titel",
              "pl": "Tytuł",
              "tr": "Başlık",
              "ar": "العنوان",
              "ru": "Заголовок",
              "it": "Titolo",
              "ro": "Titlu",
              "es": "Título",
              "sq": "Titulli"
            },
            "Reaction": {
              "en": "Reaction",
              "nl": "Reactie",
              "de": "Reaktion",
              "pl": "Reakcja",
              "tr": "Tepki",
              "ar": "التفاعل",
              "ru": "Реакция",
              "it": "Reazione",
              "ro": "Reacție",
              "es": "Reacción",
              "sq": "Reagimi"
            },
            "Timestamp": {
              "en": "Timestamp",
              "nl": "Datum en tijd",
              "de": "Zeitstempel",
              "pl": "Znacznik czasu",
              "tr": "Zaman Damgası",
              "ar": "الطابع الزمني",
              "ru": "Отметка времени",
              "it": "Timestamp",
              "ro": "Marcaj temporal",
              "es": "Marca de tiempo",
              "sq": "Vula kohore"
            }
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
            "nl": "Dagen waarop je actief commentaren hebt geplaatst",
            "de": "Tage, an denen Sie aktiv kommentiert haben",
            "pl": "Dni, w których aktywnie komentowałeś/aś",
            "tr": "Aktif olarak yorum yaptığın günler",
            "ar": "الأيام التي علّقت فيها بنشاط",
            "ru": "Дни, когда вы активно комментировали",
            "it": "Giorni in cui hai commentato attivamente",
            "ro": "Zilele în care ai comentat activ",
            "es": "Días en los que comentaste activamente",
            "sq": "Ditët kur ke komentuar në mënyrë aktive"
          },
          "description": {
            "en": "This table indicates the days on which you made comments on Facebook.",
            "nl": "Deze tabel toont de dagen waarop je commentaren op Facebook hebt geplaatst.",
            "de": "Diese Tabelle zeigt die Tage, an denen Sie Kommentare auf Facebook verfasst haben.",
            "pl": "Ta tabela pokazuje dni, w których dodawałeś/aś komentarze na Facebooku.",
            "tr": "Bu tablo, Facebook'ta yorum yaptığın günleri gösterir.",
            "ar": "يوضح هذا الجدول الأيام التي أضفت فيها تعليقات على فيسبوك.",
            "ru": "В этой таблице показаны дни, в которые вы оставляли комментарии на Facebook.",
            "it": "Questa tabella mostra i giorni in cui hai pubblicato commenti su Facebook.",
            "ro": "Acest tabel arată zilele în care ai făcut comentarii pe Facebook.",
            "es": "Esta tabla muestra los días en los que hiciste comentarios en Facebook.",
            "sq": "Kjo tabelë tregon ditët kur ke bërë komente në Facebook."
          },
          "headers": {
            "Label": {
              "en": "Label",
              "nl": "Label",
              "de": "Bezeichnung",
              "pl": "Etykieta",
              "tr": "Etiket",
              "ar": "التصنيف",
              "ru": "Метка",
              "it": "Etichetta",
              "ro": "Etichetă",
              "es": "Etiqueta",
              "sq": "Etiketa"
            },
            "Value": {
              "en": "Value",
              "nl": "Waarde",
              "de": "Wert",
              "pl": "Wartość",
              "tr": "Değer",
              "ar": "القيمة",
              "ru": "Значение",
              "it": "Valore",
              "ro": "Valoare",
              "es": "Valor",
              "sq": "Vlera"
            }
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
                eh.fix_latin1_string(item.get("label", "")),
                item.get("value", ""),
            ))

        out = pd.DataFrame(datapoints, columns=["Label", "Value"]) #pyright: ignore

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
            "nl": "Je story-reacties",
            "de": "Ihre Story-Reaktionen",
            "pl": "Twoje reakcje na relacje",
            "tr": "Hikaye tepkilerin",
            "ar": "تفاعلاتك مع القصص",
            "ru": "Ваши реакции на истории",
            "it": "Le tue reazioni alle storie",
            "ro": "Reacțiile tale la povești",
            "es": "Tus reacciones a historias",
            "sq": "Reagimet e tua ndaj stories"
          },
          "description": {
            "en": "This table contains your reactions to Facebook Stories.",
            "nl": "Deze tabel bevat je reacties op Facebook Stories.",
            "de": "Diese Tabelle enthält Ihre Reaktionen auf Facebook Storys.",
            "pl": "Ta tabela zawiera Twoje reakcje na Relacje na Facebooku.",
            "tr": "Bu tablo, Facebook Hikayelerine verdiğin tepkileri içerir.",
            "ar": "يحتوي هذا الجدول على تفاعلاتك مع قصص فيسبوك.",
            "ru": "В этой таблице содержатся ваши реакции на истории Facebook.",
            "it": "Questa tabella contiene le tue reazioni alle Storie di Facebook.",
            "ro": "Acest tabel conține reacțiile tale la Poveștile de Facebook.",
            "es": "Esta tabla contiene tus reacciones a las Historias de Facebook.",
            "sq": "Kjo tabelë përmban reagimet e tua ndaj Stories në Facebook."
          },
          "headers": {
            "Title": {
              "en": "Title",
              "nl": "Titel",
              "de": "Titel",
              "pl": "Tytuł",
              "tr": "Başlık",
              "ar": "العنوان",
              "ru": "Заголовок",
              "it": "Titolo",
              "ro": "Titlu",
              "es": "Título",
              "sq": "Titulli"
            }
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


# ``label_values`` entries store their ``label`` text in whichever language
# the participant's Facebook account UI is set to (Facebook's export format,
# not a fixed English schema key). The candidate lists below are used to
# match a field regardless of account language. English is the verified
# baseline; the remaining entries are best-effort guesses sourced from
# Facebook's own terminology (see glossary.md) and are UNVERIFIED against
# real non-English exports -- see CHANGES_FACEBOOK.md, "Known limitations".
_REACTION_LABEL_CANDIDATES = ["Reaction", "Reactie", "Reaktion", "Reakcja", "Tepki", "تفاعل", "Реакция", "Reazione", "Reacție", "Reacción", "Reagimi"]
_NAME_LABEL_CANDIDATES = ["Name", "Naam", "Nazwa", "Ad", "الاسم", "Название", "Nome", "Nume", "Nombre", "Emri"]
_URL_LABEL_CANDIDATES = ["URL", "الرابط", "URL-адрес"]


def _lv_get(lv: dict, candidates: list) -> str:
    """Return the first non-empty value found in *lv* for any of *candidates*.

    ``lv`` is built from Facebook's ``label_values`` structure (a list of
    ``{"label": ..., "value": ...}`` dicts turned into a plain dict). The
    ``label`` text is displayed in whatever language the participant's
    Facebook account UI uses, so a single hardcoded English key (as the
    original implementation used) silently returns "" for any non-English
    export. This checks each language-specific candidate in turn.
    """
    for key in candidates:
        value = lv.get(key)
        if value:
            return value
    return ""


def likes_and_reactions_base_to_df(
    reader: ZipArchiveReader,
    errors: Counter
) -> pd.DataFrame:
    """Extract likes and reactions from Facebook.

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
        extraction. Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``Account``, ``Reaction``, ``URL``, ``Timestamp``.
        Empty DataFrame when no matching files are found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents a like or reaction the participant gave on Facebook, including the account or author of the content, reaction type, URL, and timestamp.",
          "source_file": "likes_and_reactions.json or likes_and_reactions_1.json (and numbered variants)",
          "columns": {
            "Account": "Name of the account or author whose content the participant reacted to.",
            "Reaction": "Type of reaction (e.g. Like, Love, Haha).",
            "URL": "URL of the content that was reacted to.",
            "Timestamp": "ISO 8601 timestamp of when the reaction was made."
          }
        }

    Table config::

        {
          "id": "facebook_likes_and_reactions_base",
          "title": {
            "en": "Likes and reactions on Facebook",
            "nl": "Likes en reacties op Facebook",
            "de": "Likes und Reaktionen auf Facebook",
            "pl": "Polubienia i reakcje na Facebooku",
            "tr": "Facebook'taki beğeniler ve tepkiler",
            "ar": "الإعجابات والتفاعلات على فيسبوك",
            "ru": "Отметки «Нравится» и реакции на Facebook",
            "it": "Mi piace e reazioni su Facebook",
            "ro": "Aprecieri și reacții pe Facebook",
            "es": "Me gusta y reacciones en Facebook",
            "sq": "Pëlqime dhe reagime në Facebook"
          },
          "description": {
            "en": "This table shows your likes and reactions to posts and other content on Facebook.",
            "nl": "Deze tabel toont je likes en reacties op berichten en andere content op Facebook.",
            "de": "Diese Tabelle zeigt Ihre Likes und Reaktionen auf Beiträge und andere Inhalte auf Facebook.",
            "pl": "Ta tabela pokazuje Twoje polubienia i reakcje na posty i inne treści na Facebooku.",
            "tr": "Bu tablo, Facebook'taki gönderilere ve diğer içeriklere verdiğin beğenileri ve tepkileri gösterir.",
            "ar": "يعرض هذا الجدول إعجاباتك وتفاعلاتك مع المنشورات والمحتويات الأخرى على فيسبوك.",
            "ru": "В этой таблице показаны ваши отметки «Нравится» и реакции на публикации и другой контент на Facebook.",
            "it": "Questa tabella mostra i tuoi mi piace e le tue reazioni a post e altri contenuti su Facebook.",
            "ro": "Acest tabel arată aprecierile și reacțiile tale la postări și alt conținut de pe Facebook.",
            "es": "Esta tabla muestra tus me gusta y reacciones a publicaciones y otro contenido en Facebook.",
            "sq": "Kjo tabelë tregon pëlqimet dhe reagimet e tua ndaj postimeve dhe përmbajtjeve të tjera në Facebook."
          },
          "headers": {
            "Account": {
              "en": "Account / author",
              "nl": "Account / auteur",
              "de": "Autor*in",
              "pl": "Konto / autor",
              "tr": "Hesap / yazar",
              "ar": "الحساب / الكاتب",
              "ru": "Аккаунт / автор",
              "it": "Account / autore",
              "ro": "Cont / autor",
              "es": "Cuenta / autor",
              "sq": "Llogaria / autori"
            },
            "Reaction": {
              "en": "Reaction",
              "nl": "Reactie",
              "de": "Reaktion",
              "pl": "Reakcja",
              "tr": "Tepki",
              "ar": "التفاعل",
              "ru": "Реакция",
              "it": "Reazione",
              "ro": "Reacție",
              "es": "Reacción",
              "sq": "Reagimi"
            },
            "URL": {
              "en": "URL",
              "nl": "URL",
              "de": "URL",
              "pl": "URL",
              "tr": "URL",
              "ar": "الرابط (URL)",
              "ru": "URL-адрес",
              "it": "URL",
              "ro": "URL",
              "es": "URL",
              "sq": "URL"
            },
            "Timestamp": {
              "en": "Date and time",
              "nl": "Datum en tijd",
              "de": "Zeitstempel",
              "pl": "Data i godzina",
              "tr": "Tarih ve saat",
              "ar": "التاريخ والوقت",
              "ru": "Дата и время",
              "it": "Data e ora",
              "ro": "Data și ora",
              "es": "Fecha y hora",
              "sq": "Data dhe ora"
            }
          }
        }
    """

    datapoints = []

    def _parse_items(d: list) -> None:
        for item in d:
            lv = {
                x.get("label", ""): x.get("value", "")
                for x in item.get("label_values", [])
            }

            datapoints.append((
                eh.fix_latin1_string(
                    _lv_get(lv, _NAME_LABEL_CANDIDATES)
                ),
                eh.fix_latin1_string(
                    _lv_get(lv, _REACTION_LABEL_CANDIDATES)
                ),
                _lv_get(lv, _URL_LABEL_CANDIDATES),
                eh.epoch_to_iso(
                    item.get("timestamp", ""),
                    errors=errors,
                ),
            ))

    try:
        result = reader.json("likes_and_reactions.json")

        if result.found:
            _parse_items(result.data)  # pyright: ignore
        else:
            # Fall back to numbered files for DDPs that only export _1, _2, ...
            results = reader.json_all(
                r"(^|/)likes_and_reactions_\d+\.json$"
            )

            for r in results:
                _parse_items(r.data)  # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    out = (
        pd.DataFrame(
            datapoints,
            columns=["Account", "Reaction", "URL", "Timestamp"],
        )
        if datapoints
        else pd.DataFrame()
    )

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
            "nl": "Feed-voorkeuren (meer zien / minder zien)",
            "de": "Feed-Einstellungen (mehr anzeigen / weniger anzeigen)",
            "pl": "Ustawienia aktualności (pokaż więcej / pokaż mniej)",
            "tr": "Akış kontrolleri (daha fazla göster / daha az göster)",
            "ar": "إعدادات آخر الأخبار (عرض المزيد / عرض أقل)",
            "ru": "Настройки ленты (показывать больше / показывать меньше)",
            "it": "Impostazioni del feed (mostra di più / mostra di meno)",
            "ro": "Setările fluxului (arată mai mult / arată mai puțin)",
            "es": "Controles de la sección de noticias (mostrar más / mostrar menos)",
            "sq": "Kontrollet e feed-it (shfaq më shumë / shfaq më pak)"
          },
          "description": {
            "en": "This table shows the actions you've taken to customise what content you see more or less of on Facebook.",
            "nl": "Deze tabel toont de acties die je hebt ondernomen om aan te passen welke content je meer of minder ziet op Facebook.",
            "de": "Diese Tabelle zeigt die Aktionen, mit denen Sie festgelegt haben, welche Inhalte Sie auf Facebook mehr oder weniger sehen.",
            "pl": "Ta tabela pokazuje działania, które podjąłeś/aś, aby dostosować, jakie treści widzisz częściej lub rzadziej na Facebooku.",
            "tr": "Bu tablo, Facebook'ta hangi içerikleri daha fazla veya daha az gördüğünü özelleştirmek için yaptığın işlemleri gösterir.",
            "ar": "يعرض هذا الجدول الإجراءات التي اتخذتها لتخصيص المحتوى الذي تراه أكثر أو أقل على فيسبوك.",
            "ru": "В этой таблице показаны действия, которые вы предприняли, чтобы настроить, какого контента вы видите больше или меньше на Facebook.",
            "it": "Questa tabella mostra le azioni che hai intrapreso per personalizzare quali contenuti vedi di più o di meno su Facebook.",
            "ro": "Acest tabel arată acțiunile pe care le-ai întreprins pentru a personaliza ce conținut vezi mai mult sau mai puțin pe Facebook.",
            "es": "Esta tabla muestra las acciones que has tomado para personalizar qué contenido ves más o menos en Facebook.",
            "sq": "Kjo tabelë tregon veprimet që ke ndërmarrë për të personalizuar përmbajtjen që sheh më shumë ose më pak në Facebook."
          },
          "headers": {
            "Action": {
              "en": "Action",
              "nl": "Actie",
              "de": "Aktion",
              "pl": "Działanie",
              "tr": "Eylem",
              "ar": "الإجراء",
              "ru": "Действие",
              "it": "Azione",
              "ro": "Acțiune",
              "es": "Acción",
              "sq": "Veprimi"
            },
            "Content": {
              "en": "Content",
              "nl": "Inhoud",
              "de": "Inhalt",
              "pl": "Treść",
              "tr": "İçerik",
              "ar": "المحتوى",
              "ru": "Содержание",
              "it": "Contenuto",
              "ro": "Conținut",
              "es": "Contenido",
              "sq": "Përmbajtja"
            },
            "Date": {
              "en": "Date",
              "nl": "Datum",
              "de": "Datum",
              "pl": "Data",
              "tr": "Tarih",
              "ar": "التاريخ",
              "ru": "Дата",
              "it": "Data",
              "ro": "Data",
              "es": "Fecha",
              "sq": "Data"
            }
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
            action = eh.fix_latin1_string(group.get("name", ""))
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
    "facebook_reels_usage_to_df": facebook_reels_usage_to_df,
    "last_28_days_to_df": last_28_days_to_df,
    "your_search_history_to_df": your_search_history_to_df,
    "your_friends_to_df": your_friends_to_df,
    "ads_interests_to_df": ads_interests_to_df,
    "recently_viewed_to_df": recently_viewed_to_df,
    "recently_visited_to_df": recently_visited_to_df,
    "pages_and_profiles_you_follow_to_df": pages_and_profiles_you_follow_to_df,
    "pages_youve_liked_to_df": pages_youve_liked_to_df,
    "your_saved_items_to_df": your_saved_items_to_df,
    "comments_to_df": comments_to_df,
    "your_comment_active_days_to_df": your_comment_active_days_to_df,
    "story_reactions_to_df": story_reactions_to_df,
    "likes_and_reactions_base_to_df": likes_and_reactions_base_to_df,
    "controls_to_df": controls_to_df,
}


# ---------------------------------------------------------------------------
# Main extraction & flow
# ---------------------------------------------------------------------------

def extraction(facebook_zip: SeekableBinaryReader, validation) -> ExtractionResult:
    """Extract data from a Facebook DDP zip and return consent-form tables.

    Parameters
    ----------
    facebook_zip:
        Seekable binary reader over the Facebook DDP zip — the upload
        adapter itself, never a path (ADR-0026).
    validation:
        Validation result object whose ``archive_members`` attribute is passed
        to ``ZipArchiveReader``.
    """
    config = load_port_config(EXTRACTOR_REGISTRY, "facebook")
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
