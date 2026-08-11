"""
Instagram

This module contains an example flow of a Instagram data donation study

Assumptions:
It handles DDPs in the english language with filetype JSON.

Configuration
-------------
The ``extraction`` function is driven by ``port_config.json``.  Generate one with::

    pnpm generate-config instagram

Each extractor function carries its own table config in a ``Table config::``
JSON block inside its docstring.  The generator reads those blocks and
assembles the JSON file.

Platform info::

    {
        "name": "Instagram",
        "filetypes": ["json"],
        "languages": ["en", "nl", "de", "pl", "tr", "ar", "ru", "it", "ro", "es", "sq"],
        "description": "Note that supported DDP language also includes Dutch and probably other languages as well. You get an english DDP regardless of the Dutch language setting. These data donation flows have not been tested yet, if you find anything wrong with them report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "not yet implemented"
    }
"""

import logging
from collections import Counter
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
            "ads_viewed.json",
            "ads_interests.json",
            "account_searches.json",
            "profile_searches.json",
            "followers_1.json",
            "saved_posts.json",
            "following.json",
            "posts_viewed.json",
            "post_comments_1.json",
            "recently_unfollowed_accounts.json",
            "post_comments.json",
            "account_information.json",
            "accounts_you're_not_interested_in.json",
            "liked_comments.json",
            "story_likes.json",
            "threads_viewed.json",
            "use_cross-app_messaging.json",
            "profile_changes.json",
            "reels.json",
        ],
    )
]



# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _sort_by_date(out: pd.DataFrame, date_column: str) -> pd.DataFrame:
    """Sort *out* by *date_column* using ISO-timestamp ordering.

    Parameters
    ----------
    out:
        DataFrame to sort.
    date_column:
        Name of the column that contains ISO-formatted timestamp strings.
        Rows with empty timestamps are placed last.
    """
    return out.sort_values(by=date_column, key=eh.sort_isotimestamp_empty_timestamp_last)


# ---------------------------------------------------------------------------
# Language-aware label candidates
# ---------------------------------------------------------------------------
# Field labels are localized to the account's UI language, not fixed
# English keys -- match against known language variants.
_URL_LABELS = ["URL", "الرابط", "URL-адрес"]
_NAME_LABELS = ["Naam", "Name", "Nazwa", "Ad", "الاسم", "Имя", "Nome", "Nume", "Nombre", "Emri"]
_USERNAME_LABELS = ["Gebruikersnaam", "Username", "Author", "Nutzername", "Nazwa użytkownika", "Kullanıcı adı", "اسم المستخدم", "Имя пользователя", "Nome utente", "Nume de utilizator", "Nombre de usuario", "Emri i përdoruesit"]
_AUTHOR_LABELS = ["Author", "Auteur", "Autor", "Yazar", "الكاتب", "Автор", "Autore"]
_TIME_LABELS = ["Time", "Tijd", "Zeit", "Godzina", "Saat", "الوقت", "Время", "Ora", "Hora"]
_COMMENT_LABELS = ["Comment", "Opmerking", "Kommentar", "Komentarz", "Yorum", "تعليق", "Комментарий", "Commento", "Comentariu", "Comentario", "Koment"]
_MEDIA_OWNER_LABELS = ["Media Owner", "Media-eigenaar", "Medieninhaber", "Właściciel mediów", "Medya sahibi", "مالك الوسائط", "Владелец медиафайла", "Proprietario del contenuto multimediale", "Proprietarul conținutului media", "Propietario del contenido multimedia", "Pronari i medias"]
_SAVED_ON_LABELS = ["Saved on", "Opgeslagen op", "Gespeichert am", "Zapisano", "Kaydedilme tarihi", "تم الحفظ في", "Сохранено", "Salvato il", "Salvat la", "Guardado el", "Ruajtur më"]


def _first_present(data: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    """Return the first dict value found for the given keys, or empty dict.

    Parameters
    ----------
    data:
        Dictionary to search.
    keys:
        Ordered list of keys to try; the value of the first key whose
        corresponding value is a ``dict`` is returned.
    """
    for key in keys:
        value = data.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _extract_owner_details(label_values: list[dict[str, Any]]) -> tuple[str, str, str]:
    """Extract ``(owner_name, owner_username, url)`` from a nested label_values structure.

    This structure is used in newer Instagram export formats.

    Parameters
    ----------
    label_values:
        Nested list/dict structure from the Instagram DDP containing labelled
        metadata fields such as ``"Name"``, ``"Username"``, and ``"URL"``.

    Returns
    -------
    tuple[str, str, str]
        A three-tuple of ``(owner_name, owner_username, url)``.  Any field
        not found in *label_values* is returned as an empty string.
    """
    owner_name = ""
    owner_username = ""
    url = ""

    def visit(node: Any) -> None:
        nonlocal owner_name, owner_username, url

        if isinstance(node, list):
            for item in node:
                visit(item)
            return

        if not isinstance(node, dict):
            return

        label = str(node.get("label", ""))
        value = str(node.get("value", ""))
        href = str(node.get("href", ""))

        if label in _URL_LABELS and not url:
            url = href or value
        elif label in _NAME_LABELS and not owner_name:
            owner_name = eh.fix_latin1_string(value)
        elif label in _USERNAME_LABELS and not owner_username:
            owner_username = eh.fix_latin1_string(value)

        for child in node.values():
            visit(child)

    visit(label_values)
    return owner_name, owner_username, url


# ---------------------------------------------------------------------------
# Per-table extraction functions
# ---------------------------------------------------------------------------

def followers_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "followers_1.json",
) -> pd.DataFrame:
    """Extract the list of followers into a DataFrame.

    Handles both the newer bare top-level list format and the older format
    where entries are wrapped under a ``"relationships_followers"`` key.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"followers_1.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Account``, ``URL``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one account that follows the participant on Instagram, including when they started following.",
          "source_file": "followers_1.json",
          "columns": {
            "Account": "Username or display name of the follower account.",
            "URL": "Direct URL to the follower's Instagram profile.",
            "Date": "ISO 8601 timestamp of when the account started following the participant."
          }
        }

    Table config::

                {
          "id": "instagram_followers",
          "title": {
            "en": "Your Instagram followers",
            "nl": "Je Instagram-volgers",
            "de": "Ihre Instagram-Follower",
            "pl": "Twoi obserwujący na Instagramie",
            "tr": "Instagram takipçilerin",
            "ar": "متابعوك على إنستغرام",
            "ru": "Ваши подписчики в Instagram",
            "it": "I tuoi follower su Instagram",
            "ro": "Urmăritorii tăi de pe Instagram",
            "es": "Tus seguidores de Instagram",
            "sq": "Ndjekësit e tu në Instagram"
          },
          "description": {
            "en": "List of accounts that follow you on Instagram.",
            "nl": "Lijst van accounts die jou op Instagram volgen.",
            "de": "Liste der Konten, die Ihnen auf Instagram folgen.",
            "pl": "Lista kont, które obserwują cię na Instagramie.",
            "tr": "Instagram'da seni takip eden hesapların listesi.",
            "ar": "قائمة الحسابات التي تتابعك على إنستغرام.",
            "ru": "Список аккаунтов, которые подписаны на вас в Instagram.",
            "it": "Elenco degli account che ti seguono su Instagram.",
            "ro": "Lista conturilor care te urmăresc pe Instagram.",
            "es": "Lista de cuentas que te siguen en Instagram.",
            "sq": "Lista e llogarive që të ndjekin në Instagram."
          },
          "headers": {
            "Account": {
              "en": "Account",
              "nl": "Account",
              "de": "Konto",
              "pl": "Konto",
              "tr": "Hesap",
              "ar": "الحساب",
              "ru": "Аккаунт",
              "it": "Account",
              "ro": "Cont",
              "es": "Cuenta",
              "sq": "Llogari"
            },
            "URL": {
              "en": "URL",
              "nl": "URL",
              "de": "URL",
              "pl": "URL",
              "tr": "URL",
              "ar": "الرابط",
              "ru": "URL-адрес",
              "it": "URL",
              "ro": "URL",
              "es": "URL",
              "sq": "URL"
            },
            "Date": {
              "en": "Date",
              "nl": "Datum en tijd",
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
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data.get("relationships_followers", [])
        else:
            items = data  # pyright: ignore

        for item in items:
            d = eh.dict_denester(item)
            datapoints.append((
                eh.fix_latin1_string(eh.find_item(d, "value") or eh.find_item(d, "title")),
                eh.find_item(d, "href"),
                eh.epoch_to_iso(eh.find_item(d, "timestamp"), errors=errors),
            ))
        out = pd.DataFrame(datapoints, columns=["Account", "URL", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def following_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "following.json",
) -> pd.DataFrame:
    """Extract the list of followed accounts into a DataFrame.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"following.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Account``, ``URL``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one account that the participant follows on Instagram, including when they started following.",
          "source_file": "following.json",
          "columns": {
            "Account": "Username or display name of the followed account.",
            "URL": "Direct URL to the followed account's Instagram profile.",
            "Date": "ISO 8601 timestamp of when the participant started following this account."
          }
        }

    Table config::

                {
          "id": "instagram_following",
          "title": {
            "en": "Accounts that you follow on Instagram",
            "nl": "Accounts die je volgt op Instagram",
            "de": "Konten, denen Sie auf Instagram folgen",
            "pl": "Konta, które obserwujesz na Instagramie",
            "tr": "Instagram'da takip ettiğin hesaplar",
            "ar": "الحسابات التي تتابعها على إنستغرام",
            "ru": "Аккаунты, на которые вы подписаны в Instagram",
            "it": "Account che segui su Instagram",
            "ro": "Conturile pe care le urmărești pe Instagram",
            "es": "Cuentas que sigues en Instagram",
            "sq": "Llogaritë që ndjek në Instagram"
          },
          "description": {
            "en": "In this table, you find the accounts that you follow on Instagram.",
            "nl": "In deze tabel zie je de accounts die je volgt op Instagram.",
            "de": "In dieser Tabelle finden Sie die Konten, denen Sie auf Instagram folgen.",
            "pl": "W tej tabeli znajdziesz konta, które obserwujesz na Instagramie.",
            "tr": "Bu tabloda Instagram'da takip ettiğin hesapları bulabilirsin.",
            "ar": "في هذا الجدول، تجد الحسابات التي تتابعها على إنستغرام.",
            "ru": "В этой таблице вы найдёте аккаунты, на которые вы подписаны в Instagram.",
            "it": "In questa tabella trovi gli account che segui su Instagram.",
            "ro": "În acest tabel găsești conturile pe care le urmărești pe Instagram.",
            "es": "En esta tabla encontrarás las cuentas que sigues en Instagram.",
            "sq": "Në këtë tabelë gjen llogaritë që ndjek në Instagram."
          },
          "headers": {
            "Account": {
              "en": "Account",
              "nl": "Account",
              "de": "Konto",
              "pl": "Konto",
              "tr": "Hesap",
              "ar": "الحساب",
              "ru": "Аккаунт",
              "it": "Account",
              "ro": "Cont",
              "es": "Cuenta",
              "sq": "Llogari"
            },
            "URL": {
              "en": "URL",
              "nl": "URL",
              "de": "URL",
              "pl": "URL",
              "tr": "URL",
              "ar": "الرابط",
              "ru": "URL-адрес",
              "it": "URL",
              "ro": "URL",
              "es": "URL",
              "sq": "URL"
            },
            "Date": {
              "en": "Date",
              "nl": "Datum en tijd",
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
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data["relationships_following"]  # pyright: ignore
        for item in items:
            d = eh.dict_denester(item)
            datapoints.append((
                eh.fix_latin1_string(eh.find_item(d, "title") or eh.find_item(d, "value")),
                eh.find_item(d, "href"),
                eh.epoch_to_iso(eh.find_item(d, "timestamp"), errors=errors),
            ))
        out = pd.DataFrame(datapoints, columns=["Account", "URL", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def ads_viewed_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "ads_viewed.json",
) -> pd.DataFrame:
    """Extract the list of viewed ads into a DataFrame.

    Supports both the list-at-root format and the dict format keyed by
    ``"impressions_history_ads_seen"``.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"ads_viewed.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Account name``, ``Name``, ``URL``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one advertisement impression shown to the participant on Instagram. Includes the advertiser identity and when the ad was displayed.",
          "source_file": "ads_viewed.json",
          "columns": {
            "Account name": "Username of the advertiser's Instagram account.",
            "Name": "Display name of the advertiser.",
            "URL": "URL associated with the advertisement.",
            "Date": "ISO 8601 timestamp of when the ad was shown to the participant."
          }
        }

    Table config::

                {
          "id": "instagram_ads_viewed",
          "title": {
            "en": "Ads viewed on Instagram",
            "nl": "Advertenties bekeken op Instagram",
            "de": "Auf Instagram angesehene Werbeanzeigen",
            "pl": "Wyświetlone reklamy na Instagramie",
            "tr": "Instagram'da görüntülenen reklamlar",
            "ar": "الإعلانات التي شاهدتها على إنستغرام",
            "ru": "Реклама, просмотренная в Instagram",
            "it": "Inserzioni visualizzate su Instagram",
            "ro": "Reclame vizualizate pe Instagram",
            "es": "Anuncios vistos en Instagram",
            "sq": "Reklamat e shikuara në Instagram"
          },
          "description": {
            "en": "List of ads that you viewed on Instagram.",
            "nl": "Lijst van advertenties die je op Instagram hebt bekeken.",
            "de": "Liste der Werbeanzeigen, die Sie sich auf Instagram angesehen haben.",
            "pl": "Lista reklam, które wyświetliłeś/aś na Instagramie.",
            "tr": "Instagram'da görüntülediğin reklamların listesi.",
            "ar": "قائمة الإعلانات التي شاهدتها على إنستغرام.",
            "ru": "Список рекламы, которую вы просмотрели в Instagram.",
            "it": "Elenco delle inserzioni che hai visualizzato su Instagram.",
            "ro": "Lista reclamelor pe care le-ai vizualizat pe Instagram.",
            "es": "Lista de anuncios que viste en Instagram.",
            "sq": "Lista e reklamave që ke shikuar në Instagram."
          },
          "headers": {
            "Account name": {
              "en": "Account name",
              "nl": "Accountnaam",
              "de": "Kontoname",
              "pl": "Nazwa konta",
              "tr": "Hesap adı",
              "ar": "اسم الحساب",
              "ru": "Имя аккаунта",
              "it": "Nome account",
              "ro": "Numele contului",
              "es": "Nombre de la cuenta",
              "sq": "Emri i llogarisë"
            },
            "Name": {
              "en": "Name",
              "nl": "Naam",
              "de": "Name",
              "pl": "Nazwa",
              "tr": "Ad",
              "ar": "الاسم",
              "ru": "Имя",
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
              "ar": "الرابط",
              "ru": "URL-адрес",
              "it": "URL",
              "ro": "URL",
              "es": "URL",
              "sq": "URL"
            },
            "Date": {
              "en": "Date",
              "nl": "Datum en tijd",
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
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("impressions_history_ads_seen", [])  # pyright: ignore
        else:
            items = []

        for item in items:  # pyright: ignore
            owner_name, owner_username, url = _extract_owner_details(item.get("label_values", []))
            datapoints.append((
                owner_username or owner_name,
                owner_name,
                url,
                eh.epoch_to_iso(item.get("timestamp", ""), errors=errors),
            ))

        out = pd.DataFrame(datapoints, columns=["Account name", "Name", "URL", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def posts_viewed_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "posts_viewed.json",
) -> pd.DataFrame:
    """Extract the list of viewed posts into a DataFrame.

    Handles both the older ``string_map_data`` format (dict root keyed by
    ``"impressions_history_posts_seen"``) and the newer ``label_values``
    list-at-root format.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"posts_viewed.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Author``, ``URL``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one post that appeared in the participant's Instagram feed and was registered as viewed. Captures the author and timing of each impression.",
          "source_file": "posts_viewed.json",
          "columns": {
            "Author": "Username or display name of the account that published the viewed post.",
            "URL": "Direct URL to the viewed post.",
            "Date": "ISO 8601 timestamp of when the post was viewed."
          }
        }

    Table config::

                {
          "id": "instagram_posts_viewed",
          "title": {
            "en": "Posts viewed on Instagram",
            "nl": "Berichten bekeken op Instagram",
            "de": "Auf Instagram angesehene Beiträge",
            "pl": "Wyświetlone posty na Instagramie",
            "tr": "Instagram'da görüntülenen gönderiler",
            "ar": "المنشورات التي شاهدتها على إنستغرام",
            "ru": "Публикации, просмотренные в Instagram",
            "it": "Post visualizzati su Instagram",
            "ro": "Postări vizualizate pe Instagram",
            "es": "Publicaciones vistas en Instagram",
            "sq": "Postimet e shikuara në Instagram"
          },
          "description": {
            "en": "In this table you find the accounts of posts you viewed on Instagram sorted over time. Below, you find visualizations of different parts of this table. First, you find a timeline showing you the number of posts you viewed over time. Next, you find a histogram indicating how many posts you have viewed per hour of the day.",
            "nl": "In deze tabel zie je de accounts van berichten die je op Instagram hebt bekeken, gesorteerd op tijd. Hieronder vind je visualisaties van verschillende onderdelen van deze tabel. Eerst zie je een tijdlijn met het aantal berichten dat je in de loop van de tijd hebt bekeken. Daarna zie je een histogram dat aangeeft hoeveel berichten je per uur van de dag hebt bekeken.",
            "de": "In dieser Tabelle finden Sie die Konten der Beiträge, die Sie auf Instagram angesehen haben, sortiert nach Zeit. Unten finden Sie Visualisierungen verschiedener Teile dieser Tabelle. Zuerst sehen Sie eine Zeitachse mit der Anzahl der Beiträge, die Sie im Laufe der Zeit angesehen haben. Danach sehen Sie ein Histogramm, das zeigt, wie viele Beiträge Sie pro Stunde des Tages angesehen haben.",
            "pl": "W tej tabeli znajdziesz konta postów, które wyświetliłeś/aś na Instagramie, posortowane chronologicznie. Poniżej znajdziesz wizualizacje różnych części tej tabeli. Najpierw zobaczysz oś czasu pokazującą liczbę postów, które wyświetlałeś/aś w czasie. Następnie zobaczysz histogram pokazujący, ile postów wyświetlałeś/aś w poszczególnych godzinach doby.",
            "tr": "Bu tabloda, zamana göre sıralanmış olarak Instagram'da görüntülediğin gönderilerin hesaplarını bulabilirsin. Aşağıda, bu tablonun farklı bölümlerine ait görselleştirmeler bulunur. İlk olarak, zaman içinde görüntülediğin gönderi sayısını gösteren bir zaman çizelgesi görürsün. Ardından, günün saatine göre kaç gönderi görüntülediğini gösteren bir histogram görürsün.",
            "ar": "في هذا الجدول، تجد حسابات المنشورات التي شاهدتها على إنستغرام مرتبة حسب الوقت. أدناه، تجد تصورات لأجزاء مختلفة من هذا الجدول. أولاً، تجد خطاً زمنياً يوضح عدد المنشورات التي شاهدتها عبر الوقت. بعد ذلك، تجد رسماً بيانياً يوضح عدد المنشورات التي شاهدتها في كل ساعة من اليوم.",
            "ru": "В этой таблице вы найдёте аккаунты публикаций, которые вы просматривали в Instagram, отсортированные по времени. Ниже вы найдёте визуализации различных частей этой таблицы. Сначала вы увидите временную шкалу с количеством просмотренных публикаций с течением времени. Затем вы увидите гистограмму, показывающую, сколько публикаций вы просматривали по часам суток.",
            "it": "In questa tabella trovi gli account dei post che hai visualizzato su Instagram, ordinati nel tempo. Di seguito trovi le visualizzazioni di diverse parti di questa tabella. Prima trovi una sequenza temporale che mostra il numero di post che hai visualizzato nel tempo. Poi trovi un istogramma che indica quanti post hai visualizzato per ogni ora del giorno.",
            "ro": "În acest tabel găsești conturile postărilor pe care le-ai vizualizat pe Instagram, sortate în timp. Mai jos găsești vizualizări ale diferitelor părți ale acestui tabel. Mai întâi găsești o cronologie care arată numărul de postări vizualizate în timp. Apoi găsești o histogramă care indică câte postări ai vizualizat pe oră din zi.",
            "es": "En esta tabla encontrarás las cuentas de las publicaciones que viste en Instagram, ordenadas cronológicamente. A continuación, encontrarás visualizaciones de diferentes partes de esta tabla. Primero, verás una línea de tiempo que muestra el número de publicaciones que viste a lo largo del tiempo. Luego, verás un histograma que indica cuántas publicaciones viste por hora del día.",
            "sq": "Në këtë tabelë gjen llogaritë e postimeve që ke shikuar në Instagram, të renditura sipas kohës. Më poshtë gjen vizualizime të pjesëve të ndryshme të kësaj tabele. Së pari, gjen një vijë kohore që tregon numrin e postimeve që ke parë me kalimin e kohës. Më pas, gjen një histogram që tregon sa postime ke parë për çdo orë të ditës."
          },
          "headers": {
            "Author": {
              "en": "Author",
              "nl": "Auteur",
              "de": "Autor",
              "pl": "Autor",
              "tr": "Yazar",
              "ar": "الكاتب",
              "ru": "Автор",
              "it": "Autore",
              "ro": "Autor",
              "es": "Autor",
              "sq": "Autor"
            },
            "URL": {
              "en": "URL",
              "nl": "URL",
              "de": "URL",
              "pl": "URL",
              "tr": "URL",
              "ar": "الرابط",
              "ru": "URL-адрес",
              "it": "URL",
              "ro": "URL",
              "es": "URL",
              "sq": "URL"
            },
            "Date": {
              "en": "Date",
              "nl": "Datum en tijd",
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
                "en": "The total number of Instagram posts you viewed over time",
                "nl": "Het totale aantal Instagram-berichten dat je in de loop van de tijd hebt bekeken",
                "de": "Die Gesamtzahl der Instagram-Beiträge, die Sie im Laufe der Zeit angesehen haben",
                "pl": "Łączna liczba postów na Instagramie, które wyświetliłeś/aś w czasie",
                "tr": "Zaman içinde görüntülediğin Instagram gönderilerinin toplam sayısı",
                "ar": "إجمالي عدد منشورات إنستغرام التي شاهدتها عبر الوقت",
                "ru": "Общее количество публикаций в Instagram, которые вы просмотрели с течением времени",
                "it": "Il numero totale di post di Instagram che hai visualizzato nel tempo",
                "ro": "Numărul total de postări de pe Instagram pe care le-ai vizualizat în timp",
                "es": "El número total de publicaciones de Instagram que viste a lo largo del tiempo",
                "sq": "Numri total i postimeve në Instagram që ke parë me kalimin e kohës"
              },
              "type": "area",
              "group": {
                "column": "Date",
                "dateFormat": "auto"
              },
              "values": [
                {
                  "label": "Count",
                  "aggregate": "count"
                }
              ]
            },
            {
              "title": {
                "en": "The total number of Instagram posts you have viewed per hour of the day",
                "nl": "Het totale aantal Instagram-berichten dat je per uur van de dag hebt bekeken",
                "de": "Die Gesamtzahl der Instagram-Beiträge, die Sie pro Stunde des Tages angesehen haben",
                "pl": "Łączna liczba postów na Instagramie, które wyświetliłeś/aś w poszczególnych godzinach doby",
                "tr": "Günün saatine göre görüntülediğin Instagram gönderilerinin toplam sayısı",
                "ar": "إجمالي عدد منشورات إنستغرام التي شاهدتها في كل ساعة من اليوم",
                "ru": "Общее количество публикаций в Instagram, которые вы просматривали по часам суток",
                "it": "Il numero totale di post di Instagram che hai visualizzato per ogni ora del giorno",
                "ro": "Numărul total de postări de pe Instagram pe care le-ai vizualizat pe oră din zi",
                "es": "El número total de publicaciones de Instagram que viste por hora del día",
                "sq": "Numri total i postimeve në Instagram që ke parë për çdo orë të ditës"
              },
              "type": "bar",
              "group": {
                "column": "Date",
                "dateFormat": "hour_cycle",
                "label": "Hour of the day"
              },
              "values": [
                {
                  "label": "Count"
                }
              ]
            }
          ]
        }
    """
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data["impressions_history_posts_seen"]  # pyright: ignore
            for item in items:
                string_map_data = item.get("string_map_data", {})
                author = _first_present(string_map_data, _AUTHOR_LABELS)
                time = _first_present(string_map_data, _TIME_LABELS)
                url = _first_present(string_map_data, _URL_LABELS)
                datapoints.append((
                    eh.fix_latin1_string(str(author.get("value", ""))),
                    url.get("href", ""),
                    eh.epoch_to_iso(time.get("timestamp", ""), errors=errors),
                ))
        else:
            for item in data:  # pyright: ignore
                owner_name, owner_username, url = _extract_owner_details(item.get("label_values", []))
                datapoints.append((
                    owner_username or owner_name,
                    url,
                    eh.epoch_to_iso(item.get("timestamp", ""), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Author", "URL", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def videos_watched_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "videos_watched.json",
) -> pd.DataFrame:
    """Extract the list of watched videos into a DataFrame.

    Handles both the older ``string_map_data`` format (dict root keyed by
    ``"impressions_history_videos_watched"``) and the newer ``label_values``
    list-at-root format.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"videos_watched.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Author``, ``URL``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one video (including Reels) that the participant watched on Instagram. Captures the creator and timing of each view event.",
          "source_file": "videos_watched.json",
          "columns": {
            "Author": "Username or display name of the account that published the watched video.",
            "URL": "Direct URL to the watched video.",
            "Date": "ISO 8601 timestamp of when the video was watched."
          }
        }

    Table config::

                {
          "id": "instagram_videos_watched",
          "title": {
            "en": "Videos watched on Instagram",
            "nl": "Video's bekeken op Instagram",
            "de": "Auf Instagram angesehene Videos",
            "pl": "Obejrzane filmy na Instagramie",
            "tr": "Instagram'da izlenen videolar",
            "ar": "مقاطع الفيديو التي شاهدتها على إنستغرام",
            "ru": "Видео, просмотренные в Instagram",
            "it": "Video visualizzati su Instagram",
            "ro": "Videoclipuri vizionate pe Instagram",
            "es": "Vídeos vistos en Instagram",
            "sq": "Videot e shikuara në Instagram"
          },
          "description": {
            "en": "In this table you find the accounts of videos you watched on Instagram sorted over time. Below, you find a timeline showing you the number of videos you watched over time.",
            "nl": "In deze tabel zie je de accounts van video's die je op Instagram hebt bekeken, gesorteerd op tijd. Hieronder zie je een tijdlijn met het aantal video's dat je in de loop van de tijd hebt bekeken.",
            "de": "In dieser Tabelle finden Sie die Konten der Videos, die Sie auf Instagram angesehen haben, sortiert nach Zeit. Unten sehen Sie eine Zeitachse mit der Anzahl der Videos, die Sie im Laufe der Zeit angesehen haben.",
            "pl": "W tej tabeli znajdziesz konta filmów, które obejrzałeś/aś na Instagramie, posortowane chronologicznie. Poniżej zobaczysz oś czasu pokazującą liczbę filmów, które oglądałeś/aś w czasie.",
            "tr": "Bu tabloda, zamana göre sıralanmış olarak Instagram'da izlediğin videoların hesaplarını bulabilirsin. Aşağıda, zaman içinde izlediğin video sayısını gösteren bir zaman çizelgesi görürsün.",
            "ar": "في هذا الجدول، تجد حسابات مقاطع الفيديو التي شاهدتها على إنستغرام مرتبة حسب الوقت. أدناه، ترى خطاً زمنياً يوضح عدد مقاطع الفيديو التي شاهدتها عبر الوقت.",
            "ru": "В этой таблице вы найдёте аккаунты видео, которые вы смотрели в Instagram, отсортированные по времени. Ниже вы увидите временную шкалу с количеством просмотренных видео с течением времени.",
            "it": "In questa tabella trovi gli account dei video che hai guardato su Instagram, ordinati nel tempo. Di seguito vedi una sequenza temporale con il numero di video che hai guardato nel tempo.",
            "ro": "În acest tabel găsești conturile videoclipurilor pe care le-ai vizionat pe Instagram, sortate în timp. Mai jos vezi o cronologie cu numărul de videoclipuri vizionate în timp.",
            "es": "En esta tabla encontrarás las cuentas de los vídeos que viste en Instagram, ordenados cronológicamente. A continuación, verás una línea de tiempo con el número de vídeos que viste a lo largo del tiempo.",
            "sq": "Në këtë tabelë gjen llogaritë e videove që ke parë në Instagram, të renditura sipas kohës. Më poshtë sheh një vijë kohore me numrin e videove që ke parë me kalimin e kohës."
          },
          "headers": {
            "Author": {
              "en": "Author",
              "nl": "Auteur",
              "de": "Autor",
              "pl": "Autor",
              "tr": "Yazar",
              "ar": "الكاتب",
              "ru": "Автор",
              "it": "Autore",
              "ro": "Autor",
              "es": "Autor",
              "sq": "Autor"
            },
            "URL": {
              "en": "URL",
              "nl": "URL",
              "de": "URL",
              "pl": "URL",
              "tr": "URL",
              "ar": "الرابط",
              "ru": "URL-адрес",
              "it": "URL",
              "ro": "URL",
              "es": "URL",
              "sq": "URL"
            },
            "Date": {
              "en": "Date",
              "nl": "Datum en tijd",
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
                "en": "The total number of videos watched on Instagram over time",
                "nl": "Het totale aantal video's dat je op Instagram hebt bekeken in de loop van de tijd",
                "de": "Die Gesamtzahl der auf Instagram angesehenen Videos im Laufe der Zeit",
                "pl": "Łączna liczba filmów obejrzanych na Instagramie w czasie",
                "tr": "Zaman içinde Instagram'da izlenen videoların toplam sayısı",
                "ar": "إجمالي عدد مقاطع الفيديو التي شوهدت على إنستغرام عبر الوقت",
                "ru": "Общее количество видео, просмотренных в Instagram с течением времени",
                "it": "Il numero totale di video visualizzati su Instagram nel tempo",
                "ro": "Numărul total de videoclipuri vizionate pe Instagram în timp",
                "es": "El número total de vídeos vistos en Instagram a lo largo del tiempo",
                "sq": "Numri total i videove të shikuara në Instagram me kalimin e kohës"
              },
              "type": "area",
              "group": {
                "column": "Date",
                "dateFormat": "auto"
              },
              "values": [
                {
                  "aggregate": "count",
                  "label": "Count"
                }
              ]
            }
          ]
        }
    """
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data["impressions_history_videos_watched"]  # pyright: ignore
            for item in items:
                string_map_data = item.get("string_map_data", {})
                author = _first_present(string_map_data, _AUTHOR_LABELS)
                time = _first_present(string_map_data, _TIME_LABELS)
                url = _first_present(string_map_data, _URL_LABELS)
                datapoints.append((
                    eh.fix_latin1_string(str(author.get("value", ""))),
                    url.get("href", ""),
                    eh.epoch_to_iso(time.get("timestamp", ""), errors=errors),
                ))
        else:
            for item in data:  # pyright: ignore
                owner_name, owner_username, url = _extract_owner_details(item.get("label_values", []))
                datapoints.append((
                    owner_username or owner_name,
                    url,
                    eh.epoch_to_iso(item.get("timestamp", ""), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Author", "URL", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def post_comments_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename_pattern: str = r"(^|/)post_comments(?:_\d+)?\.json$",
) -> pd.DataFrame:
    """Extract all post comments across multiple matching files into a DataFrame.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename_pattern:
        Regular expression matched against archive member paths.  All matching
        files are read and combined.  Defaults to a pattern that matches
        ``post_comments.json``, ``post_comments_1.json``, etc.

    Returns
    -------
    pd.DataFrame
        Columns: ``Comment``, ``Media owner``, ``Date``.
        Empty DataFrame when no matching files are found or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one comment the participant posted on an Instagram post. Covers all matching comment files in the archive (e.g. post_comments.json, post_comments_1.json).",
          "source_file": "post_comments*.json",
          "columns": {
            "Comment": "The full text of the comment posted by the participant.",
            "Media owner": "Username of the account that owns the post the comment was placed on.",
            "Date": "ISO 8601 timestamp of when the comment was posted."
          }
        }

    Table config::

                {
          "id": "instagram_post_comments",
          "title": {
            "en": "Comments posted on Instagram",
            "nl": "Reacties geplaatst op Instagram",
            "de": "Auf Instagram veröffentlichte Kommentare",
            "pl": "Komentarze opublikowane na Instagramie",
            "tr": "Instagram'da paylaşılan yorumlar",
            "ar": "التعليقات التي نشرتها على إنستغرام",
            "ru": "Комментарии, опубликованные в Instagram",
            "it": "Commenti pubblicati su Instagram",
            "ro": "Comentarii publicate pe Instagram",
            "es": "Comentarios publicados en Instagram",
            "sq": "Komentet e publikuara në Instagram"
          },
          "description": {
            "en": "List of comments you posted on Instagram.",
            "nl": "Lijst van reacties die je op Instagram hebt geplaatst.",
            "de": "Liste der Kommentare, die Sie auf Instagram veröffentlicht haben.",
            "pl": "Lista komentarzy, które opublikowałeś/aś na Instagramie.",
            "tr": "Instagram'da paylaştığın yorumların listesi.",
            "ar": "قائمة التعليقات التي نشرتها على إنستغرام.",
            "ru": "Список комментариев, которые вы опубликовали в Instagram.",
            "it": "Elenco dei commenti che hai pubblicato su Instagram.",
            "ro": "Lista comentariilor pe care le-ai publicat pe Instagram.",
            "es": "Lista de comentarios que publicaste en Instagram.",
            "sq": "Lista e komenteve që ke publikuar në Instagram."
          },
          "headers": {
            "Comment": {
              "en": "Comment",
              "nl": "Reactie",
              "de": "Kommentar",
              "pl": "Komentarz",
              "tr": "Yorum",
              "ar": "تعليق",
              "ru": "Комментарий",
              "it": "Commento",
              "ro": "Comentariu",
              "es": "Comentario",
              "sq": "Koment"
            },
            "Media owner": {
              "en": "Media owner",
              "nl": "Media-eigenaar",
              "de": "Medieninhaber",
              "pl": "Właściciel mediów",
              "tr": "Medya sahibi",
              "ar": "مالك الوسائط",
              "ru": "Владелец медиафайла",
              "it": "Proprietario del contenuto multimediale",
              "ro": "Proprietarul conținutului media",
              "es": "Propietario del contenido multimedia",
              "sq": "Pronari i medias"
            },
            "Date": {
              "en": "Date",
              "nl": "Datum en tijd",
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
    out = pd.DataFrame()
    datapoints = []

    try:
        results = reader.json_all(filename_pattern)
        if not results:
            return pd.DataFrame()

        for result in results:
            data = result.data
            items = data if isinstance(data, list) else data.get("comments_media_comments", [])
            for item in items:  # pyright: ignore[assignment]
                string_map_data = item.get("string_map_data", {})
                comment = _first_present(string_map_data, _COMMENT_LABELS)
                owner = _first_present(string_map_data, _MEDIA_OWNER_LABELS)
                time = _first_present(string_map_data, _TIME_LABELS)
                datapoints.append((
                    eh.fix_latin1_string(str(comment.get("value", ""))),
                    eh.fix_latin1_string(str(owner.get("value", ""))),
                    eh.epoch_to_iso(time.get("timestamp", ""), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Comment", "Media owner", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def liked_comments_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "liked_comments.json",
) -> pd.DataFrame:
    """Extract the list of liked comments into a DataFrame.

    Handles both the older ``string_list_data`` format (dict root keyed by
    ``"likes_comment_likes"``) and the newer ``label_values`` list-at-root
    format.  Note that the comment text is not available in the newer format.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"liked_comments.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Account name``, ``Value``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one comment the participant liked on Instagram. Comment text may be absent in newer export formats.",
          "source_file": "liked_comments.json",
          "columns": {
            "Account name": "Username of the account whose comment was liked.",
            "Value": "Text of the liked comment, if available in the export (empty in newer export formats).",
            "Date": "ISO 8601 timestamp of when the comment was liked."
          }
        }

    Table config::

                {
          "id": "instagram_liked_comments",
          "title": {
            "en": "Instagram liked comments",
            "nl": "Instagram-reacties die je leuk vond",
            "de": "Auf Instagram gelikte Kommentare",
            "pl": "Polubione komentarze na Instagramie",
            "tr": "Instagram'da beğenilen yorumlar",
            "ar": "التعليقات التي أعجبت بها على إنستغرام",
            "ru": "Комментарии, которые вам понравились в Instagram",
            "it": "Commenti apprezzati su Instagram",
            "ro": "Comentarii apreciate pe Instagram",
            "es": "Comentarios que te gustaron en Instagram",
            "sq": "Komentet që i ke pëlqyer në Instagram"
          },
          "description": {
            "en": "List of comments that you liked on Instagram.",
            "nl": "Lijst van reacties die je leuk vond op Instagram.",
            "de": "Liste der Kommentare, die Ihnen auf Instagram gefallen haben.",
            "pl": "Lista komentarzy, które polubiłeś/aś na Instagramie.",
            "tr": "Instagram'da beğendiğin yorumların listesi.",
            "ar": "قائمة التعليقات التي أعجبت بها على إنستغرام.",
            "ru": "Список комментариев, которые вам понравились в Instagram.",
            "it": "Elenco dei commenti che ti sono piaciuti su Instagram.",
            "ro": "Lista comentariilor care ți-au plăcut pe Instagram.",
            "es": "Lista de comentarios que te gustaron en Instagram.",
            "sq": "Lista e komenteve që i ke pëlqyer në Instagram."
          },
          "headers": {
            "Account name": {
              "en": "Account name",
              "nl": "Accountnaam",
              "de": "Kontoname",
              "pl": "Nazwa konta",
              "tr": "Hesap adı",
              "ar": "اسم الحساب",
              "ru": "Имя аккаунта",
              "it": "Nome account",
              "ro": "Numele contului",
              "es": "Nombre de la cuenta",
              "sq": "Emri i llogarisë"
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
            },
            "Date": {
              "en": "Date",
              "nl": "Datum en tijd",
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
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data["likes_comment_likes"]  # pyright: ignore
            for item in items:
                entry = item.get("string_list_data", [{}])[0]
                datapoints.append((
                    eh.fix_latin1_string(item.get("title", "")),
                    eh.fix_latin1_string(entry.get("value", "")),
                    eh.epoch_to_iso(entry.get("timestamp", ""), errors=errors),
                ))
        else:
            for item in data:  # pyright: ignore
                owner_name, owner_username, url = _extract_owner_details(item.get("label_values", []))
                datapoints.append((
                    owner_username or owner_name,
                    "",  # comment text not available in label_values format
                    eh.epoch_to_iso(item.get("timestamp", ""), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Account name", "Value", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def liked_posts_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "liked_posts.json",
) -> pd.DataFrame:
    """Extract the list of liked posts into a DataFrame.

    Handles both the older ``dict_denester`` format (dict root keyed by
    ``"likes_media_likes"``) and the newer ``label_values`` list-at-root
    format.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"liked_posts.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Account name``, ``Value``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one post the participant liked on Instagram, including the account whose post was liked and when the like was given.",
          "source_file": "liked_posts.json",
          "columns": {
            "Account name": "Username of the account whose post was liked.",
            "Value": "Display name or additional label for the liked post, depending on export format.",
            "Date": "ISO 8601 timestamp of when the post was liked."
          }
        }

    Table config::

                {
          "id": "instagram_liked_posts",
          "title": {
            "en": "Instagram liked posts",
            "nl": "Instagram-berichten die je leuk vond",
            "de": "Auf Instagram gelikte Beiträge",
            "pl": "Polubione posty na Instagramie",
            "tr": "Instagram'da beğenilen gönderiler",
            "ar": "المنشورات التي أعجبت بها على إنستغرام",
            "ru": "Публикации, которые вам понравились в Instagram",
            "it": "Post apprezzati su Instagram",
            "ro": "Postări apreciate pe Instagram",
            "es": "Publicaciones que te gustaron en Instagram",
            "sq": "Postimet që i ke pëlqyer në Instagram"
          },
          "description": {
            "en": "List of posts that you liked on Instagram.",
            "nl": "Lijst van berichten die je leuk vond op Instagram.",
            "de": "Liste der Beiträge, die Ihnen auf Instagram gefallen haben.",
            "pl": "Lista postów, które polubiłeś/aś na Instagramie.",
            "tr": "Instagram'da beğendiğin gönderilerin listesi.",
            "ar": "قائمة المنشورات التي أعجبت بها على إنستغرام.",
            "ru": "Список публикаций, которые вам понравились в Instagram.",
            "it": "Elenco dei post che ti sono piaciuti su Instagram.",
            "ro": "Lista postărilor care ți-au plăcut pe Instagram.",
            "es": "Lista de publicaciones que te gustaron en Instagram.",
            "sq": "Lista e postimeve që i ke pëlqyer në Instagram."
          },
          "headers": {
            "Account name": {
              "en": "Account name",
              "nl": "Accountnaam",
              "de": "Kontoname",
              "pl": "Nazwa konta",
              "tr": "Hesap adı",
              "ar": "اسم الحساب",
              "ru": "Имя аккаунта",
              "it": "Nome account",
              "ro": "Numele contului",
              "es": "Nombre de la cuenta",
              "sq": "Emri i llogarisë"
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
            },
            "Date": {
              "en": "Date",
              "nl": "Datum en tijd",
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
                "en": "Most liked accounts",
                "nl": "Meest gelikete accounts",
                "de": "Am häufigsten gelikte Konten",
                "pl": "Najczęściej polubione konta",
                "tr": "En çok beğenilen hesaplar",
                "ar": "الحسابات الأكثر إعجاباً",
                "ru": "Самые популярные аккаунты по лайкам",
                "it": "Account più apprezzati",
                "ro": "Cele mai apreciate conturi",
                "es": "Cuentas más gustadas",
                "sq": "Llogaritë më të pëlqyera"
              },
              "type": "wordcloud",
              "textColumn": "Account name",
              "tokenize": false
            }
          ]
        }
    """
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data["likes_media_likes"]  # pyright: ignore
            for item in items:
                d = eh.dict_denester(item)
                datapoints.append((
                    eh.fix_latin1_string(eh.find_item(d, "title")),
                    eh.fix_latin1_string(eh.find_item(d, "value")),
                    eh.epoch_to_iso(eh.find_item(d, "timestamp"), errors=errors),
                ))
        else:
            for item in data:  # pyright: ignore
                owner_name, owner_username, url = _extract_owner_details(item.get("label_values", []))
                datapoints.append((
                    owner_username or owner_name,
                    owner_name,
                    eh.epoch_to_iso(item.get("timestamp", ""), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Account name", "Value", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def profile_searches_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "profile_searches.json",
) -> pd.DataFrame:
    """Extract the list of profile searches into a DataFrame.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"profile_searches.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Timestamp``, ``Name``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one profile search performed by the participant on Instagram, recording what was searched and when.",
          "source_file": "profile_searches.json",
          "columns": {
            "Name": "Username or display name that was searched for.",
            "Timestamp": "ISO 8601 timestamp of when the search was performed."
          }
        }

    Table config::

                {
          "id": "instagram_profile_searches",
          "title": {
            "en": "Your Instagram profile searches",
            "nl": "Je Instagram-profielzoekopdrachten",
            "de": "Ihre Instagram-Profilsuchen",
            "pl": "Twoje wyszukiwania profili na Instagramie",
            "tr": "Instagram profil aramaların",
            "ar": "عمليات بحثك عن الملفات الشخصية على إنستغرام",
            "ru": "Ваши поиски профилей в Instagram",
            "it": "Le tue ricerche di profili su Instagram",
            "ro": "Căutările tale de profiluri pe Instagram",
            "es": "Tus búsquedas de perfiles en Instagram",
            "sq": "Kërkimet e tua për profile në Instagram"
          },
          "description": {
            "en": "List of profiles you have searched for on Instagram.",
            "nl": "Lijst van profielen die je op Instagram hebt gezocht.",
            "de": "Liste der Profile, nach denen Sie auf Instagram gesucht haben.",
            "pl": "Lista profili, których szukałeś/aś na Instagramie.",
            "tr": "Instagram'da aradığın profillerin listesi.",
            "ar": "قائمة الملفات الشخصية التي بحثت عنها على إنستغرام.",
            "ru": "Список профилей, которые вы искали в Instagram.",
            "it": "Elenco dei profili che hai cercato su Instagram.",
            "ro": "Lista profilurilor pe care le-ai căutat pe Instagram.",
            "es": "Lista de perfiles que buscaste en Instagram.",
            "sq": "Lista e profileve që ke kërkuar në Instagram."
          },
          "headers": {
            "Name": {
              "en": "Name",
              "nl": "Naam",
              "de": "Name",
              "pl": "Nazwa",
              "tr": "Ad",
              "ar": "الاسم",
              "ru": "Имя",
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
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data["searches_user"]  # pyright: ignore
        for item in items:
            d = eh.dict_denester(item)
            datapoints.append((
                eh.epoch_to_iso(eh.find_item(d, "timestamp"), errors=errors),
                eh.fix_latin1_string(eh.find_item(d, "title") or eh.find_item(d, "value")),
            ))
        out = pd.DataFrame(datapoints, columns=["Timestamp", "Name"])  # pyright: ignore
        out = _sort_by_date(out, "Timestamp")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def story_likes_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "story_likes.json",
) -> pd.DataFrame:
    """Extract the list of liked stories into a DataFrame.

    Handles both the older ``string_list_data`` format (dict root keyed by
    ``"story_activities_story_likes"``) and the newer ``label_values``
    list-at-root format.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"story_likes.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Account name``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one Instagram Story the participant liked, recording the account whose story was liked and when.",
          "source_file": "story_likes.json",
          "columns": {
            "Account name": "Username of the account whose story was liked.",
            "Date": "ISO 8601 timestamp of when the story was liked."
          }
        }

    Table config::

                {
          "id": "instagram_story_likes",
          "title": {
            "en": "Story likes on Instagram",
            "nl": "Story-likes op Instagram",
            "de": "Story-Likes auf Instagram",
            "pl": "Polubienia relacji na Instagramie",
            "tr": "Instagram hikaye beğenilerin",
            "ar": "إعجاباتك بالقصص على إنستغرام",
            "ru": "Понравившиеся истории в Instagram",
            "it": "Storie apprezzate su Instagram",
            "ro": "Aprecieri la povești pe Instagram",
            "es": "Historias que te gustaron en Instagram",
            "sq": "Pëlqimet e stories në Instagram"
          },
          "description": {
            "en": "List of Instagram stories you liked.",
            "nl": "Lijst van Instagram-stories die je leuk vond.",
            "de": "Liste der Instagram-Storys, die Ihnen gefallen haben.",
            "pl": "Lista relacji na Instagramie, które polubiłeś/aś.",
            "tr": "Beğendiğin Instagram hikayelerinin listesi.",
            "ar": "قائمة قصص إنستغرام التي أعجبت بها.",
            "ru": "Список историй в Instagram, которые вам понравились.",
            "it": "Elenco delle storie di Instagram che ti sono piaciute.",
            "ro": "Lista poveștilor de pe Instagram care ți-au plăcut.",
            "es": "Lista de historias de Instagram que te gustaron.",
            "sq": "Lista e stories në Instagram që i ke pëlqyer."
          },
          "headers": {
            "Account name": {
              "en": "Account name",
              "nl": "Accountnaam",
              "de": "Kontoname",
              "pl": "Nazwa konta",
              "tr": "Hesap adı",
              "ar": "اسم الحساب",
              "ru": "Имя аккаунта",
              "it": "Nome account",
              "ro": "Numele contului",
              "es": "Nombre de la cuenta",
              "sq": "Emri i llogarisë"
            },
            "Date": {
              "en": "Date",
              "nl": "Datum en tijd",
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
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data["story_activities_story_likes"]  # pyright: ignore
            for item in items:
                entry = item.get("string_list_data", [{}])[0]
                datapoints.append((
                    eh.fix_latin1_string(item.get("title", "")),
                    eh.epoch_to_iso(entry.get("timestamp", ""), errors=errors),
                ))
        else:
            for item in data:  # pyright: ignore
                owner_name, owner_username, _ = _extract_owner_details(item.get("label_values", []))
                datapoints.append((
                    owner_username or owner_name,
                    eh.epoch_to_iso(item.get("timestamp", ""), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Account name", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def threads_viewed_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "threads_viewed.json",
) -> pd.DataFrame:
    """Extract the list of viewed Threads posts into a DataFrame.

    Handles both the older ``string_map_data`` format (dict root keyed by
    ``"text_post_app_text_post_app_posts_seen"``) and the newer
    ``label_values`` list-at-root format.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"threads_viewed.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Author``, ``URL``, ``Date``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one post on Threads (Meta's text-based social network linked to Instagram) that the participant viewed, including the author and timing.",
          "source_file": "threads_viewed.json",
          "columns": {
            "Author": "Username or display name of the account that published the viewed Threads post.",
            "URL": "Direct URL to the viewed Threads post.",
            "Date": "ISO 8601 timestamp of when the post was viewed."
          }
        }

    Table config::

                {
          "id": "instagram_threads_viewed",
          "title": {
            "en": "Threads viewed",
            "nl": "Threads bekeken",
            "de": "Angesehene Threads-Beiträge",
            "pl": "Wyświetlone posty na Threads",
            "tr": "Görüntülenen Threads gönderileri",
            "ar": "منشورات Threads التي شاهدتها",
            "ru": "Просмотренные публикации в Threads",
            "it": "Post di Threads visualizzati",
            "ro": "Postări Threads vizualizate",
            "es": "Publicaciones de Threads vistas",
            "sq": "Postimet e Threads të shikuara"
          },
          "description": {
            "en": "List of Threads posts you viewed.",
            "nl": "Lijst van Threads-berichten die je hebt bekeken.",
            "de": "Liste der Threads-Beiträge, die Sie sich angesehen haben.",
            "pl": "Lista postów z Threads, które wyświetliłeś/aś.",
            "tr": "Görüntülediğin Threads gönderilerinin listesi.",
            "ar": "قائمة منشورات Threads التي شاهدتها.",
            "ru": "Список публикаций Threads, которые вы просмотрели.",
            "it": "Elenco dei post di Threads che hai visualizzato.",
            "ro": "Lista postărilor Threads pe care le-ai vizualizat.",
            "es": "Lista de publicaciones de Threads que viste.",
            "sq": "Lista e postimeve të Threads që ke parë."
          },
          "headers": {
            "Author": {
              "en": "Author",
              "nl": "Auteur",
              "de": "Autor",
              "pl": "Autor",
              "tr": "Yazar",
              "ar": "الكاتب",
              "ru": "Автор",
              "it": "Autore",
              "ro": "Autor",
              "es": "Autor",
              "sq": "Autor"
            },
            "URL": {
              "en": "URL",
              "nl": "URL",
              "de": "URL",
              "pl": "URL",
              "tr": "URL",
              "ar": "الرابط",
              "ru": "URL-адрес",
              "it": "URL",
              "ro": "URL",
              "es": "URL",
              "sq": "URL"
            },
            "Date": {
              "en": "Date",
              "nl": "Datum en tijd",
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
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        if isinstance(data, dict):
            items = data["text_post_app_text_post_app_posts_seen"]  # pyright: ignore
            for item in items:
                string_map_data = item.get("string_map_data", {})
                author = _first_present(string_map_data, _AUTHOR_LABELS)
                time = _first_present(string_map_data, _TIME_LABELS)
                url = _first_present(string_map_data, _URL_LABELS)
                datapoints.append((
                    eh.fix_latin1_string(str(author.get("value", ""))),
                    url.get("href", ""),
                    eh.epoch_to_iso(time.get("timestamp", ""), errors=errors),
                ))
        else:
            for item in data:  # pyright: ignore
                owner_name, owner_username, url = _extract_owner_details(item.get("label_values", []))
                datapoints.append((
                    owner_username or owner_name,
                    url,
                    eh.epoch_to_iso(item.get("timestamp", ""), errors=errors),
                ))

        out = pd.DataFrame(datapoints, columns=["Author", "URL", "Date"])  # pyright: ignore
        out = _sort_by_date(out, "Date")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


def saved_posts_to_df(
    reader: ZipArchiveReader,
    errors: Counter,
    *,
    filename: str = "saved_posts.json",
) -> pd.DataFrame:
    """Extract the list of saved posts into a DataFrame.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.
    filename:
        Path inside the zip archive to read.  Defaults to
        ``"saved_posts.json"``.

    Returns
    -------
    pd.DataFrame
        Columns: ``Title``, ``URL``, ``Timestamp``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one post the participant bookmarked (saved) on Instagram for later viewing.",
          "source_file": "saved_posts.json",
          "columns": {
            "Title": "Title or label of the saved post as stored in the export.",
            "URL": "Direct URL to the saved post.",
            "Timestamp": "ISO 8601 timestamp of when the post was saved."
          }
        }

    Table config::

                {
          "id": "instagram_saved_posts",
          "title": {
            "en": "Your saved posts on Instagram",
            "nl": "Je opgeslagen berichten op Instagram",
            "de": "Ihre gespeicherten Beiträge auf Instagram",
            "pl": "Twoje zapisane posty na Instagramie",
            "tr": "Instagram'da kaydettiğin gönderiler",
            "ar": "منشوراتك المحفوظة على إنستغرام",
            "ru": "Ваши сохранённые публикации в Instagram",
            "it": "I tuoi post salvati su Instagram",
            "ro": "Postările tale salvate pe Instagram",
            "es": "Tus publicaciones guardadas en Instagram",
            "sq": "Postimet e tua të ruajtura në Instagram"
          },
          "description": {
            "en": "List of posts you have saved on Instagram.",
            "nl": "Lijst van berichten die je hebt opgeslagen op Instagram.",
            "de": "Liste der Beiträge, die Sie auf Instagram gespeichert haben.",
            "pl": "Lista postów, które zapisałeś/aś na Instagramie.",
            "tr": "Instagram'da kaydettiğin gönderilerin listesi.",
            "ar": "قائمة المنشورات التي حفظتها على إنستغرام.",
            "ru": "Список публикаций, которые вы сохранили в Instagram.",
            "it": "Elenco dei post che hai salvato su Instagram.",
            "ro": "Lista postărilor pe care le-ai salvat pe Instagram.",
            "es": "Lista de publicaciones que guardaste en Instagram.",
            "sq": "Lista e postimeve që ke ruajtur në Instagram."
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
            },
            "URL": {
              "en": "URL",
              "nl": "URL",
              "de": "URL",
              "pl": "URL",
              "tr": "URL",
              "ar": "الرابط",
              "ru": "URL-адрес",
              "it": "URL",
              "ro": "URL",
              "es": "URL",
              "sq": "URL"
            }
          }
        }
    """
    result = reader.json(filename)
    if not result.found:
        return pd.DataFrame()
    data = result.data

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data["saved_saved_media"]  # pyright: ignore
        for item in items:
            title = eh.fix_latin1_string(item.get("title", ""))
            if "string_list_data" in item:
                string_list = item.get("string_list_data", [{}])
                entry = string_list[0] if string_list else {}
            else:
                entry = _first_present(item.get("string_map_data", {}), _SAVED_ON_LABELS)
            datapoints.append((
                title,
                entry.get("href", ""),
                eh.epoch_to_iso(entry.get("timestamp", ""), errors=errors),
            ))
        out = pd.DataFrame(datapoints, columns=["Title", "URL", "Timestamp"])  # pyright: ignore
        out = _sort_by_date(out, "Timestamp")

    except Exception as e:
        logger.error("Exception caught: %s", e)
        errors[type(e).__name__] += 1

    return out


# ---------------------------------------------------------------------------
# Extractor registry & platform info
# ---------------------------------------------------------------------------

#: Mapping from the string names used in port_config.json to actual extractor functions.
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "followers_to_df": followers_to_df,
    "following_to_df": following_to_df,
    "ads_viewed_to_df": ads_viewed_to_df,
    "posts_viewed_to_df": posts_viewed_to_df,
    "videos_watched_to_df": videos_watched_to_df,
    "post_comments_to_df": post_comments_to_df,
    "liked_comments_to_df": liked_comments_to_df,
    "liked_posts_to_df": liked_posts_to_df,
    "profile_searches_to_df": profile_searches_to_df,
    "story_likes_to_df": story_likes_to_df,
    "threads_viewed_to_df": threads_viewed_to_df,
    "saved_posts_to_df": saved_posts_to_df,
}


# ---------------------------------------------------------------------------
# Main extraction & flow
# ---------------------------------------------------------------------------

def extraction(
    instagram_zip: str,
    validation,
) -> ExtractionResult:
    """Extract data from an Instagram DDP zip and return consent-form tables.

    Parameters
    ----------
    instagram_zip:
        Path to the Instagram DDP zip archive on disk.
    validation:
        Validation result object whose ``archive_members`` attribute is passed
        to ``ZipArchiveReader``.
    """
    config = load_port_config(EXTRACTOR_REGISTRY, "instagram")
    errors: Counter = Counter()
    reader = ZipArchiveReader(instagram_zip, validation.archive_members, errors)
    return run_extraction(reader, errors, config)


class InstagramFlow(FlowBuilder):
    """Flow implementation for the Instagram data donation study.

    Parameters
    ----------
    session_id:
        Unique identifier for the current participant session.
    """

    def __init__(self, session_id: str):
        super().__init__(session_id, "Instagram")

    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)

    def extract_data(self, file_value, validation):
        return extraction(file_value, validation)


def process(session_id):
    flow = InstagramFlow(session_id)
    return flow.start_flow()
