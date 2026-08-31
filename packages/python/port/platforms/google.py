"""
Google

This module provides the flow of a data donation study on the Google Takeout archive.
YouTube is one source within that archive; other sources are added by extending
``TAKEOUT_PATHS`` and registering an extractor per table.

Assumptions:
It handles DDPs in the English, Dutch, German, Spanish, Arabic, Turkish and Chinese language.
Takeout asks for the export format per source, so the watch and search histories are
read as JSON or as HTML depending on what the archive holds; subscriptions and comments
are always CSV. The archive is recognized and its locale determined by ``validate_ddp``
in this module, not by the shared filename matching of ``validate.validate_zip``. A
Takeout export may arrive as several zip parts rather than one; recognition and
extraction both run against the union member inventory of an ``ArchiveSet`` (ADR-0013,
ADR-0040), so a source is found regardless of which part it landed in.

Ported from the algosoc-2026 study fork's google.py (Erik van Haeringen; fork tip
1ca6b1a), re-implemented on the archive-set pipeline. The path table's locale
provenance comments are his.

Configuration
-------------
The ``extraction`` function is driven by ``port_config.json``.  Generate one with::

    pnpm generate-config google

Each extractor function carries its own table config in a ``Table config::``
JSON block inside its docstring.  The generator reads those blocks and
assembles the JSON file.

Platform info::

    {
        "name": "Google",
        "filetypes": ["json", "html", "csv", "txt"],
        "languages": ["en", "nl", "de", "es", "ar", "tr", "zh"],
        "description": "Handles the Google Takeout archive, uploaded as one or more zip parts (a multi-zip archive-set, so the export never needs to fit in a single upload). Extracts 12 tables: four from YouTube (watch history, search history, subscriptions, comments), one each from Search, Chrome, Video Search, Ads, Discover and Google News, and two from News — the News product's My Activity stream and its own export (followed sources, topics and locations, saved articles and magazines). Each source is read as JSON, HTML, CSV or TXT, whichever format it was exported in, which may differ per source within one archive. Handles DDPs in English, Dutch, German, Spanish, Arabic, Turkish and Chinese (Simplified only); the seven locales' path tables for the core sources were verified against real Google Takeout exports in August 2026. A handful of lower-traffic sources still carry older, unverified path guesses: the News My-Activity folder in every locale, Discover outside English (its English path is real-export-verified), and subscriptions/comments outside English and Dutch. Tested against the project's synthetic/canary test suite as of the date below; a live browser run against real exports updates this note when performed. If you find anything wrong with this script, report to datadonation@uu.nl and it will be fixed!",
        "time_last_tested": "30-08-2026"
    }
"""
import json
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, IO, Literal, TypeGuard, overload

import pandas as pd
from dateutil import parser
from lxml import etree

from port.api.d3i_props import ExtractionResult
from port.helpers.archive_set import ArchiveSet
from port.helpers.extraction_helpers import ZipArchiveReader
from port.helpers.flow_builder import FlowBuilder
from port.helpers.table_extractor import load_port_config, run_extraction
from port.helpers.validate import BaseValidation

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Localized archive paths
# ---------------------------------------------------------------------------

#: Locations of the DDP files per locale, keyed by ``source.role``, without extension.
#:
#: Google Takeout translates both folder and file names to the main language of the
#: account, and one archive holds many sources whose filenames collide across folders
#: (every ``My Activity`` product exports a ``MyActivity`` file). Entries are therefore
#: paths, not filenames, and are matched against the end of the archive member paths:
#: ``Verlauf/Wiedergabeverlauf`` resolves ``Takeout/YouTube und YouTube Music/Verlauf/
#: Wiedergabeverlauf.html`` and cannot be confused with a file of the same name in
#: another folder. Only as many trailing segments as are needed to be unambiguous.
#:
#: Each entry lists one or more variants, tried in order. Variants absorb uncertainty:
#: put the exact path first and a shorter, more forgiving one after it. Never fall back
#: to a bare filename that occurs in more than one folder of the archive — that lookup
#: is ambiguous and resolves to nothing.
#:
#: Adding a locale is one block; nothing outside this file needs to change.
#:
#: Two eras of evidence sit in this table. The English and Dutch blocks come from a
#: paired real-export pair (uu-acct / uu-acct-nl, verified 2026-08-26); the Spanish,
#: Arabic, Chinese, Turkish and German blocks were rebuilt from five fresh same-account
#: exports (es-ES, ar-EG, zh-CN, tr-TR, de-DE, verified 2026-08-27) — see the report
#: for data-donation-task's Google-Takeout task 2. The current-era exports showed
#: substantial drift from the older AlgoSoc-study archive this table originally ported
#: from (Erik van Haeringen, algosoc-2026 fork): Takeout translates the activity
#: FILENAME per locale (not just folders), and several folder names, casings and
#: hyphenation choices had simply changed since that archive was taken. Rather than
#: discard the older spellings, every corrected entry keeps its older-era variant as a
#: trailing fallback — a participant exports fresh, but the old spelling costs nothing
#: to keep trying. Ordering within a list is always current-verified-first.
#:
#: Byte-exactness is load-bearing, not stylistic: matching is exact-string, so a
#: diacritic, a casing choice, or an unusual Unicode character in a real folder or
#: filename must be copied out of the export, never retyped. The current Arabic export
#: carries the shadda combining character (U+0651) in ``السجلّ`` and ``سجلّ البحث``
#: (but not in ``سجل المشاهدة`` — the watch-history filename lacks it); the current
#: German export has a non-breaking hyphen (U+2011) in an AI-Mode label not tracked
#: here, which is the same hazard by example. Turkish is case-sensitive for a
#: different reason: its dotted/dotless-i pairs (İ/i, I/ı) make casual case-folding
#: wrong, so ``izleme geçmişi`` (lowercase, current) and ``İzleme geçmişi`` (capital,
#: older-era fallback) are genuinely different strings, not a style choice.
#:
#: Some keys still carry only the older, unverified-against-a-real-export guess.
#: ``discover.history`` is now real-export-verified for en: the gmail-acct export
#: contains ``My Activity/Discover/My Activity.html``, and the integration canaries
#: pin ``discover_history_to_df`` non-empty for that set — it stays an unverified
#: guess for the other six locales. Also unverified: the ``news.history``
#: My-Activity entry in every locale, and ``youtube.subscriptions``/
#: ``youtube.comments`` outside en/nl — both are absent from all five 2026-08-27
#: exports (the study account never generated that activity), so de's
#: subscriptions/comments paths are still the older AlgoSoc-fork guess, not a
#: current-export verification. Chinese covers Simplified only; a Traditional
#: export writes different characters (e.g. 觀看紀錄 instead of 观看记录).
#:
#: The activity FILENAME is locale-dependent, independently of the folder: the current
#: exports show es writing ``MiActividad.html``, ar ``نشاطي.html`` (the same word as its
#: My-Activity folder), zh ``我的活动记录.html``, tr ``Etkinliğim.html`` (again the same
#: word as its folder), de ``MeineAktivitäten.html``. English itself carries two
#: spellings verified in the wild, not an old-vs-current pair: an en-GB export writes
#: the spaced ``My Activity.html`` (verified 2026-08), and a separate English export
#: (gmail-acct) writes the unspaced ``MyActivity.html``. Which of locale variant
#: (en-GB vs. en-US), account, or export rollout causes the split is unresolved —
#: treat both as live spellings worth trying, never one as a stale fallback of the
#: other. Every ``.../MyActivity``-family entry below therefore lists its locale's
#: verified translated filename first, then both untranslated spellings as fallbacks,
#: so any of the three resolves.
#:
#: zh is a partial-translation locale: its top-level folders and activity filename
#: translate, but several My-Activity product SUBfolders stay English in the current
#: export (``Search``, ``Ads``, ``Video Search``, ``Google News``, ``Image Search``) —
#: the opposite pattern from Arabic, which translates its subfolders. Those English
#: zh subfolder names are always paired with the Chinese top folder and the Chinese
#: translated filename in the full path, so they cannot be confused with an English
#: archive (see ``TestLocaleDetection`` in the test module for the check).
#:
#: The five ``news.*`` keys locate the News product's own export — its followed
#: sources, topics and locations, saved articles and magazines — which is a different
#: export than the ``news.history`` activity log above it. Its filenames stay English
#: in every locale (verified in all seven), so every non-English locale also lists the
#: bare English ``News/<file>`` path as a defensive fallback. Its folder is verified
#: current for all seven: English and Dutch (``News``, ``Nieuws``, verified
#: 2026-08-26); Spanish, Arabic, Chinese (``Noticias``, ``الأخبار``, ``新闻``) and,
#: notably, Turkish and German did NOT match their derived activity-folder guess —
#: Turkish is ``Google Haberler`` (not bare ``Haberler``, which is what the My-Activity
#: ``google_news.history`` folder uses) and German is ``Google News`` (not
#: ``Nachrichten``) — all verified 2026-08-27. The older guesses are kept as fallbacks.
TAKEOUT_PATHS: dict[str, dict[str, list[str]]] = {
    "en": {
        "youtube.watch_history": ["YouTube and YouTube Music/history/watch-history", "My Activity/YouTube/MyActivity", "My Activity/YouTube/My Activity"],
        "youtube.search_history": ["YouTube and YouTube Music/history/search-history", "My Activity/YouTube/MyActivity", "My Activity/YouTube/My Activity"],
        "youtube.subscriptions": ["YouTube and YouTube Music/subscriptions/subscriptions"],
        "youtube.comments": ["YouTube and YouTube Music/comments/comments"],
        "search.search_history": ["My Activity/Search/MyActivity", "My Activity/Search/My Activity"],
        "chrome.history": ["Chrome/History", "My Activity/Chrome/MyActivity", "My Activity/Chrome/My Activity"],
        "video_search.history": ["My Activity/Video Search/MyActivity", "My Activity/Video Search/My Activity"],
        "ads.history": ["My Activity/Ads/MyActivity", "My Activity/Ads/My Activity"],
        "discover.history": ["My Activity/Discover/MyActivity", "My Activity/Discover/My Activity"],
        "google_news.history": ["My Activity/Google News/MyActivity", "My Activity/Google News/My Activity"],
        "news.history": ["My Activity/News/MyActivity", "My Activity/News/My Activity"],
        "news.articles": ["News/articles"],
        "news.followed_locations": ["News/followed_locations"],
        "news.followed_sources": ["News/followed_sources"],
        "news.followed_topics": ["News/followed_topics"],
        "news.magazines": ["News/magazines"],
    },
    "nl": {
        "youtube.watch_history": ["YouTube en YouTube Music/geschiedenis/kijkgeschiedenis", "Mijn activiteit/YouTube/MyActivity", "Mijn activiteit/YouTube/My Activity"],
        "youtube.search_history": ["YouTube en YouTube Music/geschiedenis/zoekgeschiedenis", "Mijn activiteit/YouTube/MyActivity", "Mijn activiteit/YouTube/My Activity"],
        "youtube.subscriptions": ["YouTube en YouTube Music/abonnementen/abonnementen"],
        "youtube.comments": ["YouTube en YouTube Music/reacties/reacties"],
        "search.search_history": ["Mijn activiteit/Zoeken/MyActivity", "Mijn activiteit/Zoeken/My Activity"],
        "chrome.history": ["Chrome/Geschiedenis", "Mijn activiteit/Chrome/MyActivity", "Mijn activiteit/Chrome/My Activity"],
        "video_search.history": ["Mijn activiteit/Video_s zoeken/MyActivity", "Mijn activiteit/Video_s zoeken/My Activity"],
        "ads.history": ["Mijn activiteit/Advertenties/MyActivity", "Mijn activiteit/Advertenties/My Activity"],
        "discover.history": ["Mijn activiteit/Discover/MyActivity", "Mijn activiteit/Discover/My Activity"],
        "google_news.history": ["Mijn activiteit/Google Nieuws/MyActivity", "Mijn activiteit/Google Nieuws/My Activity"],
        "news.history": ["Mijn activiteit/Nieuws/MyActivity", "Mijn activiteit/Nieuws/My Activity"],
        "news.articles": ["Nieuws/articles", "News/articles"],
        "news.followed_locations": ["Nieuws/followed_locations", "News/followed_locations"],
        "news.followed_sources": ["Nieuws/followed_sources", "News/followed_sources"],
        "news.followed_topics": ["Nieuws/followed_topics", "News/followed_topics"],
        "news.magazines": ["Nieuws/magazines", "News/magazines"],
    },
    "de": {
        "youtube.watch_history": ["YouTube und YouTube Music/Verlauf/Wiedergabeverlauf", "Meine Aktivitäten/YouTube/MeineAktivitäten", "Meine Aktivitäten/YouTube/MyActivity", "Meine Aktivitäten/YouTube/My Activity"],
        "youtube.search_history": ["YouTube und YouTube Music/Verlauf/Suchverlauf", "Meine Aktivitäten/YouTube/MeineAktivitäten", "Meine Aktivitäten/YouTube/MyActivity", "Meine Aktivitäten/YouTube/My Activity"],
        "youtube.subscriptions": ["YouTube und YouTube Music/Abos/Abos"],
        "youtube.comments": ["YouTube und YouTube Music/Kommentare/Kommentare"],
        "search.search_history": ["Meine Aktivitäten/Google Suche/MeineAktivitäten", "Meine Aktivitäten/Google Suche/MyActivity", "Meine Aktivitäten/Google Suche/My Activity", "Meine Aktivitäten/Suche/MyActivity", "Meine Aktivitäten/Suche/My Activity"],
        "chrome.history": ["Chrome/Verlauf", "Meine Aktivitäten/Chrome/MeineAktivitäten", "Meine Aktivitäten/Chrome/MyActivity", "Meine Aktivitäten/Chrome/My Activity"],
        "video_search.history": ["Meine Aktivitäten/Videosuche/MeineAktivitäten", "Meine Aktivitäten/Videosuche/MyActivity", "Meine Aktivitäten/Videosuche/My Activity", "Meine Aktivitäten/Videosuchen/MyActivity", "Meine Aktivitäten/Videosuchen/My Activity"],
        "ads.history": ["Meine Aktivitäten/Anzeigen/MeineAktivitäten", "Meine Aktivitäten/Anzeigen/MyActivity", "Meine Aktivitäten/Anzeigen/My Activity"],
        "discover.history": ["Meine Aktivitäten/Entdecken/MeineAktivitäten", "Meine Aktivitäten/Entdecken/MyActivity", "Meine Aktivitäten/Entdecken/My Activity"],
        "google_news.history": ["Meine Aktivitäten/Google News/MeineAktivitäten", "Meine Aktivitäten/Google News/MyActivity", "Meine Aktivitäten/Google News/My Activity"],
        "news.history": ["Meine Aktivitäten/Nachrichten/MeineAktivitäten", "Meine Aktivitäten/Nachrichten/MyActivity", "Meine Aktivitäten/Nachrichten/My Activity"],
        "news.articles": ["Google News/articles", "Nachrichten/articles", "News/articles"],
        "news.followed_locations": ["Google News/followed_locations", "Nachrichten/followed_locations", "News/followed_locations"],
        "news.followed_sources": ["Google News/followed_sources", "Nachrichten/followed_sources", "News/followed_sources"],
        "news.followed_topics": ["Google News/followed_topics", "Nachrichten/followed_topics", "News/followed_topics"],
        "news.magazines": ["Google News/magazines", "Nachrichten/magazines", "News/magazines"],
    },
    "es": {
        "youtube.watch_history": ["YouTube y YouTube Music/historial/historial-de-reproducciones", "YouTube y YouTube Music/historial/historial de reproducciones", "Mi actividad/YouTube/MiActividad", "Mi actividad/YouTube/MyActivity", "Mi actividad/YouTube/My Activity"],
        "youtube.search_history": ["YouTube y YouTube Music/historial/historial-de-búsqueda", "YouTube y YouTube Music/historial/historial de búsquedas", "Mi actividad/YouTube/MiActividad", "Mi actividad/YouTube/MyActivity", "Mi actividad/YouTube/My Activity"],
        "youtube.subscriptions": ["YouTube y YouTube Music/suscripciones/suscripciones"],
        "youtube.comments": ["YouTube y YouTube Music/comentarios/comentarios"],
        "search.search_history": ["Mi actividad/Búsqueda/MiActividad", "Mi actividad/Búsqueda/MyActivity", "Mi actividad/Búsqueda/My Activity"],
        "chrome.history": ["Chrome/Historial", "Mi actividad/Chrome/MiActividad", "Mi actividad/Chrome/MyActivity", "Mi actividad/Chrome/My Activity"],
        "video_search.history": [
            "Mi actividad/Búsqueda de vídeos/MiActividad", "Mi actividad/Búsqueda de vídeos/MyActivity", "Mi actividad/Búsqueda de vídeos/My Activity", "Mi actividad/Búsqueda de videos/MyActivity", "Mi actividad/Búsqueda de videos/My Activity",
            "Mi actividad/Búsqueda de videos/MiActividad",  # fork 70f34dc, report-derived; trailing fallback — our 2026-08-27 export verifies Búsqueda de vídeos/MiActividad as current
        ],
        "ads.history": ["Mi actividad/Publicidad/MiActividad", "Mi actividad/Publicidad/MyActivity", "Mi actividad/Publicidad/My Activity", "Mi actividad/Anuncios/MyActivity", "Mi actividad/Anuncios/My Activity"],
        "discover.history": [
            "Mi actividad/Descubrir/MiActividad", "Mi actividad/Descubrir/MyActivity", "Mi actividad/Descubrir/My Activity",
            "Mi actividad/Discover/MiActividad",  # fork 70f34dc, report-derived; trailing fallback — our 2026-08-27 export verifies Descubrir/MiActividad as current
        ],
        "google_news.history": [
            "Mi actividad/Google News/MiActividad", "Mi actividad/Google News/MyActivity", "Mi actividad/Google News/My Activity", "Mi actividad/Google Noticias/MyActivity", "Mi actividad/Google Noticias/My Activity",
            "Mi actividad/Google Noticias/MiActividad",  # fork 70f34dc, report-derived; trailing fallback — our 2026-08-27 export verifies Google News/MiActividad as current
        ],
        "news.history": ["Mi actividad/Noticias/MiActividad", "Mi actividad/Noticias/MyActivity", "Mi actividad/Noticias/My Activity"],
        "news.articles": ["Noticias/articles", "News/articles"],
        "news.followed_locations": ["Noticias/followed_locations", "News/followed_locations"],
        "news.followed_sources": ["Noticias/followed_sources", "News/followed_sources"],
        "news.followed_topics": ["Noticias/followed_topics", "News/followed_topics"],
        "news.magazines": ["Noticias/magazines", "News/magazines"],
    },
    "ar": {
        "youtube.watch_history": ["YouTube وYouTube Music/السجلّ/سجل المشاهدة", "YouTube و YouTube Music/سجل/سجل المشاهدة", "نشاطي/YouTube/نشاطي", "نشاطي/YouTube/MyActivity", "نشاطي/YouTube/My Activity", "أنشطتي/YouTube/MyActivity", "أنشطتي/YouTube/My Activity"],
        "youtube.search_history": ["YouTube وYouTube Music/السجلّ/سجلّ البحث", "YouTube و YouTube Music/سجل/سجل البحث", "نشاطي/YouTube/نشاطي", "نشاطي/YouTube/MyActivity", "نشاطي/YouTube/My Activity", "أنشطتي/YouTube/MyActivity", "أنشطتي/YouTube/My Activity"],
        "youtube.subscriptions": ["YouTube وYouTube Music/اشتراكات/اشتراكات", "YouTube و YouTube Music/اشتراكات/اشتراكات"],
        "youtube.comments": ["YouTube وYouTube Music/تعليقات/تعليقات", "YouTube و YouTube Music/تعليقات/تعليقات"],
        "search.search_history": ["نشاطي/البحث/نشاطي", "نشاطي/البحث/MyActivity", "نشاطي/البحث/My Activity", "أنشطتي/بحث/MyActivity", "أنشطتي/بحث/My Activity"],
        "chrome.history": ["Chrome/السجلّ", "Chrome/السجل", "نشاطي/Chrome/نشاطي", "نشاطي/Chrome/MyActivity", "نشاطي/Chrome/My Activity", "أنشطتي/Chrome/MyActivity", "أنشطتي/Chrome/My Activity"],
        "video_search.history": ["نشاطي/بحث الفيديو/نشاطي", "نشاطي/بحث الفيديو/MyActivity", "نشاطي/بحث الفيديو/My Activity", "أنشطتي/البحث عن الفيديو/MyActivity", "أنشطتي/البحث عن الفيديو/My Activity"],
        "ads.history": ["نشاطي/الإعلانات/نشاطي", "نشاطي/الإعلانات/MyActivity", "نشاطي/الإعلانات/My Activity", "أنشطتي/الإعلانات/MyActivity", "أنشطتي/الإعلانات/My Activity"],
        "discover.history": ["نشاطي/اكتشف/نشاطي", "نشاطي/اكتشف/MyActivity", "نشاطي/اكتشف/My Activity", "أنشطتي/اكتشف/MyActivity", "أنشطتي/اكتشف/My Activity"],
        "google_news.history": ["نشاطي/أخبار Google/نشاطي", "نشاطي/أخبار Google/MyActivity", "نشاطي/أخبار Google/My Activity", "أنشطتي/أخبار جوجل/MyActivity", "أنشطتي/أخبار جوجل/My Activity"],
        "news.history": ["نشاطي/الأخبار/نشاطي", "نشاطي/الأخبار/MyActivity", "نشاطي/الأخبار/My Activity", "أنشطتي/الأخبار/MyActivity", "أنشطتي/الأخبار/My Activity"],
        "news.articles": ["الأخبار/articles", "News/articles"],
        "news.followed_locations": ["الأخبار/followed_locations", "News/followed_locations"],
        "news.followed_sources": ["الأخبار/followed_sources", "News/followed_sources"],
        "news.followed_topics": ["الأخبار/followed_topics", "News/followed_topics"],
        "news.magazines": ["الأخبار/magazines", "News/magazines"],
    },
    "tr": {
        "youtube.watch_history": ["YouTube ve YouTube Music/geçmiş/izleme geçmişi", "YouTube ve YouTube Music/geçmiş/İzleme geçmişi", "Etkinliğim/YouTube/Etkinliğim", "Etkinliğim/YouTube/MyActivity", "Etkinliğim/YouTube/My Activity"],
        "youtube.search_history": ["YouTube ve YouTube Music/geçmiş/arama geçmişi", "YouTube ve YouTube Music/geçmiş/Arama geçmişi", "Etkinliğim/YouTube/Etkinliğim", "Etkinliğim/YouTube/MyActivity", "Etkinliğim/YouTube/My Activity"],
        "youtube.subscriptions": ["YouTube ve YouTube Music/Abonelikler/Abonelikler"],
        "youtube.comments": ["YouTube ve YouTube Music/Yorumlar/Yorumlar"],
        "search.search_history": ["Etkinliğim/Arama/Etkinliğim", "Etkinliğim/Arama/MyActivity", "Etkinliğim/Arama/My Activity"],
        "chrome.history": ["Chrome/Tarih", "Chrome/Geçmiş", "Etkinliğim/Chrome/Etkinliğim", "Etkinliğim/Chrome/MyActivity", "Etkinliğim/Chrome/My Activity"],
        "video_search.history": ["Etkinliğim/Video Arama/Etkinliğim", "Etkinliğim/Video Arama/MyActivity", "Etkinliğim/Video Arama/My Activity"],
        "ads.history": ["Etkinliğim/Reklamlar/Etkinliğim", "Etkinliğim/Reklamlar/MyActivity", "Etkinliğim/Reklamlar/My Activity"],
        "discover.history": ["Etkinliğim/Keşfet/Etkinliğim", "Etkinliğim/Keşfet/MyActivity", "Etkinliğim/Keşfet/My Activity"],
        "google_news.history": ["Etkinliğim/Google Haberler/Etkinliğim", "Etkinliğim/Google Haberler/MyActivity", "Etkinliğim/Google Haberler/My Activity"],
        "news.history": ["Etkinliğim/Haberler/Etkinliğim", "Etkinliğim/Haberler/MyActivity", "Etkinliğim/Haberler/My Activity"],
        "news.articles": ["Google Haberler/articles", "Haberler/articles", "News/articles"],
        "news.followed_locations": ["Google Haberler/followed_locations", "Haberler/followed_locations", "News/followed_locations"],
        "news.followed_sources": ["Google Haberler/followed_sources", "Haberler/followed_sources", "News/followed_sources"],
        "news.followed_topics": ["Google Haberler/followed_topics", "Haberler/followed_topics", "News/followed_topics"],
        "news.magazines": ["Google Haberler/magazines", "Haberler/magazines", "News/magazines"],
    },
    "zh": {
        "youtube.watch_history": ["YouTube 和 YouTube Music/历史记录/观看记录", "YouTube 和 YouTube Music/记录/观看记录", "我的活动/YouTube/我的活动记录", "我的活动/YouTube/MyActivity", "我的活动/YouTube/My Activity"],
        "youtube.search_history": ["YouTube 和 YouTube Music/历史记录/搜索记录", "YouTube 和 YouTube Music/记录/搜索记录", "我的活动/YouTube/我的活动记录", "我的活动/YouTube/MyActivity", "我的活动/YouTube/My Activity"],
        "youtube.subscriptions": ["YouTube 和 YouTube Music/订阅内容/订阅内容"],
        "youtube.comments": ["YouTube 和 YouTube Music/评论/评论"],
        "search.search_history": ["我的活动/Search/我的活动记录", "我的活动/Search/MyActivity", "我的活动/Search/My Activity", "我的活动/搜索/MyActivity", "我的活动/搜索/My Activity"],
        "chrome.history": ["Chrome/历史记录", "我的活动/Chrome/我的活动记录", "我的活动/Chrome/MyActivity", "我的活动/Chrome/My Activity"],
        "video_search.history": ["我的活动/Video Search/我的活动记录", "我的活动/Video Search/MyActivity", "我的活动/Video Search/My Activity", "我的活动/视频搜索/MyActivity", "我的活动/视频搜索/My Activity"],
        "ads.history": ["我的活动/Ads/我的活动记录", "我的活动/Ads/MyActivity", "我的活动/Ads/My Activity", "我的活动/广告/MyActivity", "我的活动/广告/My Activity"],
        "discover.history": ["我的活动/发现/我的活动记录", "我的活动/发现/MyActivity", "我的活动/发现/My Activity"],
        "google_news.history": ["我的活动/Google News/我的活动记录", "我的活动/Google News/MyActivity", "我的活动/Google News/My Activity", "我的活动/Google 新闻/MyActivity", "我的活动/Google 新闻/My Activity"],
        "news.history": ["我的活动/新闻/我的活动记录", "我的活动/新闻/MyActivity", "我的活动/新闻/My Activity"],
        "news.articles": ["新闻/articles", "News/articles"],
        "news.followed_locations": ["新闻/followed_locations", "News/followed_locations"],
        "news.followed_sources": ["新闻/followed_sources", "News/followed_sources"],
        "news.followed_topics": ["新闻/followed_topics", "News/followed_topics"],
        "news.magazines": ["新闻/magazines", "News/magazines"],
    },
}

#: File formats each source can be exported in, tried in this order. Takeout asks for
#: the format per source, so one archive can hold the watch history as JSON and the
#: Chrome history as HTML — the format belongs to the file that is there, not to the DDP.
#: The News product's own files are always plain text, unlike the activity-based sources.
KEY_FORMATS: dict[str, list[str]] = {
    "youtube.watch_history": ["json", "html"],
    "youtube.search_history": ["json", "html"],
    "youtube.subscriptions": ["csv"],
    "youtube.comments": ["csv"],
    "search.search_history": ["json", "html"],
    "chrome.history": ["json", "html"],
    "video_search.history": ["json", "html"],
    "ads.history": ["json", "html"],
    "discover.history": ["json", "html"],
    "google_news.history": ["json", "html"],
    "news.history": ["json", "html"],
    "news.articles": ["txt"],
    "news.followed_locations": ["txt"],
    "news.followed_sources": ["txt"],
    "news.followed_topics": ["txt"],
    "news.magazines": ["txt"],
}


@dataclass
class GoogleValidation(BaseValidation):
    """What validating a Google Takeout archive established: whether it was recognized
    (status code 0) or not (1), which locale the DDP is in, and the member paths
    extraction reads from. This platform defines no ``DDP_CATEGORIES``: a category
    pairs one file format with a set of filenames, and a Takeout archive has neither —
    see ``validate_ddp``.

    ``ddp_locale`` is the language Takeout wrote this archive's folders and filenames
    in, detected by ``validate_ddp`` — never conflate it with the participant's UI
    locale, which is a separate, independently-chosen setting."""

    archive_members: list[str] = field(default_factory=list)
    ddp_locale: str = ""


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _path_suffixes(archive_members: list[str]) -> set[str]:
    """Returns every trailing path fragment of the archive members, without extension.

    Mirrors how ``ZipArchiveReader.resolve_member`` matches a lookup against a member:
    on a folder boundary, from the right. ``a/b/c.json`` yields ``a/b/c``, ``b/c`` and
    ``c``, so a path from ``TAKEOUT_PATHS`` is present exactly when it is in this set."""

    suffixes = set()
    for member in archive_members:
        segments = member.rsplit(".", 1)[0].split("/")
        for start in range(len(segments)):
            suffixes.add("/".join(segments[start:]))
    return suffixes


def _detect_locale(archive_members: list[str]) -> tuple[str, int]:
    """Returns the locale whose paths best cover the archive, and how many of its
    sources were found.

    Folder-qualified paths decide, and the number of sources found only breaks ties:
    the filename-only variants exist to be forgiving about folders, so letting them
    weigh in equally would drown out the folder evidence. A locale that translates only
    its folder names — leaving every filename in English — is recognized on folders
    alone this way."""

    suffixes = _path_suffixes(archive_members)
    scores = {}
    for locale, keys in TAKEOUT_PATHS.items():
        found = [
            any(path in suffixes for path in paths)
            for paths in keys.values()
        ]
        folders = [
            any(path in suffixes for path in paths if "/" in path)
            for paths in keys.values()
        ]
        scores[locale] = (sum(folders), sum(found))
    best = max(scores, key=lambda locale: scores[locale])

    return best, scores[best][1]


def validate_ddp(archive_set: ArchiveSet) -> GoogleValidation:
    """Recognizes a Google Takeout archive-set and determines its DDP locale.

    Replaces the shared ``validate.validate_zip`` for this platform, which matches bare
    filenames against a DDP category. That does not fit a Takeout archive: it holds many
    sources whose filenames collide across folders, its export format is chosen per
    source rather than for the DDP as a whole, and a locale may translate only its
    folder names. Recognition runs on the member paths instead, which answers all three.

    An archive counts as recognized as soon as one known source is found — the
    participant chooses which sources to export, so any subset is a legitimate DDP.

    Recognition runs on the union member inventory of the whole set — a source may live
    in any part, so a Takeout export split across several zip parts is recognized the
    same way as one uploaded whole. Corrupt parts never reach here: ``ArchiveSet``
    construction raises ``zipfile.BadZipFile`` and the flow routes that to retry
    (ADR-0040 / ``FlowBuilder``)."""

    archive_members = list(archive_set.members)
    ddp_locale, sources_found = _detect_locale(archive_members)
    logger.info("Detected DDP locale: %s (%d sources found)", ddp_locale, sources_found)

    return GoogleValidation(
        status_code=0 if sources_found else 1,
        archive_members=archive_members,
        ddp_locale=ddp_locale,
    )


def _parse_activity_html(data: IO[bytes]) -> list[dict]:
    """Reads an activity file in html format and parses it into a list of dictionaries with
    the same shape as the json format: the title of the activity, the url it points to, what
    stands under it and its timestamp.

    Every activity file of the DDP — the YouTube histories as well as the My Activity file
    of any product — is one page of ``outer-cell`` blocks, in which the activity itself
    sits in the ``content-cell`` of body text::

        <div class="content-cell ... mdl-typography--body-1">Watched
            <a href="https://www.youtube.com/watch?v=abc">A video</a><br>
            <a href="https://www.youtube.com/channel/UC1">A channel</a><br>
            15 jun 2026, 20:30:41 CEST
        </div>

    A line between the activity and the timestamp is read by whether it links out. One that
    does is a subtitle — the channel of the video above, which the json writes as
    ``{"name": ..., "url": ...}`` — and one that does not is the ``description`` of the
    activity, such as the "Watched at 11:39 AM" an ad is recorded with. A record carries
    either, both, or neither.

    Selecting those cells by class is what makes one parser enough for every source: the
    activity is read the same way regardless of which product wrote the file, and callers
    select the records that are theirs by url, exactly as they do for the json format.

    The caption cell beside it carries the lists some sources record with an activity —
    the locations and details of a Discover card, say — which ``_parse_activity_caption``
    reads onto the same record.

    The document is walked as a stream and each cell is dropped once it is read, so the
    *parse tree* stays proportional to one activity rather than to the size of the file —
    a watch history of a heavy user runs to hundreds of megabytes. That bound covers only
    the input side: the ``records`` list this function returns holds one dict per activity
    and grows with the file, so overall memory still scales with row count once parsing is
    done — see ADR-0034 for the measured, unbounded downstream cost.

    ``data`` is any binary file-like ``etree.iterparse`` can stream from, not necessarily a
    seekable one: production hands it the decompression stream ``ArchiveSet.open_member``
    yields (ADR-0040), never a fully materialized member, so this parser's own streaming
    read bounds how much of the *zip member* is resident at once — it says nothing about
    the size of the parsed records it accumulates and returns.

    The activity HTML declares no charset in its head (verified against real exports),
    and lxml's HTML parser defaults to latin-1 when it finds none — silently double-
    decoding every non-ASCII byte. Takeout's export bytes are UTF-8 (empirical), so the
    encoding is pinned explicitly here rather than left to that default."""

    records = []
    for _, cell in etree.iterparse(data, html=True, tag="div", events=("end",), encoding="utf-8"):
        classes = cell.get("class") or ""
        texts = (
            [text.strip() for text in cell.itertext() if text.strip()]
            if "mdl-typography--body-1" in classes
            else []
        )
        # An activity always carries text, ending in its timestamp. A body cell without
        # any is the empty one the layout puts beside it, right-aligned, and not a record.
        if texts:
            # The activity is the text up to the first line break and the timestamp is the
            # line the cell closes with. What stands in between is told apart by its link:
            # a line that links out is a subtitle, the channel of a video say, and one that
            # does not is the description of the activity.
            lines = _parse_activity_lines(cell)
            middle = lines[1:-1]

            record = {
                "title": lines[0]["text"],
                "titleUrl": _strip_redirect(lines[0]["url"]) if lines[0]["url"] else "",
                "time": _convert_to_iso8601(texts[-1]),
            }
            subtitles = [_subtitle(line) for line in middle if line["url"]]
            if subtitles:
                record["subtitles"] = subtitles
            description = " ".join(line["text"] for line in middle if not line["url"])
            if description:
                record["description"] = description
            records.append(record)
        elif records and "mdl-typography--caption" in classes:
            # The caption follows the activity it belongs to, so it lands on the record
            # that was just read.
            records[-1].update(_parse_activity_caption(cell))

        # Drop every div once it ends — the record it held has been read, and its children
        # ended before it did — so the tree does not grow with the file.
        cell.clear()
        while cell.getprevious() is not None:
            del cell.getparent()[0]

    return records


#: The link that gives away a location, whatever language its section is headed in.
MAPS_LINK = "google.com/maps"


def _close_line(line: dict, section: list) -> dict:
    """Adds the line to the section if it holds anything, and starts an empty one."""

    text = " ".join(part.strip() for part in line["texts"] if part.strip())
    if text or line["url"]:
        section.append({"text": text, "url": line["url"]})
    return {"texts": [], "url": ""}


def _parse_activity_lines(cell) -> list[dict]:
    """Reads a body cell as the lines its line breaks separate, each with the first url it
    links to. A line that holds neither text nor a link is left out, so the break the cell
    tends to close with does not add an empty one."""

    lines: list[dict] = []
    line = {"texts": [cell.text or ""], "url": ""}
    for child in cell:
        if child.tag == "br":
            line = _close_line(line, lines)
        else:
            if child.tag == "a" and not line["url"]:
                line["url"] = child.get("href") or ""
            line["texts"].append("".join(child.itertext()))
        line["texts"].append(child.tail or "")
    _close_line(line, lines)

    return lines


def _subtitle(line: dict) -> dict:
    """Reads a line linking out from under an activity in the shape the json format writes
    it: the name it shows and where it points."""

    return {"name": line["text"], "url": _strip_redirect(line["url"])}


def _parse_activity_caption(cell) -> dict:
    """Reads the lists an activity carries beside it, in the shape the json format writes
    them: ``details`` as ``{"name": ...}`` and ``locationInfos`` as ``{"name": ..., "url":
    ..., "source": ...}``. Returns only the lists that are there, as the json does.

    The caption is a run of sections, each headed by a bold label and holding one entry per
    line break::

        <b>Locations:</b><br> At <a href="...maps...">this general area</a> - Based on your
        past activity<br><b>Details:</b><br> Armed forces<br> Business - viewed<br>

    Which section is which is read from where it sits and what it holds, not from the
    labels, which are written in the language of the account: the caption opens with the
    products and closes with why the activity was kept, and in between a section of
    locations links to Maps. What is left is the details."""

    sections: list[list[dict]] = [[]]
    line = {"texts": [cell.text or ""], "url": ""}
    for child in cell:
        if child.tag == "b":
            # The label of a section, which the section it opens is recognized without.
            line = _close_line(line, sections[-1])
            sections.append([])
        elif child.tag == "br":
            line = _close_line(line, sections[-1])
        else:
            if child.tag == "a" and not line["url"]:
                line["url"] = child.get("href") or ""
            line["texts"].append(child.text or "")
        line["texts"].append(child.tail or "")
    _close_line(line, sections[-1])

    caption = {}
    filled = [section for section in sections if section]
    for position, section in enumerate(filled):
        if position == 0 or position == len(filled) - 1:
            # A caption opens with the products the activity belongs to and closes with why
            # it was kept, neither of which says anything about the activity itself.
            continue
        if any(MAPS_LINK in line["url"] for line in section):
            caption["locationInfos"] = [_location(line) for line in section]
        else:
            caption["details"] = [{"name": line["text"]} for line in section if line["text"]]

    return caption


def _location(line: dict) -> dict:
    """Reads a location entry, whose source of the location follows its name."""

    name, separator, source = line["text"].rpartition(" - ")
    if not separator:
        name, source = line["text"], ""

    location = {"name": name, "url": line["url"]}
    if source:
        location["source"] = source
    return location


def _strip_redirect(url: str) -> str:
    """Returns the destination of a Google redirect url, other urls unchanged. Activities
    that leave Google, such as a visit from the Chrome history, are recorded as one."""

    prefix = "https://www.google.com/url?q="
    return url[len(prefix):] if url.startswith(prefix) else url


def _first_subtitle(item: dict) -> dict:
    """Returns the subtitle an activity stands under, empty when it carries none. A record
    may hold a list of them, but the sources read here name a single one — the channel of a
    video — and only where the account still has it."""

    subtitles = item.get("subtitles") or []
    return next((subtitle for subtitle in subtitles if isinstance(subtitle, dict)), {})


def _join_details(item: dict) -> str:
    """Reads the details an activity carries as one column of text, empty when it carries
    none. Most records have nothing here; the ones that do say how the activity came about,
    such as a video that was watched from an ad.

    A detail that points somewhere is written as its name and that url behind a colon. The
    json format keeps the two apart, in a ``name`` and a ``url``, where the html writes them
    as the one line ``Tried to open in app: https://...`` — so joining them here is what
    makes both formats produce the same column."""

    texts = []
    for detail in item.get("details") or []:
        if not isinstance(detail, dict):
            continue
        texts.append(": ".join(
            part for part in (detail.get("name", ""), detail.get("url", "")) if part
        ))

    return ", ".join(texts)


def _join_locations(item: dict) -> str:
    """Reads the locations an activity was recorded from as one column of text, empty when
    it carries none. Each is the area it names and the link to Maps that shows it, followed
    by how it was arrived at behind a dash, the way the archive writes it. An activity may
    be placed by several of them at once."""

    texts = []
    for location in item.get("locationInfos") or []:
        if not isinstance(location, dict):
            continue
        area = " ".join(
            part for part in (location.get("name", ""), location.get("url", "")) if part
        )
        source = location.get("source", "")
        texts.append(f"{area} - {source}" if source else area)

    return ", ".join(texts)


#: Months as Takeout abbreviates them in the languages it writes in Latin script, by the
#: first three letters, lowercased. Dates in another script are left to ``dateutil``.
MONTHS = {
    "jan": 1, "oca": 1, "ene": 1,
    "feb": 2, "şub": 2, "sub": 2,
    "mar": 3, "mrt": 3, "mär": 3, "mrz": 3,
    "apr": 4, "nis": 4, "abr": 4,
    "may": 5, "mei": 5, "mai": 5,
    "jun": 6, "haz": 6,
    "jul": 7, "tem": 7,
    "aug": 8, "ağu": 8, "agu": 8, "ago": 8,
    "sep": 9, "eyl": 9, "set": 9,
    "oct": 10, "okt": 10, "eki": 10,
    "nov": 11, "kas": 11,
    "dec": 12, "dez": 12, "ara": 12, "dic": 12,
}

#: ``15 jun 2026, 20:30:41 CEST`` — how most locales write an activity timestamp, some of
#: them with an ordinal dot after the day and after the month, as ``17. Aug. 2026`` is.
DAY_FIRST = re.compile(r"^(\d{1,2})\.? ([^\s,]+),? (\d{4}),? (\d{1,2}):(\d{2}):(\d{2})")

#: ``Aug 17, 2026, 1:14:48 PM CEST`` — how the English locale writes one.
MONTH_FIRST = re.compile(r"^([^\s,\d]+) (\d{1,2}), (\d{4}), (\d{1,2}):(\d{2}):(\d{2})(?:\s*([AaPp])\.?[Mm])?")

#: ``27.08.2026, 20:04:54 MESZ`` — a fully numeric dotted date, as the current German
#: export writes one. Dotted numeric dates are day-first in every locale that writes
#: them, so this is unambiguous by construction — unlike ``dateutil``'s own month-first
#: default, which silently swaps day and month whenever the day is <= 12 (12.07 read as
#: 2026-12-07 instead of 2026-07-12). Tried before ``dateutil`` for exactly that reason;
#: a month > 12 cannot be this shape at all, so that case (and any other ValueError, e.g.
#: an out-of-range day) falls through to the existing paths below unchanged.
NUMERIC_DAY_FIRST = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4}),? (\d{1,2}):(\d{2}):(\d{2})")

#: ``2026年7月30日 00:23:06 CEST`` — how the Chinese export writes a timestamp. The
#: 年 (year), 月 (month) and 日 (day) unit markers name each field, so this is
#: unambiguous by construction, the same reasoning as ``NUMERIC_DAY_FIRST`` — and
#: unlike either of those, there is no digit-only shape here for ``dateutil`` to
#: even attempt, so without this it falls through unread (BUG C: confirmed against
#: a real zh export, tests/ddp/google_set_uu-acct-zh/'s 观看记录.html — the zh
#: locale writes English action words, e.g. "Watched", but CJK-formatted dates).
CJK_DATE = re.compile(r"^(\d{4})年(\d{1,2})月(\d{1,2})日,? (\d{1,2}):(\d{2}):(\d{2})")

#: ``23‏/07‏/2026، 4:20:22 م CEST`` — how the Arabic export writes a timestamp:
#: Western digits in day/month/year order (day-first — confirmed against real
#: samples with a day > 12, so unambiguous by construction), each numeric field
#: followed by a U+200F RIGHT-TO-LEFT MARK, a U+060C ARABIC COMMA after the year,
#: and a 12-hour clock using the Arabic meridiem letters (ص "morning" = AM, م
#: "evening" = PM) instead of AM/PM. Confirmed against a real ar export,
#: tests/ddp/google_set_uu-acct-ar/'s activity HTML. The RTL mark is optional in
#: the pattern (harmless if a future export ever omits it); the meridiem letter
#: is not, since the hour alone is ambiguous without it.
ARABIC_DATE = re.compile(
    r"^(\d{1,2})‏?/(\d{1,2})‏?/(\d{4})، (\d{1,2}):(\d{2}):(\d{2}) ([صم])"
)


def _convert_to_iso8601(timestamp):
    """Converts a time string extracted from the HTML DDP (e.g. 15 jun 2026, 20:30:41 CEST) to
    ISO8601 format, ignoring timezone abbreviations and translating month abbreviations.

    An activity file holds one timestamp per record, hundreds of thousands of them for a
    heavy user, and reading a date in any format a participant might have is expensive.
    The formats Takeout actually writes are read directly here, which is some twenty
    times faster; anything else — another script, another separator — falls through to
    ``dateutil``, which reads what it can and leaves the rest as it found it."""

    numeric = NUMERIC_DAY_FIRST.match(timestamp)
    if numeric:
        day, month, year, hour, minute, second = numeric.groups()
        if 1 <= int(month) <= 12:
            try:
                return datetime(
                    int(year), int(month), int(day), int(hour), int(minute), int(second)
                ).isoformat()
            except ValueError:
                pass  # e.g. day out of range for the month — fall through below

    cjk = CJK_DATE.match(timestamp)
    if cjk:
        year, month, day, hour, minute, second = cjk.groups()
        if 1 <= int(month) <= 12:
            try:
                return datetime(
                    int(year), int(month), int(day), int(hour), int(minute), int(second)
                ).isoformat()
            except ValueError:
                pass  # e.g. day out of range for the month — fall through below

    arabic = ARABIC_DATE.match(timestamp)
    if arabic:
        day, month, year, hour, minute, second, meridiem = arabic.groups()
        if 1 <= int(month) <= 12:
            # A 12-hour clock counts noon as 12 PM (م) and midnight as 12 AM (ص).
            hour = int(hour) % 12 + (12 if meridiem == "م" else 0)
            try:
                return datetime(
                    int(year), int(month), int(day), hour, int(minute), int(second)
                ).isoformat()
            except ValueError:
                pass  # e.g. day out of range for the month — fall through below

    match = MONTH_FIRST.match(timestamp)
    if match:
        month, day, year, hour, minute, second, meridiem = match.groups()
    else:
        match = DAY_FIRST.match(timestamp)
        if match:
            day, month, year, hour, minute, second = match.groups()
            meridiem = None
        else:
            return _convert_with_dateutil(timestamp)

    number = MONTHS.get(month[:3].lower())
    if number is None:
        return _convert_with_dateutil(timestamp)

    hour = int(hour)
    if meridiem:
        # A 12-hour clock counts noon as 12 PM and midnight as 12 AM.
        hour = hour % 12 + (12 if meridiem.lower() == "p" else 0)

    try:
        return datetime(int(year), number, int(day), hour, int(minute), int(second)).isoformat()
    except ValueError:
        return _convert_with_dateutil(timestamp)


def _convert_usec_to_iso8601(timestamp):
    """Converts a timestamp in microseconds since the epoch, as the Chrome history writes
    them (e.g. 1787225185379660), to ISO 8601. ``eh.epoch_to_iso`` cannot read these
    numbers because it takes them for seconds and a microsecond count overflows the year.

    The time is read in UTC and written without the offset, in the shape the activity
    files record their local time in, so that one column holds one format. Sub-second
    precision is dropped for the same reason. A timestamp that is not a number is
    returned unchanged."""

    try:
        seconds = int(timestamp) // 1_000_000
        return datetime.fromtimestamp(seconds, tz=timezone.utc).replace(tzinfo=None).isoformat()
    except (OverflowError, OSError, TypeError, ValueError):
        return timestamp


def _convert_with_dateutil(timestamp):
    """Converts a timestamp of a shape ``_convert_to_iso8601`` does not read itself,
    returning it unchanged when it cannot be read at all."""
    try:
        parts = timestamp.split(' ')

        # Ignore timezone abbreviation at the end as this is not included in json either
        # and cannot be automatically parsed
        if ':' not in parts[-1]:
            parts.pop()

        # Translate month abbreviations to English
        nl_month_translations = {
            'mrt': 'mar',
            'mei': 'may',
            'okt': 'oct',
            }
        for i in range(len(parts)):
            if parts[i].lower() in nl_month_translations:
                parts[i] = nl_month_translations[parts[i].lower()]

        dt = parser.parse(' '.join(parts))
        return dt.isoformat()
    except (ValueError, TypeError) as e:
        return timestamp
