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
