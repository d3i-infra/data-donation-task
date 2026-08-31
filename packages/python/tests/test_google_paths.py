"""Tests for the locale-aware path resolution of the Google platform.

The Google DDP holds many sources in one archive, each exported in a format the
participant chooses per source, so the platform validates and looks its files up by
path instead of by filename. These tests cover what that buys: folder-qualified lookups
that survive same-named files elsewhere in the archive, variants per locale, a locale
detected from folder names alone, and formats that differ within one archive.

Recognition runs on the union member inventory of an ``ArchiveSet`` rather than on a
single zip, so a DDP that was uploaded as several parts is recognized the same way as
one uploaded whole.
"""
import io
import json
import zipfile
from collections import Counter

import pytest

import port.api.props as props
from port.helpers.archive_set import ArchiveSet
from port.helpers.extraction_helpers import ZipArchiveReader
from port.helpers.table_extractor import TableConfig
from port.platforms import google


def _named_part(name: str, members: dict[str, str | bytes]):
    """One archive part (a file-like object with ``.name``/``.size``) from a member
    path -> content mapping."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for path, content in members.items():
            zf.writestr(path, content)
    buf.seek(0)
    buf.name = name
    buf.size = buf.getbuffer().nbytes
    return buf


def make_set(members: dict[str, str | bytes], part_name: str = "takeout-001.zip") -> ArchiveSet:
    """One-part archive-set from a member path -> content mapping."""
    return ArchiveSet([_named_part(part_name, members)])


WATCH_JSON = (
    '[{"title": "Watched A video", "titleUrl": "https://www.youtube.com/watch?v=abc", '
    '"subtitles": [{"name": "A channel", "url": "https://www.youtube.com/channel/UC1"}], '
    '"time": "2026-06-15T20:30:41Z"}]'
)
WATCH_HTML = (
    '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">'
    'Watched <a href="https://www.youtube.com/watch?v=abc">A video</a><br>'
    '<a href="https://www.youtube.com/channel/UC1">A channel</a><br>'
    '15 jun 2026, 20:30:41 CEST</div>'
)
SEARCH_JSON = '[{"title": "Searched for cats", "titleUrl": "https://www.youtube.com/results?search_query=cats", "time": "2026-06-15T20:30:41Z"}]'
SEARCH_HTML = (
    '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">'
    'Searched for <a href="https://www.youtube.com/results?search_query=cats">cats</a><br>'
    '15 jun 2026, 20:30:41 CEST</div>'
)
ACTIVITY_JSON = f"[{WATCH_JSON[1:-1]}, {SEARCH_JSON[1:-1]}]"
SUBSCRIPTIONS_CSV = "Channel Id,Channel Url,Channel Title\nUC1,https://youtube.com/channel/UC1,A channel\n"
COMMENTS_CSV = (
    "Comment ID,Channel ID,Comment create timestamp,Price,Video ID,Comment text\n"
    'c1,UC1,2026-06-15T20:30:41Z,0,abc,"{""text"":""hello""}"\n'
)

# --- Task 5: the six activity-stream sources -------------------------------

GOOGLE_SEARCH_JSON = '[{"title": "Searched for cats", "titleUrl": "https://www.google.com/search?q=cats", "time": "2026-06-15T20:30:41Z"}]'
GOOGLE_SEARCH_HTML = (
    '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">'
    'Searched for <a href="https://www.google.com/search?q=cats">cats</a><br>'
    '15 jun 2026, 20:30:41 CEST</div>'
)
CHROME_JSON = '[{"title": "Visited a page", "titleUrl": "https://example.org", "time": "2026-06-15T20:30:41Z"}]'
CHROME_HTML = (
    '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">'
    'Visited <a href="https://example.org">a page</a><br>'
    '15 jun 2026, 20:30:41 CEST</div>'
)
VIDEO_SEARCH_JSON = '[{"title": "Searched for cat videos", "titleUrl": "https://www.google.com/search?q=cat+videos&tbm=vid", "time": "2026-06-15T20:30:41Z"}]'
VIDEO_SEARCH_HTML = (
    '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">'
    'Searched for <a href="https://www.google.com/search?q=cat+videos&tbm=vid">cat videos</a><br>'
    '15 jun 2026, 20:30:41 CEST</div>'
)
ADS_JSON = '[{"title": "Viewed ad", "titleUrl": "https://example.org/an-advert", "time": "2026-06-15T20:30:41Z"}]'
ADS_HTML = (
    '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">'
    'Viewed ad from <a href="https://example.org/an-advert">Example Advertiser</a><br>'
    '15 jun 2026, 20:30:41 CEST</div>'
)
DISCOVER_JSON = '[{"title": "Read an article", "time": "2026-06-15T20:30:41Z"}]'
DISCOVER_HTML = (
    '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">'
    'Read an article<br>'
    '15 jun 2026, 20:30:41 CEST</div>'
)
GOOGLE_NEWS_JSON = '[{"title": "Read a news article", "titleUrl": "https://news.google.com/articles/abc", "time": "2026-06-15T20:30:41Z"}]'
GOOGLE_NEWS_HTML = (
    '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">'
    'Read <a href="https://news.google.com/articles/abc">a news article</a><br>'
    '15 jun 2026, 20:30:41 CEST</div>'
)

# --- Task 6: the News My Activity stream (news.history), distinct from the News
# product's own files (news.articles/followed_sources/followed_topics/
# followed_locations/magazines) below. ----------------------------------------

NEWS_JSON = '[{"title": "Read a news article", "titleUrl": "https://news.google.com/articles/xyz", "time": "2026-06-15T20:30:41Z"}]'
NEWS_HTML = (
    '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">'
    'Read <a href="https://news.google.com/articles/xyz">a news article</a><br>'
    '15 jun 2026, 20:30:41 CEST</div>'
)
#: One followed-source line — enough for ``news_items_to_df`` to produce exactly
#: one row via its ``news.followed_sources`` key, the representative member of
#: the five ``news.*`` product-file keys it reads together.
NEWS_FOLLOWED_SOURCES_TXT = "NOS\n"


class TestValidation:
    def test_archive_is_recognized_and_its_locale_reported(self):
        validation = google.validate_ddp(make_set({
            "Takeout/YouTube en YouTube Music/geschiedenis/kijkgeschiedenis.html": WATCH_HTML,
            "Takeout/YouTube en YouTube Music/abonnementen/abonnementen.csv": SUBSCRIPTIONS_CSV,
        }))

        assert validation.get_status_code_id() == 0
        assert validation.ddp_locale == "nl"

    def test_archive_without_a_known_source_is_rejected(self):
        validation = google.validate_ddp(make_set({"Takeout/Chrome/BrowserHistory.json": "{}"}))

        assert validation.get_status_code_id() == 1

    # A corrupt part raises zipfile.BadZipFile at ArchiveSet construction, before
    # validate_ddp is ever called, and the flow routes that to the retry prompt.
    # Pinned by test_flow_builder.py::TestMultiFileFlow::test_corrupt_part_routes_to_retry.


class TestPathResolution:
    def test_folder_qualifier_ignores_a_same_named_file_elsewhere(self):
        validation = google.validate_ddp(make_set({
            "Takeout/YouTube und YouTube Music/Verlauf/Wiedergabeverlauf.html": WATCH_HTML,
            "Takeout/Irgendwas anderes/Wiedergabeverlauf.html": "",
        }))

        assert validation.get_status_code_id() == 0
        assert validation.ddp_locale == "de"
        assert "Takeout/YouTube und YouTube Music/Verlauf/Wiedergabeverlauf.html" in validation.archive_members

    def test_next_variant_resolves_when_the_first_is_absent(self):
        """The watch history falls back to the YouTube activity file, so an archive
        exported without the history folder still yields recognition."""
        validation = google.validate_ddp(make_set({
            "Takeout/My Activity/YouTube/MyActivity.json": WATCH_JSON,
        }))

        assert validation.get_status_code_id() == 0
        assert validation.ddp_locale == "en"


# The activity FILENAME, verified against real per-locale exports (es-ES, ar-EG,
# zh-CN, tr-TR, de-DE: 2026-08-27; en-GB, nl-NL: 2026-08-26): Google Takeout
# translates it, and it is not always one of the two untranslated spellings.
TRANSLATED_ACTIVITY_FILENAME = {
    "en": "My Activity",
    "nl": "MyActivity",
    "es": "MiActividad",
    "ar": "نشاطي",
    "zh": "我的活动记录",
    "tr": "Etkinliğim",
    "de": "MeineAktivitäten",
}


class TestFilenameSpellings:
    """The activity FILENAME is locale-dependent and, per the 2026-08-27 real-export
    evidence, usually TRANSLATED rather than just spaced/unspaced: es writes
    'MiActividad.html', ar 'نشاطي.html', zh '我的活动记录.html', tr 'Etkinliğim.html',
    de 'MeineAktivitäten.html'. en's translation happens to equal 'My Activity' (with
    a space) and nl's happens to equal 'MyActivity' (without) — the two-spelling story
    from the previous era was really this same rule with only those two locales'
    evidence in hand. Every MyActivity-family entry therefore carries three things:
    its locale's translated filename, and both untranslated spellings as cross-era/
    cross-locale fallbacks."""

    def test_spaced_filename_resolves_for_english(self):
        validation = google.validate_ddp(make_set(
            {"Takeout/My Activity/Search/My Activity.html": "x"}))
        assert validation.get_status_code_id() == 0
        assert validation.ddp_locale == "en"

    def test_unspaced_filename_resolves_for_dutch(self):
        validation = google.validate_ddp(make_set(
            {"Takeout/Mijn activiteit/Zoeken/MyActivity.html": "x"}))
        assert validation.get_status_code_id() == 0
        assert validation.ddp_locale == "nl"

    def test_every_myactivity_entry_lists_translated_filename_and_both_fallbacks(self):
        for locale, keys in google.TAKEOUT_PATHS.items():
            translated = TRANSLATED_ACTIVITY_FILENAME[locale]
            for key, paths in keys.items():
                is_myactivity_family = any(
                    p.endswith("/MyActivity") or p.endswith("/My Activity") for p in paths)
                if not is_myactivity_family:
                    continue  # a direct-source or News-product key, not part of the family
                assert any(p.endswith(f"/{translated}") for p in paths), (
                    f"{locale}/{key}: missing translated activity filename {translated!r}")
                assert any(p.endswith("/MyActivity") for p in paths), (
                    f"{locale}/{key}: missing the untranslated 'MyActivity' fallback")
                assert any(p.endswith("/My Activity") for p in paths), (
                    f"{locale}/{key}: missing the untranslated 'My Activity' fallback")


class TestMultiPartValidation:
    def test_sources_split_across_parts_union_into_one_recognition(self):
        part1 = _named_part("takeout-001.zip", {
            "Takeout/YouTube and YouTube Music/history/watch-history.json": WATCH_JSON,
        })
        part2 = _named_part("takeout-002.zip", {
            "Takeout/Chrome/History.json": "[]",
        })

        validation = google.validate_ddp(ArchiveSet([part1, part2]))

        assert validation.get_status_code_id() == 0
        assert set(validation.archive_members) >= {
            "Takeout/YouTube and YouTube Music/history/watch-history.json",
            "Takeout/Chrome/History.json",
        }


class TestNewsProductPaths:
    def test_news_product_folder_translates_but_filenames_do_not(self):
        # Verified on real exports: nl folder is 'Nieuws', filenames stay English.
        validation = google.validate_ddp(make_set(
            {"Takeout/Nieuws/followed_sources.txt": "NOS"}))
        assert validation.get_status_code_id() == 0


class TestCurrentExportPaths:
    """One small synthetic archive per locale, built from paths copied byte-exact
    (via a scratch zipfile-dump script, never retyped by hand) out of the five real
    2026-08-27 Takeout exports (es-ES, ar-EG, zh-CN, tr-TR, de-DE). These pin the
    current-era corrections in TAKEOUT_PATHS against the evidence that produced them.

    tr is the load-bearing regression here: against the pre-correction table, a
    real current tr export matched ZERO entries (sources_found == 0, a hard
    validation failure routing the participant into a retry loop) because the
    activity filename is translated and the YouTube history filenames are
    lowercase. The tr case below uses both a My-Activity file and a direct
    YouTube-history file, exactly as a real export would, and must validate."""

    def test_current_spanish_export_resolves(self):
        validation = google.validate_ddp(make_set(
            {"Takeout/Mi actividad/Búsqueda/MiActividad.html": "x"}))
        assert validation.get_status_code_id() == 0
        assert validation.ddp_locale == "es"

    def test_current_arabic_export_resolves(self):
        # Byte-exact: the history folder carries the shadda diacritic (U+0651).
        validation = google.validate_ddp(make_set(
            {"Takeout/Chrome/السجلّ.json": "{}"}))
        assert validation.get_status_code_id() == 0
        assert validation.ddp_locale == "ar"

    def test_current_chinese_export_resolves(self):
        validation = google.validate_ddp(make_set(
            {"Takeout/我的活动/Search/我的活动记录.html": "x"}))
        assert validation.get_status_code_id() == 0
        assert validation.ddp_locale == "zh"

    def test_current_turkish_export_resolves(self):
        # Regression: this exact pair matched nothing against the pre-correction
        # table. Turkish casing is load-bearing (dotted/dotless i) — copied byte-exact.
        validation = google.validate_ddp(make_set({
            "Takeout/Etkinliğim/Arama/Etkinliğim.html": "x",
            "Takeout/YouTube ve YouTube Music/geçmiş/izleme geçmişi.html": "x",
        }))
        assert validation.get_status_code_id() == 0
        assert validation.ddp_locale == "tr"

    def test_current_german_export_resolves(self):
        validation = google.validate_ddp(make_set(
            {"Takeout/Meine Aktivitäten/Google Suche/MeineAktivitäten.html": "x"}))
        assert validation.get_status_code_id() == 0
        assert validation.ddp_locale == "de"

    def test_turkish_news_product_folder_is_google_haberler_not_bare_haberler(self):
        # The pre-correction guess ("Haberler") was wrong; the real folder carries
        # a "Google " prefix, unlike the tr My-Activity News-activity folder.
        validation = google.validate_ddp(make_set(
            {"Takeout/Google Haberler/followed_sources.txt": "NTV"}))
        assert validation.get_status_code_id() == 0

    def test_german_news_product_folder_is_google_news_not_nachrichten(self):
        # The pre-correction guess ("Nachrichten") was wrong; the News product's own
        # export folder stays English, unlike the translated My-Activity folder.
        validation = google.validate_ddp(make_set(
            {"Takeout/Google News/articles.txt": "Spiegel"}))
        assert validation.get_status_code_id() == 0


class TestLocaleDetection:
    def test_locale_comes_from_folders_when_filenames_are_identical(self, monkeypatch):
        """A locale that translates only its folder names leaves every filename in
        English, which is exactly what filename matching cannot see."""
        paths = dict(google.TAKEOUT_PATHS)
        paths["xx"] = {"youtube.watch_history": ["mijn activiteit/watch-history"]}
        monkeypatch.setattr(google, "TAKEOUT_PATHS", paths)

        members = ["Takeout/mijn activiteit/watch-history.json"]

        assert google._detect_locale(members)[0] == "xx"

    def test_translated_folder_identifies_the_locale_of_an_english_filename(self):
        """The real case the rule above exists for: Dutch translates the activity
        folder but leaves the file called MyActivity."""
        members = ["Takeout/Mijn activiteit/YouTube/MyActivity.json"]

        locale, sources_found = google._detect_locale(members)

        assert locale == "nl"
        assert sources_found > 0

    def test_nothing_recognized_reports_no_sources(self):
        assert google._detect_locale(["Takeout/nothing/we/know.json"])[1] == 0

    def test_pure_current_english_archive_still_detects_as_english(self):
        # zh's current My-Activity subfolders and de's News-product folder are now
        # English words ("Search", "Google News"), which is exactly the kind of
        # thing that could tie against en. A pure current-en archive (no locale
        # mixing) must still resolve to en.
        members = [
            "Takeout/YouTube and YouTube Music/history/watch-history.html",
            "Takeout/My Activity/Search/My Activity.html",
            "Takeout/Chrome/History.json",
        ]
        locale, sources_found = google._detect_locale(members)
        assert locale == "en"
        assert sources_found > 0

    def test_pure_current_chinese_archive_still_detects_as_chinese(self):
        # zh's own folder evidence (我的活动, 历史记录, the YouTube 和 YouTube Music
        # top folder) is Chinese-specific even where the My-Activity SUBfolder
        # ("Search") is English, so a pure current-zh archive should win for zh
        # rather than tie against en.
        members = [
            "Takeout/YouTube 和 YouTube Music/历史记录/观看记录.html",
            "Takeout/我的活动/Search/我的活动记录.html",
            "Takeout/Chrome/历史记录.json",
        ]
        locale, sources_found = google._detect_locale(members)
        assert locale == "zh"
        assert sources_found > 0


class TestTableConsistency:
    @pytest.mark.parametrize("locale", list(google.TAKEOUT_PATHS))
    def test_every_locale_covers_every_key(self, locale):
        assert set(google.TAKEOUT_PATHS[locale]) == set(google.KEY_FORMATS)

    @pytest.mark.parametrize("locale", list(google.TAKEOUT_PATHS))
    def test_paths_are_extension_less(self, locale):
        for paths in google.TAKEOUT_PATHS[locale].values():
            for path in paths:
                assert not path.rsplit("/", 1)[-1].count(".")
