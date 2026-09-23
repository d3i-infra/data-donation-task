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


def _real_extractor_config() -> list[TableConfig]:
    """All 12 tables wired to their real ``EXTRACTOR_REGISTRY`` extractors,
    built without touching the generated config file (spec §8
    config-independence rule) — used to monkeypatch ``load_port_config`` in
    tests that need ``extraction()``'s real per-table extraction behavior."""
    return [
        TableConfig(
            id=name.removesuffix("_to_df"),
            extractor=extractor,
            title=props.Translatable({"en": name, "nl": name}),
            description=props.Translatable({"en": "", "nl": ""}),
            headers={},
        )
        for name, extractor in google.EXTRACTOR_REGISTRY.items()
    ]


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

    def test_missing_source_yields_no_table_and_no_error(self, monkeypatch):
        """A source ``extraction()`` cannot find contributes neither a table
        (``run_extraction`` drops empty frames) nor an error — an absent DDP
        member is expected, not a failure (ADR-0024).

        Builds the table config from the real ``EXTRACTOR_REGISTRY`` via a
        monkeypatched ``load_port_config`` rather than the generated config
        file (spec §8 config-independence rule), the same pattern
        ``test_google_platform.py``'s ddp-locale/interface tests use."""
        monkeypatch.setattr(
            google, "load_port_config", lambda registry, platform: _real_extractor_config()
        )
        archive_set = make_set({
            "Takeout/YouTube and YouTube Music/history/watch-history.json": WATCH_JSON,
        })
        validation = google.validate_ddp(archive_set)

        result = google.extraction(archive_set, validation)

        assert [table.id for table in result.tables] == ["youtube_watch_history"]
        assert result.errors == Counter()


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


# ---------------------------------------------------------------------------
# Task 4: the four YouTube extractors, called directly against a reader.
#
# The fork's own extractor tests (algosoc-2026, tip 1ca6b1a) drive everything
# through a config-driven `extract()` helper that wraps `google.extraction()`.
# That function was Task 7's and did not exist yet at the time these were
# written, so these tests build a `ZipArchiveReader` over a one-part
# `ArchiveSet` directly and call each extractor instead — a pattern later
# tasks (`TestDetailsColumn`, `TestEveryLocale`) kept even after `extraction()`
# landed, since it needs no config file. Only
# `TestPathResolution.test_missing_source_yields_no_table_and_no_error` — which
# genuinely needs `extraction()`'s table-id assertions via `result.tables` —
# was deferred to Task 7, where it now lives (above, in `TestPathResolution`).
# ---------------------------------------------------------------------------


def _reader_for(members: dict[str, str | bytes]) -> tuple[ZipArchiveReader, Counter, str]:
    """Validates an in-memory one-part archive-set and returns a reader ready for
    an extractor call, together with its error counter and detected locale."""
    archive_set = make_set(members)
    validation = google.validate_ddp(archive_set)
    errors: Counter = Counter()
    reader = ZipArchiveReader(archive_set, validation.archive_members, errors)
    return reader, errors, validation.ddp_locale


class TestActivityFile:
    def test_views_and_searches_are_split_when_read_from_the_activity_file(self):
        """Both YouTube histories fall back to the same activity file, which records
        views and searches together — each extractor must take only its own rows."""
        reader, errors, ddp_locale = _reader_for({
            "Takeout/My Activity/YouTube/MyActivity.json": ACTIVITY_JSON,
        })

        watched = google.youtube_watch_history_to_df(reader, errors, ddp_locale)
        searched = google.youtube_search_history_to_df(reader, errors, ddp_locale)

        assert len(watched) == 1
        assert len(searched) == 1


class TestWatchHistoryRow:
    """The table carries the channel a video was published by, which both formats write
    under the title, and the details a view sometimes records, such as an ad it came
    from — a row that has neither leaves those columns empty rather than dropping out."""

    #: A view from an ad names no channel, and the line it does carry under the title is
    #: the time it was watched at — a description, which is not the channel of anything.
    ADVERT_JSON = (
        '[{"title": "Watched An advert", "titleUrl": "https://www.youtube.com/watch?v=abc", '
        '"description": "Watched at 11:39 AM", "details": [{"name": "From Google Ads"}], '
        '"time": "2026-06-15T20:30:41Z"}]'
    )
    ADVERT_HTML = (
        '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">'
        'Watched <a href="https://www.youtube.com/watch?v=abc">An advert</a><br>'
        'Watched at 11:39 AM<br>'
        '15 jun 2026, 20:30:41 CEST</div>'
    )

    def row(self, content: str, extension: str) -> dict:
        reader, errors, ddp_locale = _reader_for({
            f"Takeout/YouTube and YouTube Music/history/watch-history.{extension}": content,
        })
        df = google.youtube_watch_history_to_df(reader, errors, ddp_locale)
        return df.iloc[0].to_dict()

    @pytest.mark.parametrize(
        "content,extension", [(WATCH_JSON, "json"), (WATCH_HTML, "html")], ids=["json", "html"]
    )
    def test_the_channel_comes_out_the_same_from_either_format(self, content, extension):
        row = self.row(content, extension)

        assert row["Channel name"] == "A channel"
        assert row["Channel URL"] == "https://www.youtube.com/channel/UC1"

    def test_a_view_that_came_from_an_ad_says_so(self):
        assert self.row(self.ADVERT_JSON, "json")["Details"] == "From Google Ads"

    @pytest.mark.parametrize("extension", ["json", "html"])
    def test_a_view_without_a_channel_keeps_its_row_and_names_none(self, extension):
        """The description an ad carries under its title is not a channel, so it stays out
        of the columns naming one — the row is still the video that was watched."""
        row = self.row(getattr(self, f"ADVERT_{extension.upper()}"), extension)

        assert row["Title"] == "Watched An advert"
        assert row["Channel name"] == ""
        assert row["Channel URL"] == ""


class TestFormats:
    def test_sources_may_use_different_formats_in_one_archive(self):
        reader, errors, ddp_locale = _reader_for({
            "Takeout/YouTube and YouTube Music/history/watch-history.json": WATCH_JSON,
            "Takeout/YouTube and YouTube Music/history/search-history.html": SEARCH_HTML,
            "Takeout/YouTube and YouTube Music/subscriptions/subscriptions.csv": SUBSCRIPTIONS_CSV,
        })

        watched = google.youtube_watch_history_to_df(reader, errors, ddp_locale)
        searched = google.youtube_search_history_to_df(reader, errors, ddp_locale)
        subscriptions = google.youtube_subscriptions_to_df(reader, errors, ddp_locale)

        assert len(watched) == 1
        assert len(searched) == 1
        assert len(subscriptions) == 1


class TestYoutubeSubscriptions:
    def test_columns_are_normalized_to_english(self):
        reader, errors, ddp_locale = _reader_for({
            "Takeout/YouTube and YouTube Music/subscriptions/subscriptions.csv": SUBSCRIPTIONS_CSV,
        })

        df = google.youtube_subscriptions_to_df(reader, errors, ddp_locale)

        assert list(df.columns) == ["Channel Id", "Channel URL", "Channel Name"]
        assert df.iloc[0]["Channel Name"] == "A channel"

    def test_no_matching_file_yields_an_empty_dataframe(self):
        reader, errors, ddp_locale = _reader_for({
            "Takeout/YouTube and YouTube Music/history/watch-history.json": WATCH_JSON,
        })

        df = google.youtube_subscriptions_to_df(reader, errors, ddp_locale)

        assert df.empty


class TestYoutubeComments:
    def test_columns_are_normalized_and_comment_text_is_parsed(self):
        reader, errors, ddp_locale = _reader_for({
            "Takeout/YouTube and YouTube Music/comments/comments.csv": COMMENTS_CSV,
        })

        df = google.youtube_comments_to_df(reader, errors, ddp_locale)

        assert list(df.columns) == [
            "Timestamp", "Channel ID", "Comment text", "Comment ID", "Video ID", "Price",
        ]
        assert df.iloc[0]["Comment text"] == "hello"

    def test_no_matching_file_yields_an_empty_dataframe(self):
        reader, errors, ddp_locale = _reader_for({
            "Takeout/YouTube and YouTube Music/history/watch-history.json": WATCH_JSON,
        })

        df = google.youtube_comments_to_df(reader, errors, ddp_locale)

        assert df.empty


class TestParseCommentText:
    def test_segments_join_into_one_string(self):
        assert google._parse_comment_text('{"text": "hello"}, {"text": "world"}') == "hello world"

    def test_unparseable_text_is_returned_unchanged(self):
        assert google._parse_comment_text("not json") == "not json"


class TestReadActivity:
    """``_read_activity`` is what the watch/search-history extractors call: json parsed
    whole, html streamed through ``open_member`` (ADR-0040), and a key absent from the
    archive-set yielding ``None`` rather than raising."""

    def test_json_is_read_whole(self):
        reader, errors, ddp_locale = _reader_for({
            "Takeout/YouTube and YouTube Music/history/watch-history.json": WATCH_JSON,
        })

        data = google._read_activity(reader, errors, "youtube.watch_history", ddp_locale)

        assert data == [{
            "title": "Watched A video",
            "titleUrl": "https://www.youtube.com/watch?v=abc",
            "subtitles": [{"name": "A channel", "url": "https://www.youtube.com/channel/UC1"}],
            "time": "2026-06-15T20:30:41Z",
        }]

    def test_html_is_streamed_through_open_member(self):
        reader, errors, ddp_locale = _reader_for({
            "Takeout/YouTube and YouTube Music/history/watch-history.html": WATCH_HTML,
        })

        data = google._read_activity(reader, errors, "youtube.watch_history", ddp_locale)

        assert len(data) == 1
        assert data[0]["title"] == "Watched A video"

    def test_absent_key_yields_none_without_an_error(self):
        reader, errors, ddp_locale = _reader_for({
            "Takeout/Chrome/History.json": "{}",
        })

        data = google._read_activity(reader, errors, "youtube.watch_history", ddp_locale)

        assert data is None
        assert errors == Counter()


# ---------------------------------------------------------------------------
# Task 5: the six activity-stream extractors — Search, Chrome, Video Search,
# Ads, Discover, Google News. Ported from the fork's TestSearchLocations and
# TestDetailsColumn (algosoc-2026, tip 1ca6b1a), against direct extractor
# calls per the same Task-4 pattern used above — the fork's own tests drive
# through its `extract()` helper, which wraps `google.extraction()`. That
# function is Task 7's and did not exist yet when these were written; the
# direct-call pattern was kept even once it landed, since it needs no config
# file (test_google_platform.py covers extraction()/GoogleFlow directly).
#
# `filter_explicit_content(...)` — the fork's content blocklist, called on
# four of these six extractors at fork tip — is NOT ported (fork-side
# override per ADR/handoff note), so no test here exercises it.
# ---------------------------------------------------------------------------


class TestSearchLocations:
    """A Google search is recorded with the general area it was made from, which the
    table carries as the area, its link to Maps and, behind a dash, how it was arrived
    at."""

    AREA = "https://www.google.com/maps/@?api=1&map_action=map&center=10.000000,20.000000"

    def row(self, item: dict) -> dict:
        reader, errors, ddp_locale = _reader_for({
            "Takeout/My Activity/Search/MyActivity.json": json.dumps([item]),
        })
        df = google.search_history_to_df(reader, errors, ddp_locale)
        return df.iloc[0].to_dict()

    def search(self, **extra) -> dict:
        return {
            "title": "Searched for cats",
            "titleUrl": "https://www.google.com/search?q=cats",
            "time": "2026-06-15T20:30:41Z",
            **extra,
        }

    def test_a_location_reads_as_its_area_link_and_source(self):
        row = self.row(self.search(locationInfos=[
            {"name": "At this general area", "url": self.AREA, "source": "From your device"}
        ]))

        assert row["Locations"] == f"At this general area {self.AREA} - From your device"

    def test_several_locations_stand_beside_each_other(self):
        row = self.row(self.search(locationInfos=[
            {"name": "At this general area", "url": self.AREA, "source": "From your device"},
            {"name": "Somewhere else", "url": self.AREA, "source": "Based on your past activity"},
        ]))

        assert row["Locations"].count(" - ") == 2
        assert "Somewhere else" in row["Locations"]

    def test_a_location_without_a_source_carries_no_dash(self):
        row = self.row(self.search(locationInfos=[{"name": "Somewhere", "url": self.AREA}]))

        assert row["Locations"] == f"Somewhere {self.AREA}"

    def test_a_search_placed_nowhere_leaves_the_column_empty(self):
        assert self.row(self.search())["Locations"] == ""


class TestDetailsColumn:
    """The activity files record how some activity came about — a search or an ad shown
    from Google Ads — beside the activity itself. Every extractor that reads such a file
    has to carry it, so the row says where the activity came from and not only what it
    was."""

    #: The path of the source in an English archive, the extractor it feeds, and an url
    #: the row takes, since two of them select their records by url.
    SOURCES = [
        ("YouTube and YouTube Music/history/search-history", "youtube_search_history_to_df",
         "https://www.youtube.com/results?search_query=cats"),
        ("My Activity/Search/MyActivity", "search_history_to_df",
         "https://www.google.com/search?q=cats"),
        ("My Activity/Ads/MyActivity", "ads_history_to_df", "https://example.org/an-advert"),
    ]

    def table(self, path: str, content: str, fn_name: str):
        reader, errors, ddp_locale = _reader_for({f"Takeout/{path}.json": content})
        return getattr(google, fn_name)(reader, errors, ddp_locale)

    @pytest.mark.parametrize("path,fn_name,title_url", SOURCES, ids=[s[1] for s in SOURCES])
    def test_details_reach_the_table(self, path, fn_name, title_url):
        content = json.dumps([{
            "title": "An activity",
            "titleUrl": title_url,
            "details": [{"name": "From Google Ads"}],
            "time": "2026-06-15T20:30:41Z",
        }])

        assert self.table(path, content, fn_name)["Details"].tolist() == ["From Google Ads"]

    def test_a_detail_that_links_somewhere_keeps_its_url(self):
        """The json keeps the name of such a detail and the url it points to apart, where
        the html writes them as one line — and the column has to read the same either way,
        so the json is joined back into the line the html already produces."""
        content = json.dumps([{
            "title": "Visited a page",
            "titleUrl": "https://www.google.com/search?q=cats",
            "details": [{
                "name": "Tried to open in app",
                "url": "https://example.org/groups/abc",
            }],
            "time": "2026-06-15T20:30:41Z",
        }])

        table = self.table("My Activity/Search/MyActivity", content, "search_history_to_df")

        assert table["Details"].tolist() == [
            "Tried to open in app: https://example.org/groups/abc"
        ]

    @pytest.mark.parametrize("path,fn_name,title_url", SOURCES, ids=[s[1] for s in SOURCES])
    def test_an_activity_without_details_leaves_the_column_empty(self, path, fn_name, title_url):
        content = json.dumps([
            {"title": "An activity", "titleUrl": title_url, "time": "2026-06-15T20:30:41Z"}
        ])

        assert self.table(path, content, fn_name)["Details"].tolist() == [""]


class TestChromeHistory:
    """Chrome's own export (``Chrome/History.json``) writes a dict of ``{"Browser
    History": [...]}`` with microsecond timestamps; the fallback My-Activity file
    writes the ordinary activity-list shape with a ``time`` field like every other
    source. Both must read into the same three columns."""

    def test_browser_history_dict_format_uses_usec_timestamps(self):
        content = json.dumps({
            "Browser History": [
                {"title": "A page", "url": "https://example.org", "time_usec": 1750000000000000}
            ]
        })
        reader, errors, ddp_locale = _reader_for({"Takeout/Chrome/History.json": content})

        df = google.chrome_history_to_df(reader, errors, ddp_locale)

        assert df.iloc[0]["Title"] == "A page"
        assert df.iloc[0]["URL"] == "https://example.org"
        assert df.iloc[0]["Timestamp"] == google._convert_usec_to_iso8601(1750000000000000)

    def test_myactivity_fallback_list_format_uses_the_time_field(self):
        reader, errors, ddp_locale = _reader_for({
            "Takeout/My Activity/Chrome/MyActivity.json": CHROME_JSON,
        })

        df = google.chrome_history_to_df(reader, errors, ddp_locale)

        assert df.iloc[0]["Title"] == "Visited a page"
        assert df.iloc[0]["Timestamp"] == "2026-06-15T20:30:41Z"

    def test_no_matching_file_yields_an_empty_dataframe(self):
        reader, errors, ddp_locale = _reader_for({
            "Takeout/YouTube and YouTube Music/history/watch-history.json": WATCH_JSON,
        })

        df = google.chrome_history_to_df(reader, errors, ddp_locale)

        assert df.empty


class TestEveryLocale:
    """Builds a synthetic DDP from the CURRENT (index-0, translated-filename-first)
    paths of each locale in the committed ``TAKEOUT_PATHS``, so a table entry that no
    extractor can reach fails here instead of silently producing an empty table in the
    field. Extractors are called directly per key, not through ``extraction()`` — this
    class exercises path resolution in isolation from the config-driven wiring that
    ``extraction()`` layers on top (covered separately in ``test_google_platform.py``).

    Covers the four YouTube keys, the six activity-stream keys, and both News shapes:
    ``news.history`` (the My Activity stream, via ``news_history_to_df``) and one
    representative product-file key, ``news.followed_sources`` (via
    ``news_items_to_df``, which reads all five ``news.*`` product files together —
    see that extractor's docstring). All 12 registered extractors are covered; every
    locale sweeps all 12 tables."""

    #: key -> the extractor function name that reads it.
    TABLES: dict[str, str] = {
        "youtube.watch_history": "youtube_watch_history_to_df",
        "youtube.search_history": "youtube_search_history_to_df",
        "youtube.subscriptions": "youtube_subscriptions_to_df",
        "youtube.comments": "youtube_comments_to_df",
        "search.search_history": "search_history_to_df",
        "chrome.history": "chrome_history_to_df",
        "video_search.history": "video_search_history_to_df",
        "ads.history": "ads_history_to_df",
        "discover.history": "discover_history_to_df",
        "google_news.history": "google_news_history_to_df",
        "news.history": "news_history_to_df",
        "news.followed_sources": "news_items_to_df",
    }

    CONTENT = {
        ("youtube.watch_history", "json"): WATCH_JSON,
        ("youtube.watch_history", "html"): WATCH_HTML,
        ("youtube.search_history", "json"): SEARCH_JSON,
        ("youtube.search_history", "html"): SEARCH_HTML,
        ("youtube.subscriptions", "csv"): SUBSCRIPTIONS_CSV,
        ("youtube.comments", "csv"): COMMENTS_CSV,
        ("search.search_history", "json"): GOOGLE_SEARCH_JSON,
        ("search.search_history", "html"): GOOGLE_SEARCH_HTML,
        ("chrome.history", "json"): CHROME_JSON,
        ("chrome.history", "html"): CHROME_HTML,
        ("video_search.history", "json"): VIDEO_SEARCH_JSON,
        ("video_search.history", "html"): VIDEO_SEARCH_HTML,
        ("ads.history", "json"): ADS_JSON,
        ("ads.history", "html"): ADS_HTML,
        ("discover.history", "json"): DISCOVER_JSON,
        ("discover.history", "html"): DISCOVER_HTML,
        ("google_news.history", "json"): GOOGLE_NEWS_JSON,
        ("google_news.history", "html"): GOOGLE_NEWS_HTML,
        ("news.history", "json"): NEWS_JSON,
        ("news.history", "html"): NEWS_HTML,
        ("news.followed_sources", "txt"): NEWS_FOLLOWED_SOURCES_TXT,
    }

    @pytest.mark.parametrize("preferred_format", ["json", "html"])
    @pytest.mark.parametrize("locale", list(google.TAKEOUT_PATHS))
    def test_all_tables_extract(self, locale, preferred_format):
        members = {}
        for key in self.TABLES:
            formats = google.KEY_FORMATS[key]
            extension = preferred_format if preferred_format in formats else formats[0]
            path = google.TAKEOUT_PATHS[locale][key][0]
            members[f"Takeout/{path}.{extension}"] = self.CONTENT[(key, extension)]

        reader, errors, ddp_locale = _reader_for(members)
        assert ddp_locale == locale

        rows = {
            fn_name: len(getattr(google, fn_name)(reader, errors, ddp_locale))
            for fn_name in self.TABLES.values()
        }

        assert rows == {fn_name: 1 for fn_name in self.TABLES.values()}

    @pytest.mark.parametrize("locale", list(google.TAKEOUT_PATHS))
    def test_myactivity_spellings_resolve(self, locale):
        """Every My-Activity-family path list lists its locale's translated filename
        first, then both untranslated spellings (``MyActivity`` and ``My Activity``) as
        fallbacks — any one of them must resolve the DDP as this locale and reach the
        table, not just the first entry ``test_all_tables_extract`` above exercises."""
        key, fn_name = "search.search_history", "search_history_to_df"
        paths = google.TAKEOUT_PATHS[locale][key]
        spellings = [p for p in paths if p.endswith("/MyActivity") or p.endswith("/My Activity")]
        assert spellings, f"{locale}: no untranslated MyActivity spelling listed for {key}"

        for path in spellings:
            reader, errors, ddp_locale = _reader_for(
                {f"Takeout/{path}.json": self.CONTENT[(key, "json")]}
            )

            assert ddp_locale == locale
            assert len(getattr(google, fn_name)(reader, errors, ddp_locale)) == 1


# ---------------------------------------------------------------------------
# Task 6: both News shapes, plus the best-effort Failed-Files detector.
#
# ``_reader_for`` above already builds a reader (with its errors counter and
# detected locale) from a member-path -> content mapping, and every other test
# class in this module goes through it. These classes follow that established
# pattern rather than the task brief's older ``_reader_for(archive_set) ->
# reader`` sketch — the two would collide on one name with incompatible
# signatures, and the existing three-way return already gives every case here
# what it needs (the news-items tests pass an explicit literal locale rather
# than the detected one, since a bare "News/<file>" fallback path resolves to
# several locales at once and detection would not reliably land on "en").
# ---------------------------------------------------------------------------


class TestNewsExtractors:
    """Researcher decision 2026-08-27: BOTH News shapes are extracted — the
    My Activity stream (account-dependent) and the Takeout/News product files."""

    def test_news_history_reads_the_activity_stream(self):
        # same shape as google_news history tests, key news.history
        reader, errors, ddp_locale = _reader_for({
            "Takeout/My Activity/News/MyActivity.json": NEWS_JSON,
        })

        df = google.news_history_to_df(reader, errors, ddp_locale)

        assert len(df) == 1
        assert df.iloc[0]["Title"] == "Read a news article"

    def test_news_items_collects_the_product_files_with_their_kind(self):
        reader, errors, _ = _reader_for({
            "Takeout/News/articles.txt": "Some saved article\n",
            "Takeout/News/followed_sources.txt": "NOS\nThe New York Times\n",
            "Takeout/News/followed_topics.txt": "",
            "Takeout/News/magazines.txt": "",
        })
        df = google.news_items_to_df(reader, Counter(), "en")
        assert list(df.columns) == ["Type", "Name"]
        assert ("Followed source", "NOS") in set(map(tuple, df.values))
        assert len(df) == 3  # empty files contribute no rows

    def test_news_items_resolves_the_translated_dutch_folder(self):
        reader, errors, _ = _reader_for({"Takeout/Nieuws/followed_sources.txt": "NOS\n"})
        df = google.news_items_to_df(reader, Counter(), "nl")
        assert len(df) == 1


class TestFailedFilesDetector:
    """Counts non-empty failure-message nodes in archive_browser.html when the
    participant included the manifest ('see report'); locale-robust because it
    matches the CSS class, never the message text (which localizes)."""

    MANIFEST = (
        '<html><body>'
        '<div class="file-leaf"><div class="extracted-file-name">a</div>'
        '<div class="failure-message">Service failed to retrieve this item</div></div>'
        '<div class="file-leaf"><div class="extracted-file-name">b</div>'
        '<div class="failure-message">Kon dit item niet ophalen</div></div>'
        '<div class="failure-message"></div>'  # the empty template node
        '</body></html>'
    )

    def test_counts_non_empty_failure_nodes(self):
        reader, errors, _ = _reader_for({"Takeout/archive_browser.html": self.MANIFEST})
        assert google._count_failed_files(reader) == 2

    def test_absent_manifest_counts_zero(self):
        reader, errors, _ = _reader_for({"Takeout/News/articles.txt": "x"})
        assert google._count_failed_files(reader) == 0

    def test_malformed_manifest_never_raises(self):
        reader, errors, _ = _reader_for({"Takeout/archive_browser.html": "\x00not html at all"})
        assert isinstance(google._count_failed_files(reader), int)


# ---------------------------------------------------------------------------
# Advisor fix wave (2026-08-31): containment. run_extraction (table_extractor.py)
# has no per-extractor try/except of its own — a table config runs each
# extractor in one plain loop, so an uncaught exception from any single table
# used to abort every table behind it, not just its own. Each extractor is
# therefore responsible for containing its own failures; these pin the three
# escape paths the advisor found still open.
# ---------------------------------------------------------------------------


class TestActivityShapeContainment:
    """The watch/search-history extractors filter the activity list by
    ``titleUrl`` before entering their ``try`` block — a source that parses
    to valid JSON but the wrong shape (a JSON object, say, instead of a
    list) used to raise ``AttributeError``/``TypeError`` straight out of the
    extractor. ``youtube_watch_history_to_df`` stands in as the family
    representative; every ``_read_activity``-based extractor shares the same
    guard."""

    def test_non_list_activity_json_is_counted_and_contained(self):
        reader, errors, ddp_locale = _reader_for({
            "Takeout/YouTube and YouTube Music/history/watch-history.json": '{"not": "a list"}',
        })

        df = google.youtube_watch_history_to_df(reader, errors, ddp_locale)

        assert df.empty
        assert errors["UnexpectedActivityShape"] == 1

    def test_a_malformed_activity_source_does_not_stop_other_tables(self, monkeypatch):
        """The containment point: with a full 12-table config (real
        extractors, via ``_real_extractor_config``, monkeypatching
        ``load_port_config`` per the spec §8 config-independence rule), a
        malformed watch-history source must not stop ``chrome_history_to_df``
        — a wholly separate table wired right after it in the registry —
        from extracting normally through ``extraction()``."""
        monkeypatch.setattr(
            google, "load_port_config", lambda registry, platform: _real_extractor_config()
        )
        archive_set = make_set({
            "Takeout/YouTube and YouTube Music/history/watch-history.json": '{"not": "a list"}',
            "Takeout/My Activity/Chrome/MyActivity.json": CHROME_JSON,
        })
        validation = google.validate_ddp(archive_set)

        result = google.extraction(archive_set, validation)

        assert result.errors["UnexpectedActivityShape"] == 1
        assert "chrome_history" in [t.id for t in result.tables]


class TestYoutubeSubscriptionsMalformed:
    """``df.columns = [...]`` assigns positionally and unconditionally — a
    locale/export variant with the wrong column count either raises (too
    few/many names for the frame) or silently mislabels (same count, wrong
    meaning). Localized-header name-mapping stays out of scope (PENDING
    limitation); this only adds containment around the count mismatch."""

    def test_unexpected_column_count_is_counted_and_contained(self):
        reader, errors, ddp_locale = _reader_for({
            "Takeout/YouTube and YouTube Music/subscriptions/subscriptions.csv":
                "A,B,C,D\n1,2,3,4\n",
        })

        df = google.youtube_subscriptions_to_df(reader, errors, ddp_locale)

        assert df.empty
        assert errors["UnexpectedSubscriptionsColumnCount"] == 1

    def test_a_malformed_subscriptions_source_does_not_stop_other_tables(self, monkeypatch):
        monkeypatch.setattr(
            google, "load_port_config", lambda registry, platform: _real_extractor_config()
        )
        archive_set = make_set({
            "Takeout/YouTube and YouTube Music/subscriptions/subscriptions.csv":
                "A,B,C,D\n1,2,3,4\n",
            "Takeout/YouTube and YouTube Music/comments/comments.csv": COMMENTS_CSV,
        })
        validation = google.validate_ddp(archive_set)

        result = google.extraction(archive_set, validation)

        assert result.errors["UnexpectedSubscriptionsColumnCount"] == 1
        assert "youtube_comments" in [t.id for t in result.tables]


class TestYoutubeCommentsMalformed:
    """The rename/select/parse body has no ``try`` of its own (unlike every
    other extractor's ``for item in d`` block) — anything that goes wrong
    reshaping a locale's comments export currently raises straight out.
    Forces the failure via a monkeypatch of ``_parse_comment_text`` (no
    combination of csv.DictReader-parseable content reproduces a raise with
    the pinned pandas/csv stack) to pin the containment contract regardless
    of which future shape trips it."""

    def test_a_failure_in_the_rename_path_is_counted_and_contained(self, monkeypatch):
        def _boom(raw):
            raise ValueError("unexpected comment shape")

        monkeypatch.setattr(google, "_parse_comment_text", _boom)

        reader, errors, ddp_locale = _reader_for({
            "Takeout/YouTube and YouTube Music/comments/comments.csv": COMMENTS_CSV,
        })

        df = google.youtube_comments_to_df(reader, errors, ddp_locale)

        assert df.empty
        assert errors["ValueError"] == 1

    def test_a_malformed_comments_source_does_not_stop_other_tables(self, monkeypatch):
        def _boom(raw):
            raise ValueError("unexpected comment shape")

        monkeypatch.setattr(google, "_parse_comment_text", _boom)
        monkeypatch.setattr(
            google, "load_port_config", lambda registry, platform: _real_extractor_config()
        )
        archive_set = make_set({
            "Takeout/YouTube and YouTube Music/comments/comments.csv": COMMENTS_CSV,
            "Takeout/YouTube and YouTube Music/subscriptions/subscriptions.csv": SUBSCRIPTIONS_CSV,
        })
        validation = google.validate_ddp(archive_set)

        result = google.extraction(archive_set, validation)

        assert result.errors["ValueError"] == 1
        assert "youtube_subscriptions" in [t.id for t in result.tables]
