"""Tests for reading activity records out of the HTML variant of the Google DDP.

Every source of the archive writes the same cell structure, so one parser reads them all:
the activity text and its link in the body-text content cell, the local time of the account
last. The timestamp is the part that varies most — it is written in the date format and
language of the account.
"""
import io

import pytest

from port.platforms import google


#: The section every caption closes with, on why the activity was kept.
WHY = (
    '<b>Why is this here?</b><br> This activity was saved to your Google Account because the '
    'following settings were on:&nbsp;Web &amp; App Activity.&nbsp;You can control these settings '
    '&nbsp;<a href="https://myaccount.google.com/activitycontrols">here</a>.'
)


def activity_html(cell: str, caption: str = f'<b>Products:</b><br> YouTube<br>{WHY}') -> str:
    """Wrap an activity in the cell structure Takeout writes around it."""
    return (
        '<div class="mdl-grid"><div class="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp">'
        '<div class="mdl-grid">'
        '<div class="header-cell mdl-cell mdl-cell--12-col"><p class="mdl-typography--title">YouTube</p></div>'
        '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">' + cell + '</div>'
        '<div class="content-cell mdl-cell mdl-cell--12-col mdl-typography--caption">' + caption + '</div>'
        '</div></div></div>'
    )


def parse(cell: str) -> list[dict]:
    return google._parse_activity_html(io.BytesIO(activity_html(cell).encode()))


def watch_cell(timestamp: str) -> str:
    return (
        'Watched <a href="https://www.youtube.com/watch?v=abc">A video</a><br>'
        f'<a href="https://www.youtube.com/channel/UC1">A channel</a><br>{timestamp}'
    )


def search_cell(timestamp: str) -> str:
    return f'Searched for <a href="https://www.youtube.com/results?search_query=cats">cats</a><br>{timestamp}'


def test_utf8_bytes_without_a_charset_declaration_are_not_mojibaked():
    """Google's activity HTML declares no charset in its head — verified against a
    real German export. lxml's HTML parser defaults to latin-1 when it finds none,
    double-decoding every non-ASCII UTF-8 byte (an 'ä' becomes 'Ã¤', a NBSP becomes
    'Â ') unless the parser is told the encoding explicitly. Takeout's export bytes
    are UTF-8 (empirical)."""
    record = parse(
        'Gesucht nach:&nbsp;<a href="https://www.google.com/search?q=Aktivit%C3%A4t">'
        'Aktivität</a><br>27.08.2026, 20:04:54 MESZ'
    )[0]

    assert "ä" in record["title"]
    assert "Ã" not in record["title"]
    assert "Â" not in record["title"]


TIMESTAMPS = [
    # A 12-hour clock writes no leading zero, so the hour is a single digit before 10.
    ("Aug 17, 2026, 1:14:48 PM CEST", "2026-08-17T13:14:48"),
    ("Aug 15, 2026, 11:39:58 AM CEST", "2026-08-15T11:39:58"),
    ("15 jun 2026, 9:30:41 CEST", "2026-06-15T09:30:41"),
    ("15 mrt 2026, 20:30:41 CET", "2026-03-15T20:30:41"),
]

#: Shapes the conversion reads directly, beyond the ones above.
DIRECT = [
    ("Dec 31, 2026, 12:00:00 AM CET", "2026-12-31T00:00:00"),  # midnight is 12 AM
    ("Jan 1, 2026, 12:30:00 PM CET", "2026-01-01T12:30:00"),   # noon is 12 PM
    ("1 mei 2026, 07:05:00 CEST", "2026-05-01T07:05:00"),
    ("17. Aug. 2026, 22:14:48 MESZ", "2026-08-17T22:14:48"),   # ordinal dots
    ("17 Ağu 2026, 22:14:48 GMT+3", "2026-08-17T22:14:48"),
]

#: A shape none of the fast paths match — no month name, no dot-separated numeric
#: date, no CJK unit markers, no Arabic slashes — so it genuinely hands to
#: dateutil, which reads it as the unambiguous ISO-ish ``Y-M-D H:M:S`` it is.
FALLBACK = [
    ("2026-08-17 22:14:48", "2026-08-17T22:14:48"),
]

#: Fully numeric dotted dates, as the current German export writes them
#: (``27.08.2026, 20:04:54 MESZ``) — day-first in every locale that writes them.
#: ``12.07.2026`` is the ambiguous case dateutil's month-first default gets
#: wrong (day <= 12, so it reads as 2026-12-07 instead of 2026-07-12); the third
#: entry is day-first even though the digits alone would read as a US date.
#: ``17.08.2026, 22:14:48`` carries no timezone abbreviation at all — the fast
#: path matches on the dotted numeric date alone (``NUMERIC_DAY_FIRST`` has no
#: trailing anchor), so a missing zone doesn't push it to dateutil either.
NUMERIC_DAY_FIRST = [
    ("27.08.2026, 20:04:54 MESZ", "2026-08-27T20:04:54"),
    ("12.07.2026, 23:29:21 MESZ", "2026-07-12T23:29:21"),
    ("07.12.2026, 09:00:00 MEZ", "2026-12-07T09:00:00"),
    ("17.08.2026, 22:14:48", "2026-08-17T22:14:48"),
]

#: ``2026年7月30日 00:23:06 CEST`` — how the Chinese export writes a timestamp: CJK
#: unit markers 年/月/日 name year/month/day unambiguously, even though the zh
#: locale writes English action words ("Watched") in the activity text itself.
#: Confirmed 2026-08-31 against tests/ddp/google_set_uu-acct-zh/'s real
#: 观看记录.html (youtube.watch_history) and search-history HTML — every one of
#: 16494 non-empty Timestamp cells across the set matched one of these four
#: digit-count shapes (24-hour clock, no AM/PM marker in this locale).
CJK = [
    ("2026年7月30日 00:23:06 CEST", "2026-07-30T00:23:06"),  # single-digit month, two-digit day
    ("2026年5月9日 01:40:12 CEST", "2026-05-09T01:40:12"),  # single-digit month and day
    ("2025年10月2日 11:40:30 CEST", "2025-10-02T11:40:30"),  # two-digit month, single-digit day
    ("2024年11月27日 17:58:42 CEST", "2024-11-27T17:58:42"),  # two-digit month and day
]

#: ``23‏/07‏/2026، 4:20:22 م CEST`` — how the Arabic export writes a timestamp:
#: Western digits in day/month/year order (day-first — some samples carry a day
#: > 12, so this is unambiguous by construction, the same reasoning as
#: ``NUMERIC_DAY_FIRST``), each numeric field followed by U+200F RIGHT-TO-LEFT
#: MARK, U+060C ARABIC COMMA after the year instead of a Western comma, and a
#: 12-hour clock with the Arabic meridiem letters ص (ARABIC LETTER SAD, "sabah"/
#: morning = AM) and م (ARABIC LETTER MEEM, "masa'"/evening = PM) in place of
#: AM/PM. Confirmed 2026-08-31 against tests/ddp/google_set_uu-acct-ar/'s real
#: activity HTML (نشاطي/YouTube and نشاطي/Search) — every one of 16494
#: non-empty Timestamp cells across the set matched one of these four
#: digit-count/meridiem shapes.
ARABIC = [
    ("23‏/07‏/2026، 4:20:22 م CEST", "2026-07-23T16:20:22"),  # PM, single-digit hour
    ("30‏/07‏/2026، 12:23:06 ص CEST", "2026-07-30T00:23:06"),  # 12 AM is midnight
    ("20‏/07‏/2026، 12:16:30 م CEST", "2026-07-20T12:16:30"),  # 12 PM is noon
    ("28‏/05‏/2026، 8:28:13 ص CEST", "2026-05-28T08:28:13"),  # AM, single-digit hour
]


@pytest.mark.parametrize(
    "timestamp,expected",
    TIMESTAMPS + DIRECT + FALLBACK + NUMERIC_DAY_FIRST + CJK + ARABIC,
)
def test_conversion(timestamp, expected):
    assert google._convert_to_iso8601(timestamp) == expected


@pytest.mark.parametrize("timestamp,_", TIMESTAMPS + DIRECT)
def test_conversion_agrees_with_dateutil(timestamp, _):
    """The shapes read directly are the ones dateutil is bypassed for, so they have to
    come out the same — except where dateutil cannot read them at all, as with Turkish."""
    converted = google._convert_with_dateutil(timestamp)

    if converted != timestamp:
        assert google._convert_to_iso8601(timestamp) == converted


class TestCaption:
    """Some sources record lists beside an activity — the locations a Discover card was
    picked for, the topics it covered — which the html writes into the caption cell. They
    have to come out in the shape the json format writes them in."""

    LOCATIONS = (
        '<b>Locations:</b><br> At <a href="https://www.google.com/maps/@?api=1&amp;'
        'map_action=map&amp;center=10.000000,20.000000&amp;zoom=12">this general area</a>'
        ' - Based on your past activity<br>'
        ' At <a href="https://www.google.com/maps/@?api=1&amp;map_action=map&amp;'
        'center=11.000000,21.000000&amp;zoom=8">this general area</a> - From your device<br>'
    )
    DETAILS = '<b>Details:</b><br> Birdwatching<br> Cycling - viewed<br> Nordic cuisine<br>'
    CARD = '9 cards in your feed<br>Aug 6, 2026, 4:39:33 PM CEST<br>'

    def record(self, caption: str) -> dict:
        page = activity_html(self.CARD, f'<b>Products:</b><br> Discover<br>{caption}{WHY}')
        return google._parse_activity_html(io.BytesIO(page.encode()))[0]

    def test_locations_and_details_read_as_the_json_writes_them(self):
        record = self.record(self.LOCATIONS + self.DETAILS)

        assert record["locationInfos"] == [
            {
                "name": "At this general area",
                "url": "https://www.google.com/maps/@?api=1&map_action=map&center=10.000000,20.000000&zoom=12",
                "source": "Based on your past activity",
            },
            {
                "name": "At this general area",
                "url": "https://www.google.com/maps/@?api=1&map_action=map&center=11.000000,21.000000&zoom=8",
                "source": "From your device",
            },
        ]
        assert record["details"] == [
            {"name": "Birdwatching"}, {"name": "Cycling - viewed"}, {"name": "Nordic cuisine"}
        ]

    def test_a_detail_that_links_somewhere_keeps_the_link_in_its_text(self):
        """The html writes such a detail as one line, the name and the url it points to
        behind a colon, and the whole line is what the record carries."""
        caption = ('<b>Details:</b><br> Tried to open in app: '
                   '<a href="https://example.org/groups/abc">https://example.org/groups/abc</a><br>')

        assert self.record(caption)["details"] == [
            {"name": "Tried to open in app: https://example.org/groups/abc"}
        ]

    def test_a_dash_inside_a_detail_is_left_alone(self):
        """Only a location separates its source off the end of the line."""
        assert {"name": "Cycling - viewed"} in self.record(self.DETAILS)["details"]

    def test_each_list_stands_on_its_own(self):
        assert "details" not in self.record(self.LOCATIONS)
        assert "locationInfos" not in self.record(self.DETAILS)

    def test_a_location_without_a_source_carries_none(self):
        caption = ('<b>Locations:</b><br> <a href="https://www.google.com/maps/@?api=1&amp;'
                   'center=10.000000,20.000000">Somewhere</a><br>')

        assert self.record(caption)["locationInfos"] == [
            {"name": "Somewhere", "url": "https://www.google.com/maps/@?api=1&center=10.000000,20.000000"}
        ]

    def test_a_caption_with_nothing_to_add_adds_nothing(self):
        """Most captions only name the product and say why the activity was kept."""
        record = google._parse_activity_html(io.BytesIO(activity_html(self.CARD).encode()))[0]

        assert sorted(record) == ["time", "title", "titleUrl"]


class TestMicroseconds:
    """The Chrome history writes its timestamps as a number of microseconds since the
    epoch, which the shared ``epoch_to_iso`` reads as seconds and overflows on."""

    def test_a_microsecond_timestamp_reads_as_a_time(self):
        assert google._convert_usec_to_iso8601(1787225185379660) == "2026-08-20T11:26:25"

    def test_a_number_written_as_text_reads_the_same(self):
        assert google._convert_usec_to_iso8601("1787225185379660") == "2026-08-20T11:26:25"

    def test_the_shape_matches_the_activity_timestamps(self):
        """One column holds timestamps from both, so they are written the same way."""
        from_html = google._convert_to_iso8601("Aug 20, 2026, 11:26:25 AM CEST")

        assert len(google._convert_usec_to_iso8601(1787225185379660)) == len(from_html)

    @pytest.mark.parametrize("timestamp", ["", "not a number", None])
    def test_what_is_not_a_number_is_left_as_it_was(self, timestamp):
        assert google._convert_usec_to_iso8601(timestamp) == timestamp


@pytest.mark.parametrize("timestamp,expected", TIMESTAMPS)
@pytest.mark.parametrize("cell", [watch_cell, search_cell], ids=["watched", "searched"])
def test_timestamp(cell, timestamp, expected):
    assert parse(cell(timestamp))[0]["time"] == expected


class TestRecord:
    def test_a_view_reads_like_its_json_counterpart(self):
        """The json format writes the action into the title and links to the video, so
        the html format has to produce the same record for the same activity."""
        record = parse(watch_cell("15 jun 2026, 20:30:41 CEST"))[0]

        assert record["title"] == "Watched A video"
        assert record["titleUrl"] == "https://www.youtube.com/watch?v=abc"

    def test_details_after_the_activity_stay_out_of_the_title(self):
        """The channel of a video follows the first line break, as further details do for
        every source."""
        record = parse(watch_cell("15 jun 2026, 20:30:41 CEST"))[0]

        assert "A channel" not in record["title"]

    def test_the_line_under_the_activity_reads_as_a_subtitle(self):
        """The json format writes the channel of a video as a subtitle of a name and a
        url, and the html format has to produce the same record for the same activity."""
        record = parse(watch_cell("15 jun 2026, 20:30:41 CEST"))[0]

        assert record["subtitles"] == [
            {"name": "A channel", "url": "https://www.youtube.com/channel/UC1"}
        ]
        assert "description" not in record

    def test_a_line_that_links_nowhere_is_a_description(self):
        """A view from an ad carries the time it was watched at, which the json writes as
        the description of the activity rather than as a subtitle of it."""
        record = parse(
            'Watched <a href="https://www.youtube.com/watch?v=abc">An advert</a><br>'
            'Watched at 11:39 AM<br>15 aug 2026, 11:39:42 CEST'
        )[0]

        assert record["description"] == "Watched at 11:39 AM"
        assert "subtitles" not in record

    def test_an_activity_with_nothing_under_it_carries_neither(self):
        record = parse(search_cell("15 jun 2026, 20:30:41 CEST"))[0]

        assert "subtitles" not in record
        assert "description" not in record

    def test_a_search_keeps_its_query(self):
        record = parse(search_cell("15 jun 2026, 20:30:41 CEST"))[0]

        assert record["title"] == "Searched for cats"
        assert record["titleUrl"] == "https://www.youtube.com/results?search_query=cats"

    def test_a_redirected_link_reads_as_its_destination(self):
        """Activities that leave Google are recorded as a redirect through it."""
        record = parse(
            'Visited <a href="https://www.google.com/url?q=https://example.org/page">Example</a><br>'
            '15 jun 2026, 20:30:41 CEST'
        )[0]

        assert record["titleUrl"] == "https://example.org/page"

    def test_captions_are_not_activities(self):
        """Only the body-text cell holds an activity; the caption cell beside it lists the
        products the record belongs to."""
        assert len(parse(watch_cell("15 jun 2026, 20:30:41 CEST"))) == 1

    def test_the_empty_cell_beside_an_activity_is_not_a_record(self):
        """The layout puts a second, empty body cell beside the activity, and the markup
        around it does not close all of its tags."""
        sample = (
            '<div class="mdl-grid"><div<div class="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp">'
            '<div class="mdl-grid"><div class="header-cell mdl-cell mdl-cell--12-col">'
            '<p class="mdl-typography--title">Search<br></p></div>'
            '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">Visited&nbsp;'
            '<a href="https://example.org/a-page">An example page - Example</a><br>'
            'Aug 16, 2026, 5:42:07 PM CEST<br></div>'
            '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1 '
            'mdl-typography--text-right"></div>'
            '<div class="content-cell mdl-cell mdl-cell--12-col mdl-typography--caption">'
            '<b>Products:</b><br> Search<br><b>Why is this here?</b><br> This activity was saved to '
            'your Google Account because the following settings were on:&nbsp;Web &amp; App Activity.'
            '</div></div></div<div></div>'
        )

        records = google._parse_activity_html(io.BytesIO(sample.encode()))

        assert records == [{
            "title": "Visited An example page - Example",
            "titleUrl": "https://example.org/a-page",
            "time": "2026-08-16T17:42:07",
        }]

    def test_an_activity_without_a_link_reads_as_an_empty_url(self):
        record = parse('Watched a video that has been removed<br>15 jun 2026, 20:30:41 CEST')[0]

        assert record["titleUrl"] == ""
        assert record["title"] == "Watched a video that has been removed"
