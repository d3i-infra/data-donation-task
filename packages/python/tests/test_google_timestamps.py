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


@pytest.mark.parametrize("cell", [watch_cell, search_cell], ids=["watched", "searched"])
def test_timestamp_is_kept_as_rendered(cell):
    raw = "15 jun 2026, 20:30:41 CEST"
    assert parse(cell(raw))[0]["time"] == raw


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
            "time": "Aug 16, 2026, 5:42:07 PM CEST",
        }]

    def test_an_activity_without_a_link_reads_as_an_empty_url(self):
        record = parse('Watched a video that has been removed<br>15 jun 2026, 20:30:41 CEST')[0]

        assert record["titleUrl"] == ""
        assert record["title"] == "Watched a video that has been removed"
