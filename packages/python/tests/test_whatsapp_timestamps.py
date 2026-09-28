# Synthetic chat lines only (ADR-0014). Ported from dd-education 67e2c70.
import pytest
from port.platforms import whatsapp as W

LINES = [
    ("22-07-2023 6:50 ’s avonds - Anna Example: Hey", "2023-07-22T18:50:00", "Anna Example"),
    ("22-07-2023 7:15 ’s ochtends - Anna Example: Hey", "2023-07-22T07:15:00", "Anna Example"),
    ("22-07-2023 12:05 ’s middags - Anna Example: Hey", "2023-07-22T12:05:00", "Anna Example"),
    ("22-07-2023 12:30 ’s nachts - Anna Example: Hey", "2023-07-22T00:30:00", "Anna Example"),
    ("22-07-2023 3:00 ’s middags - Anna Example: Hey", "2023-07-22T15:00:00", "Anna Example"),
    ("[22/07/2023, 6:50:12 PM] Anna: Hey", "2023-07-22T18:50:00", "Anna"),
    ("22/07/2023, 18:50 - Anna: Hey", "2023-07-22T18:50:00", "Anna"),
]


@pytest.mark.parametrize("line,iso,name", LINES)
def test_line_parses_with_correct_hour(line, iso, name):
    regexes = W.generate_regexes(W.SIMPLIFIED_REGEXES)
    for r in regexes:
        dp = W.create_data_point_from_chat(line, r)
        if dp["name"]:
            break
    assert dp["name"] == name
    assert dp["date"].startswith(iso[:16]), dp["date"]


@pytest.mark.parametrize("hour,ampm,daypart,expected", [
    (6, None, "’s avonds", 18), (12, None, "’s middags", 12), (12, None, "’s nachts", 0),
    (6, "PM", None, 18), (12, "AM", None, 0), (18, None, None, 18),
])
def test_to_24h(hour, ampm, daypart, expected):
    assert W.to_24h(hour, ampm, daypart) == expected
