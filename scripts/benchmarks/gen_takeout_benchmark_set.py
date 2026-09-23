#!/usr/bin/env python3
"""Generates an en-locale, Google-recognizable 3-part Takeout benchmark set for
scripts/benchmarks/memtest-v3-peak.cjs (see the README's "Multi-file (Google
Takeout) benchmark" section).

The three parts mirror the shape a real, sizeable Takeout export arrives in —
the case ADR-0034's ~824 MB reference budget and the streaming HTML parser
(``port.platforms.google._parse_activity_html``) exist for. The streaming
parser only bounds the input side (the file streams from the zip member and
the parse tree is cleared per record); the parsed rows, tables, consent
transport, and donation serialization it feeds all scale with row count and
stay unbounded — this benchmark's 300 MB / ~400k-row activity member is
what actually stresses the budget. See ADR-0034 for the measured peaks.

- part 1 (``...-1-001.zip``): a single legitimately-huge member — the
  YouTube watch history, sized by ``--activity-mb`` (default 300) — the
  "one massive file inside an otherwise normal zip" case the 512 MiB
  per-member guard (``MAX_MEMBER_UNCOMPRESSED_BYTES``) passes and an iOS
  WebView must survive without ballooning.
- part 2 (``...-2-001.zip``): a mid-sized My Activity/Search export plus the
  two small YouTube CSVs (subscriptions, comments).
- part 3 (``...-3-001.zip``): manifest-only — Takeout's own
  ``archive_browser.html`` "see report" page, carrying two non-empty
  Failed-Files entries so ``google._count_failed_files`` has something to
  count.

Every activity record reuses the exact MDL cell structure
``test_google_timestamps.activity_html()`` wraps a record in (one
``outer-cell`` per record — see ``port.platforms.google._parse_activity_html``'s
docstring for why this is the one shape every Takeout activity source
writes), with distinct fake video/channel ids per record and timestamps in
the English "month first" shape (``Aug 17, 2026, 1:14:48 PM CEST`` —
``google.MONTH_FIRST``) so every record's ``Timestamp`` column round-trips
to ISO 8601. Deliberately no ``<meta charset>`` in the generated HTML's
``<head>`` — real Takeout activity pages declare none either (see
``_parse_activity_html``'s docstring on why the parser pins UTF-8 itself
rather than trusting lxml's latin-1 default).

The big member is streamed straight into the open zip entry
(``ZipFile.open(zinfo, "w")``), one record at a time — never held whole in
a Python string or list — so generating a 300 MB member costs O(1 record)
of resident memory, the same discipline the extraction-side parser uses on
the read path.

Usage::

    python3 scripts/benchmarks/gen_takeout_benchmark_set.py --out /tmp/bench-set
    python3 scripts/benchmarks/gen_takeout_benchmark_set.py --out /tmp/bench-set --activity-mb 5
"""
import argparse
import csv
import io
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

#: English month abbreviations, spelled out rather than pulled from
#: ``datetime.strftime("%b")`` so the generated timestamps never depend on
#: the machine's locale.
MONTHS_ABBR = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

#: The caption every record's "why is this here" section closes with —
#: copied verbatim from ``test_google_timestamps.WHY`` so a record parses
#: exactly the way the pinned unit tests prove the real shape does.
WHY = (
    '<b>Why is this here?</b><br> This activity was saved to your Google Account because the '
    'following settings were on:&nbsp;Web &amp; App Activity.&nbsp;You can control these settings '
    '&nbsp;<a href="https://myaccount.google.com/activitycontrols">here</a>.'
)


def _format_timestamp(moment: datetime) -> str:
    """Formats *moment* the way the English Takeout export writes an activity
    timestamp: ``Aug 17, 2026, 1:14:48 PM CEST`` — parseable by
    ``port.platforms.google.MONTH_FIRST`` (a 12-hour clock with no leading
    zero on the hour). The trailing zone abbreviation is fixed at CEST;
    ``_convert_to_iso8601`` ignores it regardless of the archive's real zone,
    same as production."""
    hour12 = moment.hour % 12 or 12
    meridiem = "AM" if moment.hour < 12 else "PM"
    return (
        f"{MONTHS_ABBR[moment.month - 1]} {moment.day}, {moment.year}, "
        f"{hour12}:{moment.minute:02d}:{moment.second:02d} {meridiem} CEST"
    )


def _outer_cell(cell: str, caption: str) -> str:
    """Wraps an activity in the exact MDL ``outer-cell`` block structure
    ``test_google_timestamps.activity_html()`` uses — the shape
    ``google._parse_activity_html`` is written (and pinned by its unit
    tests) to read."""
    return (
        '<div class="mdl-grid"><div class="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp">'
        '<div class="mdl-grid">'
        '<div class="header-cell mdl-cell mdl-cell--12-col"><p class="mdl-typography--title">YouTube</p></div>'
        '<div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">' + cell + '</div>'
        '<div class="content-cell mdl-cell mdl-cell--12-col mdl-typography--caption">' + caption + '</div>'
        '</div></div></div>'
    )


def _watch_record_html(index: int, moment: datetime) -> str:
    """One YouTube watch-history record: a distinct fake video/channel id per
    record, so no two records collide, plus a MONTH_FIRST-parseable
    timestamp."""
    cell = (
        f'Watched <a href="https://www.youtube.com/watch?v=bench{index:08d}">'
        f'Fake Bench Video {index}</a><br>'
        f'<a href="https://www.youtube.com/channel/UCbench{index % 9973:06d}">'
        f'Fake Bench Channel {index % 9973}</a><br>'
        f'{_format_timestamp(moment)}'
    )
    caption = f'<b>Products:</b><br> YouTube<br>{WHY}'
    return _outer_cell(cell, caption)


def _search_record_html(index: int, moment: datetime) -> str:
    """One My Activity/Search record — same cell structure, a search query
    instead of a watch."""
    cell = (
        f'Searched for <a href="https://www.google.com/search?q=benchquery{index}">'
        f'benchquery{index}</a><br>{_format_timestamp(moment)}'
    )
    caption = f'<b>Products:</b><br> Search<br>{WHY}'
    return _outer_cell(cell, caption)


def _stream_activity_html(zf: zipfile.ZipFile, member_path: str, target_bytes: int, record_fn) -> tuple[int, int]:
    """Streams ``record_fn(index, moment)`` blocks straight into an open zip
    entry until *target_bytes* of member content is written, without ever
    holding more than one record's text (and the write buffer beneath it) in
    memory at once — the same bound ``_parse_activity_html`` enforces on the
    read side. Returns (record_count, bytes_written)."""
    zinfo = zipfile.ZipInfo(filename=member_path, date_time=time.localtime()[:6])
    zinfo.compress_type = zipfile.ZIP_STORED
    # Deliberately no <meta charset> — see the module docstring.
    header = b"<!DOCTYPE html><html><head><title>Watch history</title></head><body>"
    footer = b"</body></html>"

    written = 0
    index = 0
    # Recent-past start, ticking backwards a plausible cadence per record so
    # a heavy-user file spans a realistic time range without any record
    # postdating "now".
    moment = datetime.now() - timedelta(days=1)
    step = timedelta(seconds=41)

    with zf.open(zinfo, "w") as dest:
        dest.write(header)
        written += len(header)
        while written < target_bytes:
            block = record_fn(index, moment).encode("utf-8")
            dest.write(block)
            written += len(block)
            index += 1
            moment -= step
        dest.write(footer)
        written += len(footer)

    return index, written


def _subscriptions_csv() -> bytes:
    """A few fake YouTube subscriptions. Columns are read positionally by
    ``youtube_subscriptions_to_df`` (reassigned to Channel Id/URL/Name
    regardless of header text), so the header text itself only needs to be
    present."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Channel Id", "Channel Url", "Channel Title"])
    for i in range(5):
        writer.writerow([
            f"UCbenchsub{i:04d}",
            f"https://www.youtube.com/channel/UCbenchsub{i:04d}",
            f"Fake Bench Subscription {i}",
        ])
    return buf.getvalue().encode("utf-8")


def _comments_csv() -> bytes:
    """A few fake YouTube comments, in the exact column shape
    ``youtube_comments_to_df`` expects (English headers pass through
    unchanged except ``Comment create timestamp`` -> ``Timestamp``); the
    ``Comment text`` cell is a JSON array-of-segments fragment, the shape
    ``_parse_comment_text`` wraps in ``[...]`` and parses."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "Comment ID", "Channel ID", "Comment create timestamp", "Price", "Video ID", "Comment text",
    ])
    base = datetime.now() - timedelta(days=2)
    for i in range(4):
        stamp = (base + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
        writer.writerow([
            f"benchc{i}",
            f"UCbenchsub{i % 5:04d}",
            stamp,
            "0",
            f"bench{i:08d}",
            f'{{"text":"Fake bench comment {i}"}}',
        ])
    return buf.getvalue().encode("utf-8")


def _archive_browser_html() -> bytes:
    """Takeout's own export-status manifest: a handful of ``file-leaf``
    entries, two of them carrying a non-empty ``failure-message`` div — the
    exact structure ``google._count_failed_files`` (and its pinned unit test,
    ``TestFailedFilesDetector``) matches on, counted by CSS class only, never
    by the (localized) message text."""
    leaves = []
    for i in range(5):
        name = f"bench-leaf-{i}.html"
        if i in (1, 3):
            failure = '<div class="failure-message">Service failed to retrieve this item</div>'
        else:
            failure = '<div class="failure-message"></div>'
        leaves.append(
            f'<div class="file-leaf"><div class="extracted-file-name">{name}</div>{failure}</div>'
        )
    # The empty template node real exports carry alongside the per-file ones
    # (see TestFailedFilesDetector.MANIFEST) — proves the detector counts
    # only *non-empty* failure-message nodes.
    leaves.append('<div class="failure-message"></div>')
    body = "".join(leaves)
    html = f"<!DOCTYPE html><html><head><title>Archive browser</title></head><body>{body}</body></html>"
    return html.encode("utf-8")


def generate(out_dir: Path, activity_mb: int) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

    part1 = out_dir / f"takeout-{stamp}-1-001.zip"
    part2 = out_dir / f"takeout-{stamp}-2-001.zip"
    part3 = out_dir / f"takeout-{stamp}-3-001.zip"

    report: dict = {"stamp": stamp}

    with zipfile.ZipFile(part1, "w", zipfile.ZIP_STORED) as zf:
        count, size = _stream_activity_html(
            zf,
            "Takeout/YouTube and YouTube Music/history/watch-history.html",
            activity_mb * 1024 * 1024,
            _watch_record_html,
        )
    report["part1"] = {"path": str(part1), "records": count, "member_bytes": size}

    with zipfile.ZipFile(part2, "w", zipfile.ZIP_STORED) as zf:
        count, size = _stream_activity_html(
            zf,
            "Takeout/My Activity/Search/My Activity.html",
            20 * 1024 * 1024,
            _search_record_html,
        )
        zf.writestr("Takeout/YouTube and YouTube Music/subscriptions/subscriptions.csv", _subscriptions_csv())
        zf.writestr("Takeout/YouTube and YouTube Music/comments/comments.csv", _comments_csv())
    report["part2"] = {"path": str(part2), "records": count, "member_bytes": size}

    with zipfile.ZipFile(part3, "w", zipfile.ZIP_STORED) as zf:
        zf.writestr("Takeout/archive_browser.html", _archive_browser_html())
    report["part3"] = {"path": str(part3)}

    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate an en-locale, Google-recognizable 3-part Takeout benchmark "
            "set for scripts/benchmarks/memtest-v3-peak.cjs."
        ),
    )
    parser.add_argument("--out", required=True, help="Output directory for the three zip parts.")
    parser.add_argument(
        "--activity-mb", type=int, default=300,
        help="Uncompressed size (MB) of part 1's watch-history.html member (default: 300).",
    )
    args = parser.parse_args()

    if args.activity_mb <= 0:
        parser.error("--activity-mb must be positive")

    out_dir = Path(args.out)
    report = generate(out_dir, args.activity_mb)

    print(f"Wrote {out_dir}/ :")
    for key in ("part1", "part2", "part3"):
        info = report[key]
        size_mb = Path(info["path"]).stat().st_size / (1024 * 1024)
        extra = f", {info['records']} records" if "records" in info else ""
        print(f"  {info['path']} ({size_mb:.1f} MB on disk{extra})")
    print(
        "\nRun the benchmark with:\n"
        "  MEMTEST_PLATFORM_LABEL=Google \\\n"
        f"  MEMTEST_ZIP={report['part1']['path']}:{report['part2']['path']}:{report['part3']['path']} \\\n"
        "  node scripts/benchmarks/memtest-v3-peak.cjs"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
