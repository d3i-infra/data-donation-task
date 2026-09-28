---
status: accepted
date: "2026-09-24"
category: Extraction
applies_to:
    - packages/python/port/helpers/extraction_helpers.py
    - packages/python/port/api/d3i_props.py
    - packages/python/port/platforms/**/*.py
    - packages/data-collector/src/components/consent_form_viz/consent_form_viz.tsx
    - packages/data-collector/src/components/consent_form_viz/table.tsx
    - packages/python/tests/test_chrome_history.py
    - packages/python/tests/test_instagram_raw_timestamps.py
    - packages/python/tests/test_chatgpt_raw_timestamps.py
    - packages/python/tests/test_youtube_raw_timestamps.py
    - packages/python/tests/test_google_timestamps.py
priority: invariant
companions:
    - packages/python/tests/test_port_helpers.py
checks:
    - desc: timestamp realization only - no timestamp parsing inside an extractor; the whatsapp.py carve-out is a known violation and lapses when convert_to_iso8601 goes
      grep: 'pd\.to_datetime\(|pd\.Timestamp\(|fromisoformat\(|fromtimestamp\(|strptime\(|parser\.parse\('
      in: ["packages/python/port/platforms/**/*.py"]
      except: ["**/whatsapp.py"]
      expect: absent
---

# Donate the export's own value, never our reading of it

## Decision

An extractor writes a cell as the export's own value in its own type: a JSON number stays a number, exported text stays that text, and an absent value is the empty string. A reading of the cell may change how a row is shown, sorted or grouped; it never rewrites the cell, anywhere before the donated file.

## Guidance

- Timestamps are the realization enforced today. A timestamp the export may carry as a JSON number is read with `find_item_raw` or `raw_timestamp` (`extraction_helpers.py`), never `find_item`, which stringifies (a text timestamp, as in `x.py`, loses nothing through `find_item`). A missing key or JSON `null` becomes `""`, never `None` (a bare `None` beside ints upcasts the column to float64 and loses digits) and never via `... or ""` (which erases an epoch of `0`). Review rejects any `pd.to_datetime`, `pd.Timestamp`, `strptime`, `fromisoformat`, `fromtimestamp` or `dateutil` call on a cell in `port/platforms/`.
- Order and cap on a side key, not the cell: `pd.to_numeric(col, errors="coerce")` sorts newest first with empties last (`chrome.py` `browser_history_to_df`, `instagram.py` `_sort_by_date`); `chatgpt.py` `_time_sort_key` sorts a raw `create_time` that may be a number, `""` or garbage without raising. Rendered clock text (`instagram.py` `_html_timestamp`) is stripped and kept as read.
- The consent page may show an interpretation in the cell's place, with the raw `cell` in its tooltip (`table.tsx` `Cell` renders `display ?? cell`; an `empty` cell is blank); `serializeRow` in `consent_form_viz.tsx` donates `row.cells`, never display text. Interpreting the cell (encoding, zone, status) is the front end's `interpretTimestamp` job, driven by the column's `date_columns` entry: an extractor that adds a date column declares it there in the same change.
- What the donation path does today: Python hands the frame over as `to_json()` (`d3i_props.py` `translate_data_frame`), types intact; then `rowCell` in `consent_form_viz.tsx` applies `String(...)` to every cell when the table is parsed, so the donated JSON carries every value as a string. The digits survive; the JSON type does not, and a `null` that reaches the front end donates as the text `"null"`. Do not widen this (no formatting, rounding or re-parsing on the way out); narrowing it toward the export's own types is welcome.
- Known violations, each to be removed; until then nothing is added to this list. `whatsapp.py` `convert_to_iso8601` re-parses each chat line's date into an ISO string (the `checks` grep carves the file out only until that converter goes). `instagram.py` passes most name and text fields (about 25 call sites: `following_to_df`, `_extract_owner_details`, the comment, like and post extractors) through `eh.fix_latin1_string`, re-encoding latin-1 mojibake; whether that rewrites the export's value or decodes it correctly is open. `google.py` and `youtube.py` `_parse_comment_text` join a comment's JSON text segments into one string. Placeholders stand in for absent or unhandled values: `"No hashtags"` in `instagram.py`, `"[Unhandled content type: …]"` in `chatgpt.py`. `youtube.py` `_parse_watch_history_html` and `_parse_search_history_html` pick fields out of the markup by regex. Column renames (`rename(columns=…)`) and key coalescing (`tiktok.py` `_item_get`) are not rewrites.

## Why

"A participant needs to see exactly what they are donating and we need to receive the most raw format that we receive from the donation. If we want to also show the participant the parsed date, we can (and when the date is an epoch or microseconds, we probably should), but we should not take on responsibility for being so sure of our parsing mechanism that we save only that value." Weaken this and a wrong reading (a misjudged zone, a dropped microsecond digit, an epoch of `0` collapsed to missing) is baked into the donated dataset with the evidence gone, and the participant consents to something other than what is received.
