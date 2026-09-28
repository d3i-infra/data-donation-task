---
status: accepted
date: "2026-09-24"
category: Data collector
applies_to:
    - packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/interpretTimestamp.ts
    - packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/util.ts
    - packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/prepareChartData.ts
    - packages/data-collector/src/components/consent_form_viz/table.tsx
    - packages/data-collector/src/components/consent_form_viz/visualization_plugin/types.ts
    - packages/python/port/helpers/port_config_validator.py
    - packages/python/port/configs/**/*_config.json
priority: default
companions:
    - packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/interpretTimestamp.test.ts
    - packages/python/tests/test_date_encodings_match_typescript.py
    - packages/python/tests/test_extractor_integration_google.py
    - packages/python/tests/test_extractor_integration_facebook.py
checks:
    - desc: Date construction only in the interpreter and its formatters (whole consent-viz tree)
      grep: 'new Date\(|Date\.parse\('
      in: ["packages/data-collector/src/components/consent_form_viz/**"]
      except: ["**/util.ts", "**/interpretTimestamp.ts", "**/*.test.ts"]
      expect: absent
---

# Interpret timestamps once, in the front end, with a status

## Decision

A raw timestamp cell is interpreted in exactly one place, the front end's `interpretTimestamp`, under the encoding its column declares in `date_columns`. It yields an instant, a local wall clock, or a status (`empty`, `invalid`, `ambiguous`, `unsupported`) that every cell display and chart bucket shows or counts rather than guesses around.

## Guidance

- Never `new Date(<string>)` or `Date.parse` on a cell; `interpretTimestamp` (`interpretTimestamp.ts`) is the only parser, and `table.tsx` `cellDisplay` and `prepareChartData.ts` `prepareX` both go through it.
- A timestamp column is declared, never parsed: its entry in the extractor's `Table config::` block, `"date_columns": {<column>: {"encoding": <one of, or a list of, epoch-seconds | epoch-micros | iso-8601 | tiktok | takeout-html | meta-html>, "utcOffsetMinutes"?: int}}`, is what the interpreter runs under.
- Statuses: blank, `none`, `null` and `nan` are `empty`. Each encoding's parser yields nothing when the shape does not match and `invalid` when it matches but the calendar or epoch range rejects it. Across a list of encodings exactly one may match (an `invalid` counts as a match): none is `unsupported`, two is `ambiguous`. An undeclared column has no encodings: in a chart every value is `unsupported` and unplotted; in the table `cellDisplay` is undefined for it and the raw text shows.
- Zones are declared or carried, never guessed: a value with its own marker (ISO `Z` or offset; a Takeout token in `ZONE_OFFSET_MINUTES` or `GMT+hh:mm`) is an instant on that marker, which wins over `utcOffsetMinutes`; a naive value is an instant only when its encoding fixes the zone (`tiktok` is UTC) or the column declares `utcOffsetMinutes` (`meta-html`, naive `iso-8601`); otherwise it is `local`, as is an unknown zone abbreviation. Month names come from `Intl` via `monthTable`: `table.dateLocale` (stamped by the platform's extraction, `youtube.py`, `google.py`) for `takeout-html`, `en` for `meta-html`.
- Consumers: `prepareX` buckets an instant through `wallClockInZone` in `table.displayTimezone ?? 'Europe/Amsterdam'` (`platform_info.timezone`, stamped by `FlowBuilder.generate_review_data_prompt`), a `local` as its own wall clock, and drops the other statuses, counting the non-empty ones as `unplotted`. `displayTimestamp` (`util.ts`) formats an instant in that zone and a local as-is, in the UI locale, with one cached formatter per locale and zone; it returns `''` for `empty` and `null` otherwise, so `Cell` shows the raw text.
- `port_config_validator.validate` rejects a `date_columns` key that is not a header, an unknown encoding, an empty encoding list, a non-integer `utcOffsetMinutes`, a non-IANA `platform_info.timezone`, and a chart grouped by `dateFormat` on an undeclared column (it would silently plot nothing). `DATE_ENCODINGS` there and `zDateEncoding` (`visualization_plugin/types.ts`) stay identical; a new encoding is a parser in `interpretTimestamp.ts`, both lists, and cases in `interpretTimestamp.test.ts`.
- Declarations live in the docstring: delete `configs/<platform>_config.json` and run `pnpm generate-config <platform>` (`generate` refuses to overwrite). `test_declared_date_columns_are_populated` checks each declared Google column exists and is populated.
- A `meta-html` column with no declared `utcOffsetMinutes` is Facebook's case: the rendered clock text carries no zone marker, and the account's own zone (named elsewhere in the export, e.g. `logged_information/location/timezone.html`) is not carried into the declaration — so the clock stays `local`, the account's own wall clock, never guessed at an instant. Instagram's export always renders one fixed zone, so its `meta-html` columns declare `utcOffsetMinutes: -480`. Whether a platform's HTML export carries a zone into its declared columns is a fact about that export, not a default to change per column.
- The `checks` grep bans every `new Date(` and `Date.parse(` in the consent viz tree outside `util.ts`, `interpretTimestamp.ts` and the tests, `new Date(epochMs)` included: stricter than the rule, and kept as the tripwire. A new display formatter goes in `util.ts`.

## Why

Interpretation is a claim about the export and must stay revisable without touching the evidence; one implementation serving cells and charts cannot disagree with itself; a status makes uncertainty explicit instead of silently choosing a reading.
