---
status: accepted
date: "2026-07-17"
category: Data collector
applies_to:
    - packages/data-collector/src/components/consent_form_viz/**
priority: default
companions:
    - packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/formatDate.test.ts
checks:
    - desc: no per-call ICU construction via toLocaleString in the per-row viz data pipeline
      grep: 'toLocaleString\('
      in: ["packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/**"]
      expect: absent
    - desc: Intl formatter construction only in util.ts hoisted sites (whole consent-viz tree)
      grep: 'new Intl\.'
      in: ["packages/data-collector/src/components/consent_form_viz/**"]
      except: ["**/util.ts", "**/*.test.ts"]
      expect: absent
    - desc: no zod parsing inside the per-row pipeline (validation happens once at the table boundary)
      grep: '\.safeParse\(|zTable(Row)?\.parse\('
      in: ["packages/data-collector/src/components/consent_form_viz/visualization_plugin/visualizationDataFunctions/**"]
      except: ["**/*.test.ts"]
      expect: absent
---

# Per-row work over participant tables must not allocate

## Decision

Code that runs per table row must not construct heavyweight objects or per-row intermediate structures inside the loop: construct once above the loop and reuse. Prefer bulk/columnar transforms over new per-row closures.

## Guidance

- Anything a row-loop body needs — an `Intl.DateTimeFormat`, a compiled regex, a zod parse, a lookup table — is constructed once above the loop and captured; review rejects any allocation whose count scales with row count. Tables here reach 65k+ rows.
- Formatting rows with `toLocaleString` is per-row ICU construction in disguise; use a hoisted `Intl.DateTimeFormat` as `formatDate` does. The frontmatter grep checks enforce this for the viz data pipeline; keep new `Intl.*` construction sites inside `util.ts` so the carve-out stays narrow.
- Per-row helper-library calls that allocate (e.g. the `_.zip`/`_.fromPairs` pair in `serializeRow`) are the same pattern at donation time — acceptable only where measured, and the first place to look when the donate-phase peak grows.
- A new per-row transform ships with an O(1)-allocation tripwire (extend the construction-count spy pattern in `formatDate.test.ts`).
- The structural end-state is columnar table processing (bulk ops over per-row objects) — tracked as a follow-up; new code should not deepen the row-wise idiom.

## Why

One `Intl.DateTimeFormat` per row measured +1697 MB RSS per chart computation (2026-07-16/17 production profiling: consent flow peaked at 4.9 GB pre-fix, 839 MB after hoisting) — the per-row-allocation class, not the datetime instance, is what keeps killing iPhone donations.
