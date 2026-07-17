---
status: proposed
date: "2026-07-16"
category: Data collector
applies_to:
    - packages/data-collector/src/components/consent_form_viz/consent_form_viz.tsx
    - packages/data-collector/src/components/consent_form_viz/table_container.tsx
priority: invariant
---

# Consent-page memory work must never shrink the donated dataset

## Decision

Memory and display optimizations on the consent-viz page must never reduce the donated dataset: the payload built by `serializeConsentData()` is exactly the parsed tables minus the participant's own explicit deletions. Any future row truncation must be display-only.

## Guidance

- When cutting consent-page memory, trim only transient copies (worker messages via `selectVisualizationColumns`, display windows) — `serializeConsentData()` must keep serializing every non-deleted row of every table.
- Review rejects any row cap (e.g. a `MAX_ROWS`-style bound) applied to the `tables` state, `originalBody`, or the serialized payload; display pagination over the full data is the fix path.
- Participant-initiated deletion (delete/undo in `table_container.tsx`) is the only legitimate dataset reduction.
- This record is `proposed` — team agreement on the invariant (and on whether any display-side bound may ever interact with the donated payload) is still pending.

## Why

A silent cap turns "donate your data" into "donate a sample" without the participant or researcher knowing — it corrupts research-data integrity to save memory (issue #122 advisor review).
