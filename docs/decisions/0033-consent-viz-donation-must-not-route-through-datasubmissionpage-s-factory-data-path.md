---
status: accepted
date: "2026-07-16"
category: Data collector
applies_to:
    - packages/data-collector/src/components/consent_form_viz/consent_form_viz.tsx
priority: default
---

# Consent-viz donation must not route through DataSubmissionPage's factory data path

## Decision

The consent-viz prompt consumes shared feldspar components such as `DonateButtons` by direct import (via the D3I exports block in feldspar's `index.ts`) and resolves its own donation payload. It must not report table data through `DataSubmissionPage`'s `onDataSubmissionDataChanged`/`DonateButtonsFactory` path.

## Guidance

- Wire donate/cancel by passing `handleDonate`/`handleCancel` into a directly imported `DonateButtons` — do not route table data through `DataSubmissionPage`'s `onDataSubmissionDataChanged`/`DonateButtonsFactory` path; the component builds its own `PayloadJSON` from `serializeConsentData()` at click time.
- Review rejects pushing serialized consent tables into `onDataSubmissionDataChanged` or composing the consent-viz page from a separate `PropsUIDataSubmissionButtons` body item, until upstream stops holding a page-lifetime payload copy and double-stringifying at donate.
- Revisit if upstream `data_submission_page.tsx` moves to lazy pull-at-donate and drops its payload `console.log`s (tracked in UPSTREAM_REQUESTS).

## Why

The factory path holds a serialized copy of all table data for the page lifetime and stacks `Object.fromEntries`/`JSON.stringify` copies (plus a log-only stringify) at donate-click — code-verified in `data_submission_page.tsx`; the path itself was never benchmarked. The donate click is empirically the flow's peak-memory moment (the 2026-07-16 baseline measured a +314 MB spike there on the then-current direct path, development build), and the factory path would stack its additional payload-sized copies at exactly that moment — squarely in the iOS kill zone.
