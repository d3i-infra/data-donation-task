---
status: accepted
date: "2026-03-13"
tags:
    - upstream-alignment
    - feldspar
category: Fork governance
applies_to:
    - packages/feldspar/**
priority: default
---

# Don't modify feldspar for study-specific features

## Decision

`packages/feldspar/` tracks upstream (eyra/feldspar via d3i-infra) and is not modified for study-specific features — those are added in `packages/data-collector/`, and a genuine framework fix is upstreamed rather than patched in place.

## Guidance

- A PR that edits `packages/feldspar/` for study-specific behavior is a violation; move the change to `packages/data-collector/` (the UI corollary is the factory/component-placement rule).
- Only a genuine framework improvement belongs in feldspar, and it should be contributed upstream to d3i-infra — a local study patch blocks clean upstream pulls and re-contribution.
- (The branch that added a TextArea component and edited Confirm/DonateButtons inside feldspar for one feature is the violation this prevents.)
