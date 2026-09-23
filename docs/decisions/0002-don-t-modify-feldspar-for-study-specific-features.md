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

`packages/feldspar/` tracks upstream `eyra/feldspar` directly and is not modified for D3I- or study-specific features — those are added in `packages/data-collector/` (UI) and `packages/python/` (extraction), and a genuine framework fix is upstreamed rather than patched in place.

## Guidance

- A PR that edits `packages/feldspar/` for D3I- or study-specific behavior is a violation; move the change to `packages/data-collector/` (the UI corollary is the factory/component-placement rule).
- `packages/feldspar/` should remain as close to upstream as possible: local feldspar changes must be limited to framework-level fixes or compatibility, documented, and upstreamed or reconciled with `eyra/feldspar` when feasible.
- On finding **any** divergence from upstream — in `packages/feldspar/`, in the Python prop mirror, or in the mono fork — stop and trace the chain of events that produced it (git history on *both* sides, and the reason given at the time) before changing anything. Then either justify it and record it in an ADR, or delete it. "It has been like that for a while" is not a justification, and neither is "upstream looks wrong"; find out why.
- A justified divergence that is a genuine framework fix or a generic host integration point gets a patch prepared against a current upstream checkout and offered upstream. The fork carries it regardless of whether upstream takes it — but a fix that is never offered is a divergence with no exit, and it is what makes the next sync expensive.
- What goes upstream is the mechanism, never the policy: a policy-free hook (`mapLocale`, `defaultLocale`) is upstreamable, the study's locale set, platform list, or UI conventions are not. Strip fork policy out of a patch before offering it, and re-verify the patch against upstream HEAD before submitting — an upstream that has drifted is a finding to report, not something to paper over.

## Why

`packages/feldspar/` tracks `eyra/feldspar` so the fork can keep pulling upstream improvements; every study-specific patch landed here turns each upstream sync into a merge-conflict resolution and blocks the change from ever being contributed back. Routing customization to `data-collector`/`python` instead keeps feldspar a clean, syncable mirror and makes each D3I addition visible where it belongs.
