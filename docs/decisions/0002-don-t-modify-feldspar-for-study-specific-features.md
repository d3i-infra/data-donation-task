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

## Why

`packages/feldspar/` tracks `eyra/feldspar` so the fork can keep pulling upstream improvements; every study-specific patch landed here turns each upstream sync into a merge-conflict resolution and blocks the change from ever being contributed back. Routing customization to `data-collector`/`python` instead keeps feldspar a clean, syncable mirror and makes each D3I addition visible where it belongs.
