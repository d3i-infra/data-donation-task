---
status: accepted
date: "2026-03-13"
tags:
    - props
    - upstream-alignment
    - types
category: Python architecture
applies_to:
    - packages/python/port/api/props.py
    - packages/python/port/api/d3i_props.py
priority: default
companions:
    - packages/data-collector/src/components/**
    - packages/data-collector/src/factories/**
    - packages/data-collector/src/App.tsx
---

# Separate upstream props from D3I-custom props

## Decision

`api/props.py` mirrors upstream Eyra prop types; D3I-specific prop dataclasses live in `api/d3i_props.py`. A prop type that is not an upstream Eyra type goes in `d3i_props.py`, never in `props.py`.

## Guidance

- Do not add D3I-specific prop/page/prompt dataclasses to `props.py`; put them in `d3i_props.py`.
- Keep `props.py` refreshable from upstream Eyra with minimal conflict resolution.
- `PropsUIPageError` is a D3I error-page body emitted by `py_worker.js` and rendered by `ErrorPageFactory` — a TS/string-contract type, not a Python prop; its leftover, unused dataclass in `props.py` is removable debt, not precedent.
- For a renderable D3I prop, also register its TypeScript renderer/factory in `data-collector` (`App.tsx` + the factory) — see companions.

## Why

`props.py` mirrors Eyra's upstream prop types so it can be refreshed by dropping in the upstream version with no conflict resolution; any D3I type added there collides on every upstream sync and blurs which types are the fork's. Splitting D3I additions into `d3i_props.py` keeps the mirror clean and makes the full set of fork-specific types auditable in one file. The lone D3I name left in `props.py`, `PropsUIPageError`, is an unused dataclass whose real form is a TS-side error body emitted by `py_worker.js` — removable debt, not a precedent for adding D3I types here.

## Checks

- Review new `PropsUI*`, prompt, or page dataclasses under `port/api/`: classify each as upstream Eyra (`props.py`) or D3I-custom (`d3i_props.py`).
