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

`api/props.py` mirrors upstream Eyra prop types; D3I-specific prop dataclasses live in `api/d3i_props.py`.

## Guidance

- Do not add D3I-specific prop/page/prompt dataclasses to `props.py`; put them in `d3i_props.py`.
- Keep `props.py` refreshable from upstream Eyra with minimal conflict resolution.
- `PropsUIPageError` in `props.py` is grandfathered debt, not precedent.
- For renderable D3I props, also update the TypeScript renderer/factory/App registration listed in `companions`.

## Checks

- Review new `PropsUI*`, prompt, or page dataclasses under `port/api/`: classify each as upstream Eyra (`props.py`) or D3I-custom (`d3i_props.py`).
