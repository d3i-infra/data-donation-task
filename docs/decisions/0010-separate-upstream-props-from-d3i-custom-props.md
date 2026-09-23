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

`api/props.py` mirrors upstream Eyra's prop types; the fork's own go in `api/d3i_props.py`, never in `props.py`. An upstream type may be edited in place only when the change is narrow and upstream-plausible, and every such divergence is inventoried here.

## Guidance

- A new D3I-specific prop/page/prompt dataclass goes in `d3i_props.py`; review rejects one added to `props.py`, and the fix is to move it, not to argue it is close enough to upstream.
- An in-place edit to an existing upstream type is allowed only when it is narrow and upstream-plausible — a docstring or typo fix, an optional `NotRequired` key, a constructor guard: something Eyra would plausibly take, and that keeps existing upstream callers working. A new field with fork semantics, or a new class, is not narrow; it belongs in `d3i_props.py`.
- Record each in-place divergence in this record's inventory in the same change that creates it, and offer the upstream-plausible ones back as a patch (the fork's divergence discipline lives in the don't-modify-feldspar record, ADR-0002). An un-inventoried divergence is what turns the next upstream refresh into archaeology.
- `PropsUIPageError` is a D3I error-page body emitted by `py_worker.js` and rendered by `ErrorPageFactory` — a TS/string-contract type, not a Python prop; its leftover, unused dataclass in `props.py` is removable debt tracked by issue #102, not precedent for adding D3I types here.
- For a renderable D3I prop, also register its TypeScript renderer/factory in `data-collector` (`App.tsx` + the factory) — see companions.

## Context

Current in-place divergences of `props.py` from upstream `eyra/feldspar`, as of 2026-08-07:

- `Translations` gains `de`, `it`, and `es` as `NotRequired` keys (`en`/`nl` stay required), and its docstring is rewritten — upstream's reads "text that is  display in a speficic language". Both are upstream-plausible; offered as a patch.
- `Translatable.__post_init__` guards construction: non-dict `translations`, non-string keys, and non-string values raise `TypeError` at the construction site rather than surfacing to a participant as unusable text. Locale *coverage* is deliberately not checked here — partial bundles are legitimate and coverage is a researcher-facing gate. Upstream-plausible; offered as a patch.
- `PropsUIPromptHelloWorld.toDict` serializes `self.text.toDict()` rather than the raw `Translatable`; upstream's version puts a dataclass into the dict. A bug fix, upstream-plausible.
- `PropsUIPageDataSubmission.body`'s `Union` is widened with `Any` (predates this record's rules), so the fork's own page bodies type-check without being listed. Not upstream-plausible as-is: the honest fix is a protocol/`toDict`-bearing bound, not `Any`.
- `PropsUIPageEnd` is removed and `PropsUIPageError` added — the flow ends by generator exhaustion, so the end page has no caller. Predates this record; the dead `PropsUIPageError` dataclass is issue #102.
- One cosmetic reflow of the `headers` dict comprehension in `PropsUIPromptConsentFormTable.toDict`. Pure noise on an upstream refresh; drop it the next time the file is touched.

## Why

`props.py` mirrors Eyra's upstream prop types so an upstream refresh is a diff a human can read; a D3I type added there collides on every sync and blurs which types are the fork's. Splitting fork additions into `d3i_props.py` keeps the mirror clean and the fork-specific set auditable in one file. The mirror is not frozen, though — refusing to fix a typo or harden a constructor in place would push those changes into a wrapper nobody upstreams, so narrow upstream-plausible edits are allowed *provided they are written down*: the inventory is what makes the next refresh a review of six known lines instead of an excavation. The lone D3I name left in `props.py`, `PropsUIPageError`, is an unused dataclass whose real form is a TS-side error body emitted by `py_worker.js` — removable debt, not a precedent for adding D3I types here.

## Checks

- Review new `PropsUI*`, prompt, or page dataclasses under `port/api/`: classify each as upstream Eyra (`props.py`) or D3I-custom (`d3i_props.py`).
