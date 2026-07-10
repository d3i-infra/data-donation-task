---
status: accepted
date: "2026-03-13"
tags:
    - ui-construction
    - helpers
    - script
category: Python architecture
applies_to:
    - packages/python/port/helpers/**/*.py
    - packages/python/port/script.py
    - packages/python/port/platforms/**/*.py
    - packages/python/port/main.py
excludes:
    - packages/python/port/helpers/port_helpers.py
priority: default
---

# All UI page and flow-prompt construction goes through port_helpers

## Decision

Participant-facing page and flow-prompt assembly lives in `helpers/port_helpers.py`: flow code chooses *what* to show, and `port_helpers` builds the `CommandUIRender`, `PropsUIPage*`, and `PropsUIPrompt*` objects. `script.py` and platform flows must not construct those objects directly.

## Guidance

- Do not assemble participant-facing pages or flow prompts outside `helpers/port_helpers.py`; call a `port_helpers` renderer/builder instead.
- Platform extraction and `helpers/table_extractor.py` may construct `PropsUIPromptConsentFormTableViz` values — those are extracted-table payloads, not page construction, and stay in the extraction helpers.
- `main.py:error_flow()` still assembles its consent-gated error page inline; that is a known deviation to be moved into a `port_helpers` renderer, not a pattern to copy.

## Why

With ten platforms and multiple flow types, letting any module construct `PropsUI*` objects duplicates page assembly across every flow and blurs "what to show" (flow logic) with "how to build it"; centralizing in `port_helpers` keeps `script.py` readable as a high-level study flow and gives one audit point for page structure. If the rule is weakened, page construction scatters and a single fix must be chased across every platform — so the only standing carve-out is `PropsUIPromptConsentFormTableViz` (an extracted-table payload, not a page), while `main.py:error_flow()`'s inline page is a known deviation to rectify, not a licence to construct elsewhere.

## Checks

- Flag `CommandUIRender(` and `PropsUIPage*(` outside `helpers/port_helpers.py`.
- Review `PropsUIPrompt*(` outside `port_helpers.py`, allowing `PropsUIPromptConsentFormTableViz` in platform extraction and `helpers/table_extractor.py`.
