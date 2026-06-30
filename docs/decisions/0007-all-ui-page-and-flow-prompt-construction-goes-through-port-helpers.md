---
status: accepted
date: "2026-03-13"
tags:
    - ui-construction
    - helpers
category: Python architecture
applies_to:
    - packages/python/port/helpers/**/*.py
    - packages/python/port/script.py
    - packages/python/port/platforms/**/*.py
    - packages/python/port/main.py
priority: default
excludes:
    - packages/python/port/helpers/port_helpers.py
---

# All UI page and flow-prompt construction goes through port_helpers

## Decision

Participant-facing page and flow-prompt assembly lives in `helpers/port_helpers.py`. Flow code chooses what to show; `port_helpers` builds the `CommandUIRender`, `PropsUIPage*`, and prompt objects.

## Guidance

- Do not assemble participant-facing pages or flow prompts outside `helpers/port_helpers.py`; call a `port_helpers` renderer/builder instead.
- Platform extraction may still construct `PropsUIPromptConsentFormTableViz` values, because those are extracted-table payloads, not page construction.
- `main.py:error_flow()` is the existing direct-construction exception for the consent-gated error page; do not copy that pattern elsewhere.

## Checks

- Flag `CommandUIRender(` and `PropsUIPage*(` outside `helpers/port_helpers.py`.
- Review `PropsUIPrompt*(` outside `port_helpers.py`, allowing `PropsUIPromptConsentFormTableViz` inside platform extraction functions.
