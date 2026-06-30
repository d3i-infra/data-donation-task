---
status: proposed
date: "2026-03-13"
tags:
    - prompt-components
    - feldspar-compatibility
    - ux
category: Data collector
applies_to:
    - packages/data-collector/src/components/**
    - packages/python/port/helpers/port_helpers.py
priority: default
---

# Prefer standard feldspar prompts; custom only when needed

## Decision

Prefer standard feldspar prompt components (`PropsUIPromptConfirm`, `ConsentForm`, `FileInput`) over D3I custom prompt components; add a custom prompt only when a standard one cannot provide the required UX (e.g. a consent form with data visualizations).

## Guidance

- Reach for a standard feldspar prompt first — it works on any feldspar host with no custom factory and inherits upstream improvements.
- Add a D3I custom prompt only when the UX genuinely can't be met by a standard one; a custom prompt needs a factory registered in data-collector and carries maintenance and bug risk (e.g. the single-button `PropsUIPromptRetry` defect, exposed once FlowBuilder actually checked the retry response).
- This record is `proposed`, not yet settled — the standing question is where exactly to draw the standard-vs-custom line for flow-control prompts.
