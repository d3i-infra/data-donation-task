---
status: accepted
date: "2026-03-13"
tags:
    - imports
    - encapsulation
category: Python architecture
applies_to:
    - packages/python/port/**/*.py
priority: default
---

# No cross-layer private imports

## Decision

Underscore-prefixed Python symbols are private to their module and must not be imported across `port` layer boundaries. Same-layer private use is allowed but should stay rare.

## Guidance

- Do not import underscore-prefixed symbols across layers; make shared contracts public in the lowest responsible layer.
- If a private helper is genuinely shared protocol/file utility, move or expose it in `api/`; if it is shared workflow/extraction logic, move or expose it in `helpers/`.
- Platform-specific private helpers usually stay in their platform module and are not reused from `script.py`, `helpers/`, or another platform.
