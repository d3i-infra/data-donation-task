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

Underscore-prefixed Python symbols are internal implementation detail, not part of a layer's cross-layer contract: they must not be imported across `port` layer boundaries. Sharing a private symbol between modules within the same layer is allowed but should stay rare.

## Guidance

- Do not import underscore-prefixed symbols across layers; make shared contracts public in the lowest responsible layer.
- If a private helper is genuinely shared protocol/file utility, move or expose it in `api/`; if it is shared workflow/extraction logic, move or expose it in `helpers/`.
- Platform-specific private helpers usually stay in their platform module and are generally not reused from `script.py`, `helpers/`, or another platform.

## Why

The underscore prefix is Python's established signal for "internal to this module"; importing such a symbol from another layer silently overrides the author's contract and couples layers that the layering rule keeps independent, so a refactor inside one module can break a distant caller with no warning. Forcing the symbol to be made public and moved to the layer that owns it (`api`/`helpers`) turns "reach across a boundary" into a deliberate decision about where shared logic belongs.
