---
status: accepted
date: "2026-03-13"
tags:
    - layering
    - structure
    - imports
category: Python architecture
applies_to:
    - packages/python/port/**/*.py
priority: default
---

# Layered Python architecture with unidirectional dependencies

## Decision

`packages/python/port/` is layered — lowest to highest: `api` -> `helpers` -> `platforms` / `script.py` -> `main.py` (the arrow is layer order, not import direction). A module may import only from its own layer or a lower one; it must never import a higher layer.

## Guidance

- Dependencies point down the stack: `main.py` imports `script.py`/`platforms`, `platforms` and `script.py` import `helpers`, `helpers` import `api`, and `api` imports no other `port` layer. Same-layer imports are OK; a lower layer importing a higher one (e.g. `helpers` -> `platforms`) is forbidden.
- `api/` contains protocol types, props, command types, and file utilities; it may import stdlib, third-party packages, and Pyodide `js`, but no other `port` layers.
- Put shared workflow, validation, upload, extraction, and UI helpers in `helpers/`; platform-specific validation/extraction stays in `platforms/`.
- When merging upstream Eyra's flat Python API, reconcile it into this layout instead of importing upward to match upstream shape.

## Why

A flat `port/` makes import direction unenforceable and pushes the per-platform modules to either duplicate shared helpers or reach into each other; layering makes direction auditable and keeps platform code thin. If the rule is weakened, `helpers/` and `platforms/` tangle, shared logic scatters and duplicates across platforms, and no layer can be refactored or tested in isolation — and reconciling upstream Eyra's flat API by importing upward would quietly reintroduce exactly that.
