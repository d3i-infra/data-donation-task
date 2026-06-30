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

`packages/python/port/` follows a one-way import order: `api` -> `helpers` -> `platforms` / `script.py` -> `main.py`. Same-layer imports are allowed; upward imports are not.

## Guidance

- Keep imports flowing downward: `api` -> `helpers` -> `platforms` / `script.py` -> `main.py`; same-layer imports are OK.
- `api/` contains protocol types, props, command types, and file utilities; it may import stdlib, third-party packages, and Pyodide `js`, but no other `port` layers.
- Put shared workflow, validation, upload, extraction, and UI helpers in `helpers/`; platform-specific validation/extraction stays in `platforms/`.
- When merging upstream Eyra's flat Python API, reconcile it into this layout instead of importing upward to match upstream shape.
