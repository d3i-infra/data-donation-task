---
status: accepted
date: "2026-03-13"
tags:
    - flowbuilder
    - platform-structure
    - template-method
category: Extraction
applies_to:
    - packages/python/port/helpers/flow_builder.py
    - packages/python/port/platforms/**/*.py
priority: default
companions:
    - packages/python/tests/test_flow_builder.py
---

# FlowBuilder template for per-platform extraction flows

## Decision

Platform flows are `FlowBuilder` subclasses. `FlowBuilder.start_flow()` owns upload receipt, retry, consent, and donation; platform modules implement `validate_file()` and `extract_data()` plus platform-specific parsing helpers.

## Guidance

- Do not implement file receipt, retry, consent, or donation in `platforms/`; keep that lifecycle in `FlowBuilder.start_flow()`.
- Add a platform by subclassing `FlowBuilder` and routing it from `script.py` with `yield from flow.start_flow()`.
- Put shared lifecycle and UI helpers in `helpers/`, not copied into platform modules.
