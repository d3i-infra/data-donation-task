---
status: accepted
date: "2026-03-13"
tags:
    - validation
    - ddp-categories
    - fail-fast
category: Extraction
applies_to:
    - packages/python/port/helpers/flow_builder.py
    - packages/python/port/helpers/validate.py
    - packages/python/port/platforms/**/*.py
priority: default
companions:
    - packages/python/tests/test_validate.py
    - packages/python/tests/test_flow_builder.py
---

# Validate DDP categories before extraction

## Decision

`FlowBuilder.start_flow()` runs each platform's `validate_file()` before `extract_data()`, so extraction receives a `ValidateInput` and may assume valid input. The standard validator checks the archive against the platform's `DDP_CATEGORIES` structural contract via `validate.validate_zip`.

## Guidance

- Do not call `extract_data()` before validation or bypass `FlowBuilder.start_flow()` to reach extraction directly.
- Keep each platform's `DDP_CATEGORIES` aligned with the files `extract_data()` actually reads.
- Invalid validation returns the retry prompt; it is not an extraction error or traceback path.
- The invariant is validate-before-extract, not `DDP_CATEGORIES` specifically. WhatsApp is the standing exception — a chat export is a single file, not a multi-file DDP, so it defines no `DDP_CATEGORIES` and validates through its own `validate_file()`. (`example.py` is a non-normative template; its placeholder validator points at the `DDP_CATEGORIES` pattern.)

## Why

Uploaded zips are routinely the wrong platform, wrong format, or corrupt, and extraction is expensive and fails cryptically on bad input — a raw `KeyError` after all the parsing is useless to a participant. Validating first against `DDP_CATEGORIES` fails fast with a meaningful retry prompt and lets `extract_data()` assume a structurally valid archive. Centralizing the order in `start_flow()` means no platform can reach extraction first. The ordering, not `DDP_CATEGORIES` itself, is the invariant — WhatsApp's single-file export validates through its own `validate_file()`. Cost: `DDP_CATEGORIES` must stay aligned with the files `extract_data()` actually reads, or invalid files slip through.
