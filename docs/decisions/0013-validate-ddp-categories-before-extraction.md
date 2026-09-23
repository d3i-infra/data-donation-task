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
- The invariant is validate-before-extract, not `DDP_CATEGORIES` specifically. WhatsApp and Google are standing exceptions to the `DDP_CATEGORIES` contract, not to the ordering. WhatsApp's chat export is a single file, not a multi-file DDP, so it defines no `DDP_CATEGORIES` and validates through its own `validate_file()`. Google's Takeout archive has neither one filetype nor a stable filename set — the export format is chosen per source and filenames collide across folders — so `platforms/google.py` defines no `DDP_CATEGORIES` either; its `validate_ddp` recognizes the archive by matching folder-qualified member paths against the union inventory of the whole `ArchiveSet`, and still runs before extraction like every other platform's validator. (`example.py` is a non-normative template; its placeholder validator points at the `DDP_CATEGORIES` pattern.)

## Why

Uploaded zips are routinely the wrong platform, wrong format, or corrupt, and extraction is expensive and fails cryptically on bad input — a raw `KeyError` after all the parsing is useless to a participant. Validating first against `DDP_CATEGORIES` fails fast with a meaningful retry prompt and lets `extract_data()` assume a structurally valid archive. Centralizing the order in `start_flow()` means no platform can reach extraction first. The ordering, not `DDP_CATEGORIES` itself, is the invariant — WhatsApp's single-file export and Google's multi-source, multi-format Takeout archive each validate through their own `validate_file()`/`validate_ddp()` instead of a `DDP_CATEGORIES` list, and both still run before extraction. Cost: `DDP_CATEGORIES` must stay aligned with the files `extract_data()` actually reads, or invalid files slip through; a platform with its own validator carries that same cost for its own recognition logic instead.

## Checks

- Aside from the e2etest platforms (excluded from release builds, ADR-0004) and the non-normative `example.py` template, confirm `platforms/google.py` and `platforms/whatsapp.py` are the only modules under `port/platforms/` that define no `DDP_CATEGORIES`, and that each instead exposes its own validator (`validate_ddp` / `validate_file`).
- Confirm `google.validate_ddp` takes an `ArchiveSet` and never calls `zipfile.ZipFile(...)` itself — recognition runs over `archive_set.members`, not a fresh unzip.
