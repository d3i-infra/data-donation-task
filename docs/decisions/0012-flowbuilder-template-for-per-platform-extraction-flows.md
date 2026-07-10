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

Platform flows are `FlowBuilder` subclasses. `FlowBuilder.start_flow()` owns the shared lifecycle — upload receipt, safety check, validation/retry, consent, donation, and the no-data/failure pages; each platform implements the two required template hooks `validate_file()` and `extract_data()` (plus platform-specific parsing helpers) and must not reimplement that shared lifecycle.

## Guidance

- Do not reimplement file receipt, retry, consent, or donation in `platforms/`; that shared lifecycle lives in `FlowBuilder.start_flow()`.
- Add a platform by subclassing `FlowBuilder` and implementing `validate_file()` and `extract_data()` (alongside the module's `EXTRACTOR_REGISTRY` / `process()` per the platform interface); `script.py` reaches it through the standard `process()` dispatch, which returns `<Platform>Flow(session_id).start_flow()`.
- Narrow platform-specific overrides are fine — e.g. overriding `generate_file_prompt()` for a non-zip upload (TikTok), or yielding an intermediate selection UI inside `extract_data()` (Netflix's profile picker). The line is *reimplementing* the shared lifecycle, not extending at these hook points.
- Put shared lifecycle and UI helpers in `helpers/`, not copied into platform modules.

## Why

Every platform needs the same control loop — upload, retry on invalid, consent, donate — and only validation and extraction actually differ, so the template method concentrates a platform to `validate_file()` + `extract_data()` instead of duplicating the loop ten times. It is also the *single* extraction architecture: the parallel `donation_flows/` system, which auto-extracted everything (~180 Facebook tables), was removed in favor of hand-curated extraction because data minimization is a project principle — extraction is deliberate, not automatic. Cost: `start_flow()` is load-bearing; a change to the shared loop touches every platform at once.

## Checks

- Review platform modules for a *reimplemented* shared lifecycle — file receipt, retry prompts, consent, or donation calls belong in `FlowBuilder.start_flow()`, not `platforms/`. Narrow hook overrides (`generate_file_prompt`, an in-`extract_data` selection UI) and the standard `process()` / `EXTRACTOR_REGISTRY` interface are expected, not violations.
