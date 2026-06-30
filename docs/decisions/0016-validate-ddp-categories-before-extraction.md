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

`FlowBuilder.start_flow()` runs platform validation (`validate_file()` / `validate.validate_zip(DDP_CATEGORIES, archive)`) before `extract_data()`. `DDP_CATEGORIES` is the per-platform structural contract; extraction receives `ValidateInput` and may assume a valid archive.

## Guidance

- Do not call `extract_data()` before validation or bypass `FlowBuilder.start_flow()` to reach extraction directly.
- Keep each platform's `DDP_CATEGORIES` aligned with the files `extract_data()` actually reads.
- Invalid validation returns the retry prompt; it is not an extraction error or traceback path.
