---
status: accepted
date: "2026-03-17"
tags:
    - safety
    - uploads
    - memory-safety
category: Extraction
applies_to:
    - packages/python/port/helpers/flow_builder.py
    - packages/python/port/helpers/uploads.py
priority: invariant
companions:
    - packages/python/tests/test_uploads.py
    - packages/python/tests/test_flow_builder.py
---

# Reject unsafe uploads before validation and extraction

## Decision

After receiving a `PayloadFile`, `FlowBuilder.start_flow()` runs `uploads.check_payload_size(file_result)` before DDP validation or extraction. The guard reads only `file_result.value.size` and renders `ph.render_safety_error_page()` for `FileTooLargeError` or `ChunkedExportError`.

## Guidance

- Keep the order upload receipt → `check_payload_size()` → validation → extraction.
- The size check is metadata-only; never read upload bytes to measure the file.
- Safety is platform-independent, so do not duplicate this guard in platform modules or in a study's `script.py`.

## Why

A file of exactly 2 GiB is the chunked-export sentinel: an incomplete multi-part download that would extract silently wrong data. Files above 2 GiB are rejected as policy — streaming removed the read ceiling, but extraction still decompresses and parses members inside the Pyodide worker heap, and the JS-reported size is a free upstream proxy for that risk. The guard runs once in FlowBuilder, before validation, because it is platform-independent and the validators assume a structurally safe file. It reads only metadata (`file_result.value.size`): reading bytes to measure would defeat the streaming it protects.
