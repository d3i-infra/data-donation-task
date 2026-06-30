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

- Keep the order upload receipt -> `check_payload_size()` -> validation -> extraction.
- The size check is metadata-only; never read upload bytes to measure the file.
- Safety is platform-independent, so do not duplicate this guard in platform modules.

## Why

A whole-upload read can fail or exhaust the Pyodide worker for multi-GiB exports, and an exactly 2 GiB file is the chunked-export sentinel for incomplete takeouts. These failures are independent of DDP shape, so the upload must be rejected before opening or extracting it.
