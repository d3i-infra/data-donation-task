---
status: accepted
date: "2026-03-20"
tags:
    - pii-safety
    - error-handling
category: Python architecture
applies_to:
    - packages/python/port/main.py
    - packages/data-collector/public/py_worker.js
    - packages/feldspar/src/framework/processing/worker_engine.ts
    - packages/feldspar/src/framework/logging.ts
    - packages/feldspar/src/framework/assembly.ts
    - packages/feldspar/src/live_bridge.ts
    - packages/feldspar/src/fake_bridge.ts
priority: invariant
---

# ScriptWrapper exception handling is a PII safety boundary

## Decision

`ScriptWrapper.send()` catches exceptions that escape the Python workflow generator and routes them through consent-gated `error_flow()` before raw Python exception text can reach JS/host logging.

## Guidance

- Do not narrow or remove the broad `except Exception` around wrapped-generator advancement unless the replacement preserves consent-gated traceback donation.
- Keep `error_flow()` as the path for participant-reviewed traceback donation; do not add a JS-side path that forwards Python exception text without consent.
- The boundary covers generator advancement, not bugs in `ScriptWrapper`, `error_flow()`, worker `unwrap()`, or ordinary expected parsing failures.

## Checks

- Confirm `ScriptWrapper.send()` still wraps generator advancement in an `except Exception` that routes to `error_flow()`.
- grep JS worker/logging/bridge paths for raw Python exception forwarding without consent.

## Why

Python exception strings often contain participant data (`ValueError` input, `KeyError` key). Without this boundary, worker logging can forward that raw text to the host before the participant has consented to donate it.
