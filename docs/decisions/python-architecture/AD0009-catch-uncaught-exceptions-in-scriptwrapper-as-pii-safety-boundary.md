---
status: accepted
date: "2026-03-20"
tags:
    - exceptions
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

`ScriptWrapper.send()` catches exceptions that escape the Python workflow generator and routes them through consent-gated `error_flow()`, so raw Python exception text does not fall through to non-consented JS fallback or logging paths.

## Guidance

- Do not narrow or remove the broad `except Exception` around wrapped-generator advancement unless the replacement preserves consent-gated traceback donation.
- Keep `error_flow()` as the path for participant-reviewed traceback donation; do not add a JS-side path that forwards Python exception text without consent.
- The boundary covers generator advancement, not bugs in `ScriptWrapper`, `error_flow()`, worker `unwrap()`, or ordinary expected parsing failures.
- The JS worker/logging/bridge files are in `applies_to` as **enforcement points** for the no-unconsented-forwarding half of this invariant, not merely as context: `main.py` is the primary consent-boundary implementation; `py_worker.js` must not post raw Python errors as log/`error` events (its current fallback only renders a UI page); `worker_engine.ts` / `logging.ts` / `assembly.ts` must not capture or forward Python traceback text without consent; `live_bridge.ts` / `fake_bridge.ts` must not widen `sendLogs()` into raw stack/context forwarding that reopens the leak.

## Why

Python exception strings routinely embed the participant data that triggered them (a `ValueError` carries the input, a `JSONDecodeError` the raw string), and extraction processes DDPs before consent. `ScriptWrapper.send()`'s broad `except Exception` is the only consent-gated traceback path: the participant reviews the error and chooses whether to donate it. Weaken it and exceptions fall through to non-consented fallbacks — `py_worker.js` renders the raw error string into a page with no consent step, and worker-level errors can reach the JS log-forwarding path. Regex-sanitizing exception text was rejected as too unreliable for a PII guarantee; disabling JS logging loses all crash diagnostics.

## Checks

- Confirm `ScriptWrapper.send()` still wraps generator advancement in an `except Exception` that routes to `error_flow()`.
- grep JS worker/logging/bridge paths for raw Python exception forwarding without consent.
