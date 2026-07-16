---
status: accepted
date: "2026-03-13"
tags:
    - pyodide
    - mocking
    - conftest
category: Testing
applies_to:
    - packages/python/tests/**/*.py
priority: default
checks:
    - desc: no per-file js shims outside conftest
      grep: 'sys\.modules\["js"\]'
      in: ["packages/python/tests/test_*.py"]
      expect: absent
---

# Mock the Pyodide js module in conftest before importing port

## Decision

Desktop pytest has no Pyodide `js` module, so `tests/conftest.py` sets `sys.modules["js"] = MagicMock()` once, before any `from port...` import; pytest loads conftest ahead of every test module, so the whole suite runs on desktop without touching production code.

## Guidance

- The shim lives once in `conftest.py` — don't add per-file shims, and don't place a `port` import above the `sys.modules["js"]` line inside conftest itself (a late shim fails immediately with `ImportError`, which is self-diagnosing).
- Keep environment awareness out of production code: no `try/except import js` conditionals — that creates a Pyodide-only path desktop tests never exercise.
- The mock is a bare `MagicMock` simulating no real JS API shape — don't write assertions that depend on mock-returned values; behavior across the real JS boundary is exercised by the Playwright e2e suite, not pytest.
- Standing exception: `test_dataframe_truncation.py` loads `props.py` directly via `importlib` to bypass `port/__init__` — a narrower isolation for a `js`-free module; don't "fix" it toward the conftest pattern, and don't copy it where the shim suffices.

## Why

Without the shim, every `from port...` import fails at collection time and the extraction logic has no desktop test coverage at all — skipping those tests was not acceptable, and conditional imports would scatter a second, test-only code path through production modules. `sys.modules` patching is the established Python pattern for standing in for a missing environment module, and centralizing it in `conftest.py` makes the ordering constraint structural rather than a per-file discipline. The accepted risk is fidelity: a bare `MagicMock` accepts anything, so code that misuses a real `js` API can pass pytest and still fail in Pyodide — which is why boundary behavior needs its own end-to-end coverage.
