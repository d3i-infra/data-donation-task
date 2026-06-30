---
status: accepted
date: "2026-03-13"
tags:
    - pyodide
    - mocking
    - testing
category: Testing
applies_to:
    - packages/python/tests/**/*.py
priority: default
---

# Mock the Pyodide js module before importing port in tests

## Decision

Desktop pytest has no Pyodide `js` module, so each test module that imports `port` sets `sys.modules["js"] = MagicMock()` before its first `from port...` import. There is no shared `conftest.py`; the shim is per test file.

## Guidance

- In a test that imports `port` code, set `sys.modules["js"] = MagicMock()` ahead of the first `from port...` import — a missing or late shim fails immediately with `ImportError` at import time (self-diagnosing).
- Keep the environment shim in the tests; don't scatter `try/except import js` conditionals through production code (that adds a Pyodide-only path that desktop tests never exercise).
