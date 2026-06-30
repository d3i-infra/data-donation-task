---
adr_id: "0002"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-13 13:41:56"
links:
    precedes: []
    succeeds: []
status: accepted
date: 2026-03-13
tags:
    - pyodide
    - mocking
    - conftest
title: Pyodide mocking strategy for desktop testing
---

## <a name="question"></a> Context and Problem Statement

The Python package runs inside Pyodide (browser), where the `js` module is available. Desktop pytest has no `js` module, so any `from port...` import fails immediately with ImportError. How should tests handle the Pyodide environment dependency?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Skip all tests that import port modules
2. <a name="option-2"></a> Mock sys.modules before all port imports using sys.modules['js'] = MagicMock()
3. <a name="option-3"></a> Conditional imports with try/except around js-dependent code

## <a name="criteria"></a> Decision Drivers

* Skipping tests eliminates coverage of extraction logic entirely — not acceptable
* Conditional imports (`try: import js`) scatter environment-awareness through production code, creating a second code path that only runs in Pyodide
* `sys.modules` patching is the established Python pattern for replacing a missing dependency at test time without touching production code

## <a name="outcome"></a> Decision Outcome
We decided for [Option 2](#option-2) because: The mock-before-import pattern is the simplest reliable approach: one line in conftest.py patches the missing module before any port code is imported, enabling the full test suite to run on desktop without changing production code paths.

### Consequences

* Good: Production code has no test-environment conditionals — it runs identically in Pyodide and under pytest
* Good: A single `sys.modules['js'] = MagicMock()` in `conftest.py` covers the entire test suite
* Bad: The mock must precede ALL `from port...` imports — import ordering is a hard constraint, not obvious to new contributors
* Bad: If production code calls `js` APIs in ways the MagicMock doesn't simulate, tests pass but runtime behaviour in Pyodide differs

### Confirmation

`CLAUDE.md` states: "`sys.modules['js'] = MagicMock()` must precede all `from port...` imports in tests." Any new test file that imports port modules without the mock will fail with `ImportError` at import time — the failure is immediate and self-diagnosing.

## More Information

See [python-architecture/AD0005](../python-architecture/AD0005-python-generator-protocol-for-workflow-orchestration.md) — the generator protocol determines what Pyodide interactions need to be accounted for in tests.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-03-13 13:41:56) Danielle McCool: marked decision as decided
