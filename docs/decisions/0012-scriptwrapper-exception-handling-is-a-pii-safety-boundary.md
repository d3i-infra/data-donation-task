---
adr_id: "0009"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-20 13:12:24"
    - author: Danielle McCool
      comment: "2"
      date: "2026-03-20 13:12:24"
links:
    precedes: []
    succeeds: []
status: decided
tags:
    - exceptions
    - pii-safety
    - error-handling
title: Catch uncaught exceptions in ScriptWrapper as PII safety boundary
---

## <a name="question"></a> Context and Problem Statement

Uncaught Python exceptions propagate to the Pyodide runtime and become JS errors. In Eyra's feldspar (develop) the JS worker_engine logs these via LogForwarder → bridge.sendLogs() → mono — forwarding the full exception message and stack trace to the host platform without participant consent. Python exceptions routinely include participant data in their messages (ValueError includes the offending input — KeyError includes the key — JSONDecodeError includes the string). In our multi-module architecture with extraction helpers processing participant DDPs — this is a concrete PII exposure risk. How should uncaught exceptions be handled to prevent PII leakage through the JS logging path?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Catch all exceptions in ScriptWrapper.send() before they reach Pyodide — route through consent-gated error_flow()
2. <a name="option-2"></a> Sanitize exceptions at the JS worker level before logging
3. <a name="option-3"></a> Disable JS-side exception logging entirely

## <a name="criteria"></a> Decision Drivers
Python exception messages routinely contain the data that caused the error — this IS participant data
The JS-side logging path (worker_engine → LogForwarder → bridge.sendLogs) forwards to mono without any consent mechanism
Researchers cannot prevent this through careful coding — any unexpected error in data processing is enough
We need crash diagnostics — silently swallowing exceptions is not acceptable
The participant should have agency over whether their error data leaves the browser
### Pros and Cons

**Catch all exceptions in ScriptWrapper.send() before they reach Pyodide — route through consent-gated error_flow()**
* Good, because no exception from script/platform/helper code ever reaches the JS logging path
* Good, because participant sees the error and chooses whether to donate the traceback
* Good, because already implemented in our fork's main.py (lines 93-97)
* Neutral, because exceptions in ScriptWrapper's own framework code (queue drain — error_flow generator) are not covered — but these don't process participant data

**Sanitize exceptions at the JS worker level before logging**
* Good, because catches everything including framework errors
* Bad, because sanitization is fragile — regex-based PII stripping is unreliable
* Bad, because requires JS-side changes to feldspar framework code

**Disable JS-side exception logging entirely**
* Good, because simple and complete
* Bad, because loses all crash diagnostics for JS-level and framework-level errors


## <a name="outcome"></a> Decision Outcome
We decided for [Option 1](#option-1) because: The except Exception handler in ScriptWrapper.send() is a PII safety boundary — not just error handling. It prevents participant data embedded in Python exception messages from reaching the JS logging path which forwards unsanitized to the host platform. The error_flow() consent mechanism gives participants agency over whether error details leave the browser. This is already implemented in our fork. The handler must not be removed or narrowed without replacing the PII protection it provides.

## <a name="comments"></a> Comments
<a name="comment-2"></a>2. (2026-03-20 13:12:24) Danielle McCool: More Information:
See feldspar/AD0002 for the bridge abstraction. The JS-side path (py_worker.js catch → worker_engine 'error' event → LogForwarder → bridge.sendLogs) exists in eyra/feldspar develop and forwards raw exception content to mono. This was reported to Eyra on 2026-03-20. See python-architecture/AD0008 for the log forwarding scope decision.
