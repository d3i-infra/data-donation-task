---
adr_id: "0005"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-13 13:33:17"
links:
    precedes:
        - "0001"
    succeeds: []
status: accepted
date: 2026-03-13
tags:
    - generator
    - bridge
    - orchestration
title: Python generator protocol for workflow orchestration
---

## <a name="question"></a> Context and Problem Statement

The Python workflow needs to drive a multi-step interaction with the TypeScript UI: show a page, receive a response, decide what to show next, repeat. The workflow logic lives in Python but rendering happens in TypeScript. How should Python orchestrate this multi-step flow?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> State machine — explicit states and transition table
2. <a name="option-2"></a> Python generators — yield commands to the bridge, receive responses via send()
3. <a name="option-3"></a> Callback chains — register handlers for each response type
4. <a name="option-4"></a> Async coroutines with await for each user interaction

## <a name="criteria"></a> Decision Drivers

* Eyra's design already uses generators — diverging would create an incompatibility with upstream that we'd carry forever
* `file_result = yield render_page(...)` expresses "show this page and wait for a response" as a single line; callbacks and state machines require far more scaffolding for the same intent
* The bridge protocol (FakeBridge / LiveBridge) is already designed to send generator commands and return payloads via `send()` — the two mechanisms fit together

## <a name="outcome"></a> Decision Outcome
We decided for [Option 2](#option-2) because: Generators are Eyra's design: `file_result = yield render_page(...)` reads naturally as a sequential script while allowing the engine to pause Python, send the command over the bridge, receive the response, and resume; this is significantly more readable than callbacks or state machines for multi-step wizard flows.

### Consequences

* Good: `script.py` reads as a sequential description of the study flow — no state tracking or callback wiring
* Good: Alignment with Eyra's design means upstream Python changes can be applied without protocol translation
* Bad: Desktop testing requires mocking `generator.send()` with simulated payloads — not obvious to developers unfamiliar with the generator protocol

### Confirmation

Code review: any use of explicit state variables, callback registration, or `asyncio.await` to drive the study flow in `script.py` or platform flows is a violation. These should use `yield` with the appropriate command.

## More Information

See [feldspar/AD0002](../feldspar/AD0002-bridge-abstraction-with-swappable-implementations.md) — the bridge carries generator commands between the Python worker and the TypeScript host.

See [fork-governance/AD0002](../fork-governance/AD0002-fork-d3i-data-donation-task-as-the-study-base.md) — generator protocol inherited from Eyra; this is the upstream design.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-03-13 13:33:17) Danielle McCool: marked decision as decided
