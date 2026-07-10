---
status: accepted
date: "2026-03-13"
tags:
    - generator
    - bridge
    - orchestration
category: Python architecture
applies_to:
    - packages/python/port/script.py
    - packages/python/port/platforms/**/*.py
    - packages/python/port/helpers/flow_builder.py
    - packages/python/port/helpers/port_helpers.py
    - packages/python/port/main.py
priority: invariant
companions:
    - packages/data-collector/public/py_worker.js
    - packages/feldspar/src/framework/processing/worker_engine.ts
---

# Python generator protocol for workflow orchestration

## Decision

Participant workflow code is a Python generator protocol: it yields command objects and receives payloads through `send()`. Do not replace workflow orchestration with callbacks, explicit state machines, or `async`/`await`.

## Guidance

- Drive study, FlowBuilder, platform, and custom participant workflows with `yield` / `yield from` command-payload steps (e.g. `file_result = yield ph.render_page(...)`).
- Test desktop flows by advancing generators with simulated `send()` payloads.
- Let flows return/exhaust; `ScriptWrapper` converts `StopIteration` to `CommandSystemExit`.

## Why

Generators are Eyra's existing design: the worker already pauses Python at each yielded command and resumes it with the response, so the generator *is* the contract the worker and bridge implement. It reads as a sequential script — `file_result = yield ph.render_page(...)` is "show this and wait" in one line — where callbacks or a state machine need scaffolding that obscures the flow. Replacing it would fork the worker/bridge contract and force translating every upstream change forever; worse, a rewrite can pass mocked-`send()` desktop tests yet break against the real bridge. Cost: desktop tests must advance the generator with simulated `send()` payloads, which is unobvious to newcomers.

## Checks

- Review workflow code for `async def` / `await`, callback registration, or manual state-machine orchestration; participant flow should advance through `yield` / `yield from`.
