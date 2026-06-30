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
priority: invariant
---

# Python generator protocol for workflow orchestration

## Decision

Participant workflow code is a Python generator protocol: it yields command objects and receives payloads through `send()`. Do not replace workflow orchestration with callbacks, explicit state machines, or `async`/`await`.

## Guidance

- Drive study, FlowBuilder, platform, and custom participant workflows with `yield` / `yield from` command-payload steps.
- Test desktop flows by advancing generators with simulated `send()` payloads.
- Let flows return/exhaust; `ScriptWrapper` converts `StopIteration` to `CommandSystemExit`.

## Checks

- Review workflow code for `async def` / `await`, callback registration, or manual state-machine orchestration; participant flow should advance through `yield` / `yield from`.

## Why

The Eyra bridge pauses Python at each yielded command and resumes it with a payload via `send()`. Replacing that protocol would require translating upstream Eyra Python and changing the worker/bridge contract, even if local code appeared to work.
