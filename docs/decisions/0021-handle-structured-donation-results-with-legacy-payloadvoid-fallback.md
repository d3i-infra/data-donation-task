---
status: accepted
date: "2026-03-17"
tags:
    - donation
    - host-compatibility
    - protocol
category: Python architecture
applies_to:
    - packages/python/port/helpers/port_helpers.py
    - packages/python/port/helpers/flow_builder.py
    - packages/python/port/main.py
priority: default
companions:
    - packages/feldspar/src/framework/command_router.ts
    - packages/feldspar/src/live_bridge.ts
    - packages/feldspar/src/fake_bridge.ts
---

# Handle structured donation results with legacy PayloadVoid fallback

## Decision

Donation command results are normalized through `port_helpers.handle_donate_result()`: `PayloadResponse.value.success` is authoritative, `PayloadVoid` / `None` is legacy success, and unexpected payloads fail closed with a local warning.

## Guidance

- Route every production `CommandSystemDonate` / `ph.donate()` result through `handle_donate_result()`, except `main.py:error_flow()`.
- Read structured responses as `result.value.success`, not `result.success`.
- Treat `PayloadVoid` / `None` as success — a defensive branch for a host or bridge that resolves a donate without an acknowledgment; no current mono does.
- Failed participant-data donations show the donation failure page; failed decline-status donations are logged and suppressed.
- `error_flow()` donates the consent-gated error report fire-and-forget after consent; do not use that exception for ordinary donations.

## Why

Both monos acknowledge every donation over the MessageChannel, so production reaches Python as `PayloadResponse` whose `value.success` says whether the donation landed — a failure the participant must see. `PayloadVoid`/`None` remains a legacy shape (older hosts, a stub bridge) that must not be read as failure, or a working deployment breaks; ignoring results altogether silently swallows real upload failures. Normalizing once lets both hosts work unconfigured, and unknown payloads fail closed. Two traps make the single location load-bearing: the result nests under `value` (`result.value.success`), and a failed *decline* recording is deliberately silent — invisible infrastructure, not the participant's problem.

## Checks

- Confirm FlowBuilder routes every data/decline donation result through `handle_donate_result()`.
- grep for direct `__type__ == "PayloadResponse"` / `PayloadVoid` handling outside `port_helpers.py` and tests.
- grep `result.success` where `result.value.success` is meant.
