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
    - packages/python/port/script.py
priority: default
---

# Handle structured donation results with legacy PayloadVoid fallback

## Decision

Donation command results are normalized through `port_helpers.handle_donate_result()`: `PayloadResponse.value.success` is authoritative, `PayloadVoid` / `None` is legacy success, and unexpected payloads fail closed with a local warning.

## Guidance

- Route every production `CommandSystemDonate` / `ph.donate()` result through `handle_donate_result()`, except `main.py:error_flow()`.
- Read structured responses as `result.value.success`, not `result.success`.
- Treat `PayloadVoid` / `None` as success for D3I mono compatibility.
- Failed participant-data donations show the donation failure page; failed decline-status donations are logged and suppressed.
- `error_flow()` donates the consent-gated error report fire-and-forget after consent; do not use that exception for ordinary donations.

## Checks

- Confirm FlowBuilder routes every data/decline donation result through `handle_donate_result()`.
- grep for direct `__type__ == "PayloadResponse"` / `PayloadVoid` handling outside `port_helpers.py` and tests.
- grep `result.success` where `result.value.success` is meant.
