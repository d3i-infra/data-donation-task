---
status: accepted
date: "2026-03-20"
tags:
    - logging
    - pii-safety
    - observability
source: 02a3a04
category: Python architecture
applies_to:
    - packages/python/port/api/logging.py
    - packages/python/port/helpers/port_helpers.py
    - packages/python/port/helpers/flow_builder.py
    - packages/python/port/script.py
    - packages/python/port/main.py
    - packages/feldspar/src/framework/logging.ts
    - packages/feldspar/src/framework/assembly.ts
    - packages/feldspar/src/framework/processing/worker_engine.ts
    - packages/feldspar/src/live_bridge.ts
    - packages/feldspar/src/fake_bridge.ts
    - packages/data-collector/public/py_worker.js
priority: invariant
---

# Three logging boundaries for diagnostics, milestones, and consent-gated errors

## Decision

Python diagnostics stay local; host-visible milestones are explicit PII-free `CommandSystemLog` events; full Python error detail leaves only through consent-gated error donation.

## Guidance

- Do not auto-forward Python module loggers, worker logs, or raw error strings to the host; hidden forwarding pipelines are forbidden.
- Keep local diagnostics on module (`__name__`) loggers for in-browser debugging.
- Emit host-visible milestones deliberately via `port_helpers.emit_log()` with a constrained PII-free vocabulary.
- JS logging/bridge changes must not capture, buffer, flush, or forward Python/worker text that can contain participant data.

## Checks

- grep `api/logging.py` for code attaching `LogForwardingHandler` or another forwarding handler to port loggers.
- Review `LogForwarder` / `sendLogs` / worker-log flush paths for raw Python or worker log/error forwarding.
- Confirm host milestones go through `emit_log()` rather than ad-hoc log forwarding.

## Why

Participant data is processed in-browser before consent, and diagnostic/error strings often contain it. Explicit PII-free milestones preserve host observability without creating a hidden PII-bearing path.
