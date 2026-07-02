---
status: accepted
date: "2026-03-20"
tags:
    - logging
    - pii-safety
    - observability
    - bridge
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

- Do not auto-forward Python module loggers or raw Python error strings to the host; hidden forwarding pipelines are forbidden — in particular, `LogForwardingHandler` (the rejected hidden-handler mechanism) must never be attached to a port logger.
- Keep local diagnostics on module (`__name__`) loggers for in-browser debugging.
- Emit host-visible milestones deliberately via `port_helpers.emit_log()` with a constrained PII-free vocabulary.
- The JS logging/bridge/worker files are in `applies_to` as enforcement points (not context). Their framework-level logging (`worker.onerror`, `LogForwarder`, `sendLogs()`) is intentional and fine for JS/worker observability; the boundary is that JS must not become a hidden path for **Python module diagnostics or raw Python participant-data error text**. Concretely, do not add a producer that posts raw Python errors into the worker → `LogForwarder` → `sendLogs()` path — the `worker_engine` `error`-event handler is inert only because `py_worker.js` posts no such event.

## Why

Participant data is processed in the browser before consent, and Python diagnostic strings routinely contain it — so by default nothing but the consented donation payload leaves the iframe. Three log kinds get conflated without explicit boundaries: developer diagnostics (may contain PII — stay in the browser), flow milestones (must be PII-safe — deliberate `emit_log()` calls), and full error detail (consent-gated donation only). The rejected shortcuts all leak: whole-tree forwarding spills helper diagnostics, a hidden `LogForwardingHandler` buries the host-boundary crossing, and automatic scrubbing is too unreliable for a PII guarantee. The discipline is per-callsite: a milestone is safe only because a human chose PII-free wording there.

## Checks

- grep `api/logging.py` for code attaching `LogForwardingHandler` or another forwarding handler to port loggers.
- Review `LogForwarder` / `sendLogs` / worker-log flush paths for raw Python or worker log/error forwarding.
- Confirm host milestones go through `emit_log()` rather than ad-hoc log forwarding.
