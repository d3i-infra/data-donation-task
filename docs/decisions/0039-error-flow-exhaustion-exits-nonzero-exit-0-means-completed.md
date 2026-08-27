---
status: accepted
date: "2026-08-27"
category: Feldspar
applies_to:
    - packages/python/port/main.py
priority: invariant
companions:
    - packages/python/tests/test_main_queue.py
    - tests/error-flow.spec.ts
---

# Error-flow exhaustion exits nonzero; exit 0 means completed

## Decision

The exit code is the completion signal across the bridge: only genuine flow-end exits 0. When the consent-gated error flow (`error_flow()`) exhausts, `ScriptWrapper.send()` returns `CommandSystemExit(1, "Error flow completed")` instead of the flow-end `CommandSystemExit(0, ...)`, so the host keeps an errored participant's task pending rather than recording it as completed.

## Guidance

- Never let the error-handler's `StopIteration` branch in `ScriptWrapper.send()` fall back to exit 0 — hosts (mono's `crew_task_helpers.ex` `handle_tool_exited()`) treat exit 0 as unconditional completion with no donation check, so an error-end exit 0 silently records an errored participant as a satisfied completion.
- Keep the exit `info` a fixed PII-free literal (`"Error flow completed"`) — never interpolate traceback or exception text into it; that text leaves the iframe only through the consent-gated `error-report` donation inside `error_flow()` (ADR-0022, ADR-0023).
- `error_flow()` terminates by yielding `ph.render_task_incomplete_page(platform)` (built in `port_helpers.py` per ADR-0009) — a single-button Confirm that resolves so the generator can exhaust. This refines, not contradicts, ADR-0025's no-in-iframe-end-page rule: success still exits through plain generator exhaustion with no terminal page; this page exists only on the error path, as a *resolvable pre-exit acknowledgment* replacing the stale error page the participant would otherwise be stranded on, and it must never become a permanent unresolved end page (an unresolved render promise would suppress the exit signal entirely, reproducing the EndPage hang ADR-0025 forbids).
- The acceptance test for this behavior is `tests/error-flow.spec.ts` (paired with the `tests/error-trigger.zip` fixture), run via the e2etest platform (`packages/python/port/platforms/e2etest.py`, `VITE_PLATFORM=e2etest pnpm test:e2e`); that platform is build-time-only and excluded from real per-platform releases by `release.sh`'s discovery loop and `scripts/verify_release_wheel.py` (ADR-0004) — it never ships to a study participant.
- Changing exit-code semantics is a workflow↔host contract change: coordinate with mono and ship it with a version bump and migration notes.

## Why

Mono completes the crew task on exit 0 without checking that anything was donated, so an error path exiting 0 silently converts errored participants into satisfied completions — invisible in funnel analysis, and completion/payment signals can fire with zero data (Issue #123). Nonzero exit plus a resolvable terminal page fixes both halves: the host sees the task as incomplete, and the participant is not left staring at a stale error page with no way forward.
