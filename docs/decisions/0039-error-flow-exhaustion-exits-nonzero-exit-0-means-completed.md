---
status: accepted
date: "2026-08-27"
category: Feldspar
applies_to:
    - packages/python/port/main.py
    - packages/python/port/helpers/flow_builder.py
priority: invariant
companions:
    - packages/python/tests/test_main_queue.py
    - packages/python/tests/test_flow_builder.py
    - tests/error-flow.spec.ts
checks:
    - desc: no interpolated text reaches TaskIncompleteError (reason keys only)
      grep: 'TaskIncompleteError\(f'
      in: ["packages/python/port/**/*.py"]
      expect: absent
    - desc: the only literal exit code in ScriptWrapper is the flow-end 0
      grep: 'CommandSystemExit\([1-9]'
      in: ["packages/python/port/main.py"]
      expect: absent
    - desc: the incomplete exit is parameterized from the handler's (code, info)
      grep: 'CommandSystemExit\(self\._exit_code, self\._exit_info\)'
      in: ["packages/python/port/main.py"]
      expect: present
---

# Exit nonzero on every incomplete ending; 0 means completed

## Decision

The exit code is the completion signal across the bridge: only genuine flow-end exits 0. Every incomplete ending exits nonzero through `ScriptWrapper.send()`, so the host keeps the task pending rather than recording it as completed.

## Guidance

- A `start_flow()` terminal path that IS a completion (donation success, consent declined with a decline record, no data found with no extraction errors) `return`s; one that is NOT raises `TaskIncompleteError` with a reason from its `EXITS` table — the exception derives the fixed `(code, info)` pair itself, so a raise site can never inject text, and never a `return` that lets an incomplete ending exhaust into exit 0.
- Never let the handler exhaustion branch in `ScriptWrapper.send()` fall back to exit 0 — hosts (mono's `crew_task_helpers.ex` `handle_tool_exited()`) treat exit 0 as unconditional completion with no donation check, so an incomplete-end exit 0 silently records the participant as a satisfied completion.
- The exit `info` is always a fixed PII-free literal (`TaskIncompleteError.EXITS`, or `"Error flow completed"`) — never interpolate traceback, exception, or participant text; error detail leaves the iframe only through the consent-gated `error-report` donation inside `error_flow()`.
- Nonzero codes are a fork-local convention (1 unhandled error, 2 participant abandoned, 3 donation delivery failed, 4 upload rejected) pending an agreed exit-code contract with Eyra: the host only distinguishes 0 from nonzero today, so codes may be re-mapped in coordination with mono, but 0 stays reserved for genuine completion.
- Both handler flows (`error_flow()` and `incomplete_flow()` in `main.py`) terminate by yielding `ph.render_task_incomplete_page(platform)`, and that page must resolve its render promise so the generator can exhaust — an unresolved terminal page suppresses the exit signal entirely (the EndPage hang). Success paths still end by plain exhaustion with no terminal page.
- Acceptance tests: `tests/error-flow.spec.ts` (crash path with `tests/error-trigger.zip`; retry-declined path with `tests/invalid.zip`), run via the e2etest platform — `VITE_PLATFORM=e2etest pnpm test:e2e`.

## Why

Mono completes the crew task on exit 0 without checking that anything was donated, so any incomplete path exiting 0 silently converts errored or abandoning participants into satisfied completions — invisible in funnel analysis, with completion/payment signals firing on zero data (Issue #123).
