---
adr_id: "0005"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-13 13:26:44"
links:
    precedes: []
    succeeds: []
status: accepted
date: 2026-03-13
tags:
    - release
    - vite-platform
    - build
title: Per-platform release builds via VITE_PLATFORM env var
---

## <a name="question"></a> Context and Problem Statement

The VU 2026 study deploys separate workflow instances per platform on Eyra Next — each platform has its own assignment and consent flow. The Python script must know which platform is active at build time. How should per-platform builds be produced and how should the platform identity be passed through the stack?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Single build with runtime platform selection (URL param or config)
2. <a name="option-2"></a> release.sh loop setting VITE_PLATFORM for each build
3. <a name="option-3"></a> CI build matrix producing platform artifacts in parallel

## <a name="criteria"></a> Decision Drivers

* Eyra Next requires a separate uploaded zip per platform — a single multi-platform build cannot be deployed
* The Python layer needs platform identity at runtime to select the right extraction logic
* CI infrastructure is not available; the release process must run locally

## <a name="outcome"></a> Decision Outcome
We decided for [Option 2](#option-2) because: A shell script loop is the simplest mechanism that produces separate deployable zips per platform without CI infrastructure; VITE_PLATFORM threads through worker_engine.ts to py_worker.js to main.py to script.py, giving the Python layer platform identity at runtime.

### Consequences

* Good: Produces 7 separate deployable zips from one `bash release.sh` invocation
* Good: `VITE_PLATFORM` is available throughout the stack at build time — no runtime platform detection needed in Python
* Good: Dev mode (no VITE_PLATFORM set) runs all platforms, which is convenient during development
* Bad: Release takes 7× the build time of a single build
* Bad: Branch names with `/` must be sanitised to `-` before use in zip filenames (known issue, handled in release.sh)

## More Information

The wiring: `release.sh` sets `VITE_PLATFORM` → Vite embeds it → `worker_engine.ts` reads `import.meta.env.VITE_PLATFORM` → passes to `py_worker.js` → `main.py` receives it → `script.py` filters `all_platforms` by name.
See [feldspar/AD0001](../feldspar/AD0001-factory-pattern-for-ui-extensibility.md) for the worker engine's role in this chain.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-03-13 13:26:44) Danielle McCool: marked decision as decided
