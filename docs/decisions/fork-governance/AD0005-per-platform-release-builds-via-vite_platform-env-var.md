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
* Bad: Release takes 7× the build time of a single build
* Bad: Branch names with `/` must be sanitised to `-` before use in zip filenames (known issue, handled in release.sh)

## More Information

The wiring: `release.sh` sets `VITE_PLATFORM` → Vite embeds it → `worker_engine.ts` reads `import.meta.env.VITE_PLATFORM` → passes to `py_worker.js` → `main.py` receives it → `script.py` filters `all_platforms` by name.
See [feldspar/AD0001](../feldspar/AD0001-factory-pattern-for-ui-extensibility.md) for the worker engine's role in this chain.

## <a name="amendment-2026-05-20"></a> Amendment — 2026-05-20: per-platform config files and VITE_PLATFORM required in dev

### What changed

**Before:** `release.sh` held a hardcoded list of platforms (`platforms=("LinkedIn" "Instagram" ...)`).  Dev mode (no `VITE_PLATFORM`) silently ran all platforms.

**Now:**

- Each platform has its own config file: `port/configs/<platform>_config.json`, written by `pnpm generate-config <platform>`.
- `release.sh` discovers which platforms to build by globbing `packages/python/port/configs/*_config.json`.  No hardcoded platform list is maintained in `release.sh` — adding a platform to a release requires only generating its config file.
- `VITE_PLATFORM` is required in dev mode.  Start a single platform with `VITE_PLATFORM=<platform> pnpm start`.  If `VITE_PLATFORM` is not set, or is set to a platform whose config file does not exist, an error is emitted in the study UI with a hint to run `pnpm generate-config <platform>`.
- `script.py` no longer reads a fallback `port_config.json`; the platform name must arrive via `VITE_PLATFORM`.

### Updated consequences

* Good: Produces one deployable zip per platform from one `bash release.sh` invocation; platform list is derived automatically from the `configs/` folder
* Good: Adding a platform to the release requires only generating its config — no `release.sh` edits needed
* Good: `VITE_PLATFORM=<platform> pnpm release` builds and zips only that one platform, avoiding the N× build cost when only one platform needs releasing

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-03-13 13:26:44) Danielle McCool: marked decision as decided
