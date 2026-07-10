---
status: accepted
date: "2026-03-13"
tags:
    - release
    - vite-platform
    - build
category: Fork governance
applies_to:
    - release.sh
    - check-deps.sh
    - packages/feldspar/src/framework/processing/worker_engine.ts
    - packages/data-collector/public/py_worker.js
    - packages/python/port/main.py
    - packages/python/port/script.py
priority: default
forbids:
    - .github/workflows/_build_release.yml
checks:
    - desc: release.sh builds per-platform via VITE_PLATFORM
      grep: 'export VITE_PLATFORM'
      in: ["release.sh"]
      expect: present
---

# Per-platform release builds via VITE_PLATFORM

## Decision

Per-platform deployment builds are produced by `release.sh`, which loops setting `VITE_PLATFORM` over the platforms discovered by globbing `packages/python/port/configs/*_config.json`, threading one build-time env var from the build through the worker to the Python layer. Researcher forks run `release.sh` to produce their own deployment zips.

## Guidance

- Produce deployable per-platform zips with `release.sh` (one per platform); don't add runtime platform detection in Python — `VITE_PLATFORM` is fixed at build time.
- The platform list is derived from `configs/`: adding a platform to a release means generating its config (`pnpm generate-config <platform>`), never editing a hardcoded list in `release.sh`.
- Preserve the `VITE_PLATFORM` thread — `release.sh → worker_engine.ts → py_worker.js → main.py → script.py` — when touching any of those files. `VITE_PLATFORM` is required — `check-deps.sh` guards dev mode, and a bundle built without a platform is *invalid*: it must fail explicitly (build-time refusal, or a clear participant-facing message), never an unhandled traceback.
- Don't reintroduce the removed Earthly build pipeline (`_build_release.yml`, `forbids`); `gh-pages.yml` validates the template build. (The separate `release.yml` — a GitHub release on a `v*` tag from CHANGELOG — is unrelated to per-platform deployment.)

## Why

Eyra Next deploys one workflow instance per platform — its own assignment, its own uploaded zip — so a single multi-platform bundle cannot be deployed, and Python needs the platform identity to pick extraction logic. One build-time env var is the simplest mechanism that works without CI (releases run locally; the Earthly pipeline this replaced was long dead). A runtime selector was rejected: nothing at runtime should decide what a deployed study extracts. Deriving the platform list from `configs/` fixed the drifted hardcoded list — generating a config is now the single registration step. Costs: N builds per release, N hand-maintained config files, and the scheme rests on the invalid-build contract — an unset-platform bundle (`platform: void 0` from the script builder) must fail loudly, not show a participant a traceback.
