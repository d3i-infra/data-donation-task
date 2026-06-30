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
    - packages/feldspar/src/framework/processing/worker_engine.ts
    - packages/data-collector/public/py_worker.js
    - packages/python/port/main.py
    - packages/python/port/script.py
priority: default
forbids:
    - .github/workflows/_build_release.yml
checks:
    - desc: release.sh builds per-platform via VITE_PLATFORM
      grep: 'VITE_PLATFORM'
      in: ["release.sh"]
      expect: present
---

# Per-platform release builds via VITE_PLATFORM

## Decision

Per-platform deployment builds are produced by `release.sh`, which loops setting `VITE_PLATFORM`; the value threads `release.sh → worker_engine.ts → py_worker.js → main.py → script.py`, giving the Python layer platform identity at build time. Researcher forks run `release.sh` to produce their own deployment zips.

## Guidance

- Produce deployable per-platform zips with `release.sh` (one per platform); don't add runtime platform detection in Python — `VITE_PLATFORM` is fixed at build time.
- Preserve the `VITE_PLATFORM` thread when touching `worker_engine.ts` / `py_worker.js` / `main.py` / `script.py`.
- Don't reintroduce the removed Earthly build pipeline (`_build_release.yml`, `forbids`); `gh-pages.yml` validates the template build. (The separate `release.yml` — a GitHub release on a `v*` tag from CHANGELOG — is unrelated to per-platform deployment.)
