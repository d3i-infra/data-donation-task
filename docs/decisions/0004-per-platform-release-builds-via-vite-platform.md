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
    - .gitignore
    - packages/python/poetry.lock
    - packages/data-collector/src/App.tsx
    - packages/feldspar/src/components/script_host_component.tsx
    - packages/feldspar/src/framework/assembly.ts
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
    - desc: release.sh probes for the Python validation environment instead of creating one
      grep: 'env info --executable'
      in: ["release.sh"]
      expect: present
    - desc: release.sh runs the validator on the resolved interpreter, never through poetry run
      grep: 'poetry run python'
      in: ["release.sh"]
      expect: absent
---

# Per-platform release builds via VITE_PLATFORM

## Decision

Per-platform deployment builds are produced by `release.sh`, which loops setting `VITE_PLATFORM` over the platforms discovered by globbing `packages/python/port/configs/*_config.json`. `App.tsx` reads that env var once and threads it onward as an explicit `platform` prop through the worker to Python — the prop chain is the only source. Researcher forks run `release.sh` to produce their own deployment zips.

## Guidance

- Produce deployable per-platform zips with `release.sh` (one per platform); don't add runtime platform detection in Python — `VITE_PLATFORM` is fixed at build time.
- The platform list is derived from `configs/`: adding a platform to a release means generating its config (`pnpm generate-config <platform>`), never editing a hardcoded list in `release.sh`.
- A `cp -r` copy of a *provisioned* checkout must validate and build with no network access and no state left behind at the original path: the in-project `packages/python/.venv` rides the copy, and `release.sh` only **probes** for that environment (`packages/python/.venv` first, else `poetry env info --executable`, then the validator's own import chain) and fails fast with the remedy — it never installs. It then runs the validator with that same resolved interpreter, never `poetry run`: a second resolution can disagree with the probe (under `virtualenvs.in-project = false` poetry ignores the `.venv`), and `poetry run` in an unprovisioned tree silently creates an *empty* environment rather than erroring. Don't add `poetry install`, `pip install`, or any other fetch to the release path; dd-script-builder runs the release from exactly such a copy with the network closed.
- Provisioning that environment belongs to whoever owns the checkout, never to `release.sh`: a development machine runs `poetry install` in `packages/python`; the VM's checkout is provisioned by the deployment repo's data-donation-task sync unit, which re-clones daily and creates `packages/python/.venv` *explicitly* before `poetry install --no-root --only main`. The explicit venv is load-bearing — poetry keys cached environments by project path, so an implicitly created one lands in the global cache where no copy of the tree can find it. Fix a broken release environment there, not by making `release.sh` self-heal.
- `packages/python/poetry.lock` is committed and must stay out of `.gitignore` — it is what makes that provisioning reproducible across the dev machine, the VM, and every build copy. It governs *tooling and validation* environments only: the package versions the participant-facing runtime gets are dictated by Pyodide, not by this lock, so a lock bump is never a runtime upgrade. `.venv/` stays ignored — it is machine-local state, not an artifact.
- Preserve the platform thread — `release.sh → App.tsx (\`VITE_PLATFORM\` read) → ScriptHostComponent \`platform\` prop → Assembly → WorkerProcessingEngine → py_worker.js \`data\` ctx → main.py \`start\` → script.py` — when touching any of those files. `App.tsx`'s `platform={import.meta.env.VITE_PLATFORM}` is the thread's single sanctioned origin — the one intentional first-party env read, substituted in both dev and build. Downstream of `App.tsx` the value travels only by prop and constructor argument: no layer re-reads the environment, and there is no fallback if the prop is absent. `packages/feldspar` contains no `import.meta` and no `VITE_*` read at all, matching upstream `eyra/feldspar` (its one `process.env.NODE_ENV` read in `script_host_component.tsx` is identical to upstream's own and is not part of this thread).
- `VITE_PLATFORM` is required — `check-deps.sh` guards dev mode, and a bundle built without a platform is *invalid*: it must fail explicitly, never an unhandled traceback. The explicit failure is `script.py`'s `if not platform: raise ValueError(...)`, raised on the generator's first `send()` so `ScriptWrapper` turns it into the consent-gated error page. Don't paper over a missing platform with a default anywhere along the thread.
- Don't reintroduce the removed Earthly build pipeline (`_build_release.yml`, `forbids`); `gh-pages.yml` validates the template build. (The separate `release.yml` — a GitHub release on a `v*` tag from CHANGELOG — is unrelated to per-platform deployment.)

## Why

Eyra Next deploys one workflow instance per platform — its own assignment, its own uploaded zip — so a single multi-platform bundle cannot be deployed, and Python needs the platform identity to pick extraction logic. One build-time env var is the simplest mechanism that works without CI (releases run locally; the Earthly pipeline this replaced was long dead). A runtime selector was rejected: nothing at runtime should decide what a deployed study extracts. Deriving the platform list from `configs/` fixed the drifted hardcoded list — generating a config is now the single registration step. Threading `platform` as an explicit prop from a single env read (rather than letting every layer re-read the environment) makes the value host-configurable and testable without a real Vite build, and keeps `packages/feldspar` env-free so it stays mergeable with upstream; a second, lower-precedence env read would only hide a broken thread behind a stale build-time value. Costs: N builds per release, N hand-maintained config files; and the scheme still rests on the invalid-build contract — an unset-platform bundle must fail loudly, not show a participant a traceback. The probe-don't-install rule exists because the release runs in three places with three different owners of state — a developer's checkout, a network-free `cp -r` build copy, and a VM checkout wiped and rebuilt nightly — and a script that provisions works in exactly one of them while quietly corrupting or fighting the other two; probing is the only behavior that is correct everywhere, and the committed lock is what makes the three provisioned environments the same environment.
