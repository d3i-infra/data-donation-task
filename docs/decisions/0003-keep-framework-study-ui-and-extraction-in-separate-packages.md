---
status: accepted
date: "2026-03-13"
tags:
    - monorepo
    - package-boundaries
category: Fork governance
applies_to:
    - pnpm-workspace.yaml
    - packages/feldspar/package.json
    - packages/data-collector/package.json
    - packages/python/pyproject.toml
priority: default
---

# Keep framework, study UI, and extraction in separate packages

## Decision

The repo is a pnpm-workspace monorepo whose three packages are kept separate by role and change rate — not merged into one, and not split further.

## Guidance

- Place new code by package role: framework → `feldspar` (rarely; see the alignment rule), study UI → `data-collector`, extraction/validation → `python`.
- Wire cross-package dependencies through the pnpm workspace; don't collapse the boundaries to take a shortcut.
