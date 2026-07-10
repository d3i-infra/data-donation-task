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

# Keep framework, custom UI, and extraction in separate packages

## Decision

The repo is a pnpm-workspace monorepo whose three packages are kept separate by role and change rate — not merged into one, and not split further.

## Guidance

- Place new code by package role: framework → `feldspar` (rarely; see the alignment rule), custom UI → `data-collector`, extraction/validation → `packages/python` (the `port` distribution).
- Wire JS package dependencies through the pnpm workspace; deliver the Python package as the Poetry `port` wheel copied into `data-collector/public`. Don't collapse the boundaries to take a shortcut.

## Why

The three packages have different audiences and change rates — an upstream-tracked framework, study UI, and extraction logic — and the split mirrors upstream so framework syncs stay clean. Merging them would entangle upstream-tracked code with study code (breaking the alignment rule), and splitting further adds coordination cost for no boundary that isn't already expressible by role.
