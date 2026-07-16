---
status: accepted
date: "2026-07-01"
tags:
    - process
    - documentation
category: Governance
applies_to:
    - docs/decisions/**/*.md
priority: default
---

# Record architecture decisions as lean ADRs

## Decision

Architecture decisions for this repo are recorded as lean ADRs in `docs/decisions/` — compact Decision/Guidance records with routing frontmatter — authored and validated with the `adg lean` commands (`new` / `index` / `brief` / `check`), not as prose or full MADR.

## Guidance

- When you make a significant architectural choice (a new package boundary, a new pattern, a divergence from upstream), add a record with `adg lean new` before or alongside the implementing PR.
- Keep each record to one screen: the governing rule as Decision + Guidance with an `applies_to` that routes it to the files it governs; mark hard constraints `invariant`.
- Gate the model in CI with `adg lean index --root` (and `adg lean check`); the PreToolUse brief then surfaces the governing records at edit time.

## Why

The team authors code through Claude with a single architectural reviewer, so conventions have to reach a session *before* code is written and be machine-checkable — not left to prose nobody re-reads. Lean records compile into a routed, token-light brief and are validated by `adg lean index`; full MADR prose can neither route to the files it governs nor gate in CI, which is how architectural drift went uncaught.
