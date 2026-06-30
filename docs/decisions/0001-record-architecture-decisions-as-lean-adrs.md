---
status: accepted
date: "2026-03-13"
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
