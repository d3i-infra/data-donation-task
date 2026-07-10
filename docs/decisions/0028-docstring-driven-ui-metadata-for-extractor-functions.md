---
status: accepted
date: "2026-05-20"
tags:
    - extraction
    - configuration
    - docstrings
category: Python architecture
applies_to:
    - packages/python/port/platforms/**/*.py
    - scripts/generate_port_config.py
priority: default
---

# Docstring-driven UI metadata for extractor functions

## Decision

Each extractor function carries its consent-UI table metadata in its docstring as two labelled JSON blocks — `Table documentation::` and `Table config::` — and `scripts/generate_port_config.py` builds the per-platform config by parsing them with `ast`, without importing the (Pyodide-dependent) platform module.

## Guidance

- Put each extractor's UI metadata in its docstring: `Table config::` (runtime metadata — id, title, description, headers, optional visualizations) and `Table documentation::` (developer summary + column descriptions). Omit the `extractor` field — the generator infers it from the function's key in `EXTRACTOR_REGISTRY`.
- Do not hand-inline table metadata (id/title/description/headers as literals) in `extraction()`, and do not reintroduce a `DEFAULT_TABLE_CONFIG`-style constant — the docstring is the single source of truth. Build `PropsUIPromptConsentFormTableViz` from the loaded config (`table_cfg.title`, …), not from literals.
- Keep the build-time generator `generate_port_config.py` AST-only: it must not `import port.platforms.*` (Pyodide-dependent), so desktop tooling (`dd-script-selector`, `dd-script-builder`) can read metadata outside the browser. This constrains the build-time metadata generator only — runtime code such as `port_config_validator.py` is out of this ADR's scope.
- `Table config::` blocks must be valid JSON; the generator is the validator (a JSON typo surfaces at generation, not through Python's type system).

## Why

A platform exposes up to ~30 extractor functions, each producing a consent-UI table that needs translatable metadata (id, title, description, headers, visualizations). That metadata must be co-located with its extractor — one docstring edit per wording change, visible in the same diff — instead of the previous inline-literal pattern that duplicated strings across platforms. And desktop tooling (`dd-script-selector`, `dd-script-builder`, the generator) must read it *without importing* the platform module, since `port.platforms.*` pulls in Pyodide-only code; an AST parse never imports. Costs: docstrings balloon (some modules pass 80 KB), JSON typos surface only when the generator runs, and nothing cross-checks header keys against the columns actually emitted.

## Checks

- Confirm every extractor in `port/platforms/*` carries both a `Table config::` and a `Table documentation::` block; run `generate_port_config.py` (validate/dry-run) to catch missing or malformed blocks.
- Confirm `generate_port_config.py` imports no `port.platforms.*` (stays AST-only).
- Flag `PropsUIPromptConsentFormTableViz(` built from literal `title=` / `headers=` values rather than config-sourced ones.
