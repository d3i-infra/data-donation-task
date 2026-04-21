---
adr_id: "0012"
comments:
    - author: Niek de Schipper
      date: "2026-04-23 00:00:00"
      comment: "Rewritten to reflect docstring-driven config generation replacing DEFAULT_TABLE_CONFIG_JSON"
links:
    precedes: []
    succeeds:
        - "0006"
status: accepted
tags:
    - extraction
    - configuration
    - helpers
    - consent-ui
title: Docstring-driven TableConfig generation for platform extraction scripts
---

## <a name="question"></a> Context and Problem Statement

Platform scripts (such as `instagram.py`) originally hardcoded all UI metadata as `PropsUIPromptConsentFormTableViz` constructor calls inside `extraction()`.  A first iteration introduced `DEFAULT_TABLE_CONFIG_JSON` — a large JSON string constant at the bottom of each platform module — and `resolve_config` which fell back to that constant when no external `port_config.json` was present.

This left table metadata duplicated: once in the extractor docstring (human-readable) and once in `DEFAULT_TABLE_CONFIG_JSON`.  
It also meant `port_config.json` was optional, so the system could silently run with stale or incorrect config.

## <a name="criteria"></a> Decision Drivers

* Single source of truth — table metadata lives with the extractor function, not in a separate constant.
* `port_config.json` is a required build artifact, not an optional override.
* Researchers generate a correct `port_config.json` from code
* Researcher can change `port_config.json` to tweak it to their liking
* Build-time validation catches typos and registry mismatches before runtime.

## <a name="options"></a> Considered Options

1. Keep `DEFAULT_TABLE_CONFIG_JSON` as the fallback — status quo
2. Move config into extractor docstrings; generate `port_config.json` at build time; require it at runtime

## <a name="outcome"></a> Decision Outcome

Chosen option: **Option 2 — Docstring-driven generation**.

### Docstring format

Each extractor function carries two JSON sections in its docstring:

**`Table documentation::`** — human-readable metadata for developers and external tooling:

```
Table documentation::

    {
      "summary": "...",
      "source_file": "filename.json",
      "columns": {
        "ColumnName": "Description."
      }
    }
```

**`Table config::`** — UI config consumed by the generator:

```
Table config::

    {
      "id": "instagram_followers",
      "title": {"en": "...", "nl": "..."},
      "description": {"en": "...", "nl": "..."},
      "headers": {
        "Account": {"en": "Account", "nl": "Account"}
      }
    }
```

The `extractor` field is omitted from the docstring JSON — the generator infers it from the function name in `EXTRACTOR_REGISTRY`.

### Build-time tooling

`scripts/generate_port_config.py <platform>`:
- Reads `port/platforms/<platform>.py` as source text (no import needed).
- Extracts `EXTRACTOR_REGISTRY` key order and the `Table config::` JSON block from each extractor's docstring.
- Injects `"extractor": fn_name` into each entry.
- Writes `packages/python/port/port_config.json` with `{"platform": ..., "tables": [...]}`.

`scripts/gen_port_config.sh <platform>` — shell wrapper that invokes `generate_port_config.py`.
`pnpm generate-config <platform>` — calls the shell wrapper.

### Runtime behaviour

`table_extractor.load_port_config(registry)`:
- Reads `port_config.json`; raises `ImportError` with an actionable message if absent.
- No fallback to any embedded default.

`table_extractor.run_extraction(reader, errors, config)`:
- Generic extraction runner shared by all platform scripts.
- Iterates over a `list[TableConfig]`, calls each extractor, builds `PropsUIPromptConsentFormTableViz` tables, and returns an `ExtractionResult` containing only non-empty tables.
- Platform scripts (e.g. `instagram.py`) keep a thin `extraction()` wrapper that constructs the `ZipArchiveReader` and delegates to `run_extraction`.

`helpers/port_config_validator.py`:
- Validates `port_config.json` against the live platform module at startup.
- Checks JSON validity, top-level schema, per-table required/optional fields, extractor cross-check against `EXTRACTOR_REGISTRY`, extractor uniqueness, and table ID uniqueness.
- Warns (non-fatal) if a registry key is absent from the config.
- Also runs `load_port_config()` as a final runtime smoke-test.

`script.py`:
- Calls `_read_port_config()` on startup; exits with a clear error if `port_config.json` is missing or fails validation.

### What was removed

- `DEFAULT_TABLE_CONFIG_JSON` constant from `instagram.py`.
- `DEFAULT_TABLE_CONFIG` constant from `instagram.py`.
- `resolve_config(default_json, registry)` from `table_extractor.py`.
- `resolve_platform()` from `table_extractor.py` (replaced by `read_platform_from_port_config()`).
- `extractor_config.py` (renamed to `table_extractor.py` to reflect its combined responsibilities: `TableConfig` dataclass, config loaders, and the generic extraction runner).

### Consequences

* Good: single source of truth — metadata lives next to the extraction logic.
* Good: `port_config.json` is a required build artifact; the system never silently uses stale config.
* Good: build-time validation catches extractor name typos and duplicate entries before runtime.
* Good: adding a table requires only updating the docstring and `EXTRACTOR_REGISTRY`.
* Bad: only `instagram.py` migrated; other platform scripts remain inline, creating temporary inconsistency.
* Bad: `port_config.json` must be generated before the first run; new contributors need to know about `pnpm generate-config`.

## <a name="comments"></a> Comments

See [AD0006](AD0006-consolidate-donation_flows-and-platforms-into-single-extraction-architecture.md) for the FlowBuilder consolidation that established the `platforms/` + `FlowBuilder` pattern this ADR extends.
