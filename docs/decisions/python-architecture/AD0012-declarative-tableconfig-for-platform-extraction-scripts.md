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

## Decision

All platform extraction scripts live in `port/platforms/`. Each file (e.g. `chatgpt.py`, `facebook.py`, `instagram.py`, `linkedin.py`, `netflix.py`, `tiktok.py`, `whatsapp.py`, `x.py`, `youtube.py`, `chrome.py`) is a self-contained module for one data source. An `example.py` is included as a template and reference for adding new platforms.
The purpose of this folder is to concentrate all platform-specific knowledge — which files to read from a donation, how to parse them, and what tables to expose in the consent UI — in one place, separate from the shared extraction infrastructure in `port/helpers/`. Each module registers its extractor functions in an `EXTRACTOR_REGISTRY` and provides a thin `extraction()` entry point; the shared helpers handle the rest.

Table UI metadata lives in extractor docstrings. Each extractor function carries two JSON blocks: `Table documentation::` (human-readable summary, source file, column descriptions) and `Table config::` (the UI config with `id`, `title`, `description`, `headers`, optional `visualizations` — all translatable). The `extractor` field is omitted from the docstring; the generator infers it from the function's key in `EXTRACTOR_REGISTRY`.

`pnpm generate-config <platform>` runs `scripts/generate_port_config.py`, which reads the platform module as source text (no import), walks `EXTRACTOR_REGISTRY` key order, extracts each `Table config::` block, injects `"extractor": fn_name`, and writes `port/configs/<platform>_config.json` as `{"platform_info": {...}, "tables": [...]}`. Generating one platform never touches another's file.

`table_extractor.load_port_config(registry, platform)` reads that file and raises with an actionable message if absent — no fallback to any embedded default. `table_extractor.run_extraction(reader, errors, config)` is the shared extraction runner: it iterates `list[TableConfig]`, calls each extractor, builds `PropsUIPromptConsentFormTableViz` tables, and returns an `ExtractionResult` with only non-empty tables. Platform modules call both via a thin `extraction()` wrapper.

`port_config_validator.validate_or_raise(platform)` checks JSON validity, top-level schema, per-table required/optional fields, extractor names against `EXTRACTOR_REGISTRY`, extractor uniqueness, and table ID uniqueness. A registry key absent from the config is a non-fatal warning; a config key absent from the registry is an error.

## What was removed

`DEFAULT_TABLE_CONFIG_JSON` and `DEFAULT_TABLE_CONFIG` constants, `resolve_config(default_json, registry)`, `resolve_platform()` (platform name now comes from `platform_info.name` in the config), and `extractor_config.py` (merged into `table_extractor.py`).


## See also

Dispatch, module interface contract, and dev/release workflow: [AD0013](AD0013-standard-platform-module-interface-with-required-config-artifacts.md). `FlowBuilder` pattern: [AD0006](AD0006-consolidate-donation_flows-and-platforms-into-single-extraction-architecture.md).
