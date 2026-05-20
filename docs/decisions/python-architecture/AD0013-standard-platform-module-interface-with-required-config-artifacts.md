---
adr_id: "0013"
comments: []
links:
    precedes:
        - "0012"
    succeeds: []
status: accepted
date: 2026-05-20
tags:
    - platform-dispatch
    - platform-interface
    - script
    - config
title: Standard platform module interface and config file contract
---

## Decision

`script.py` dispatches to platforms specific flows data donation flows. Given the platform name from `VITE_PLATFORM` (lowercased), it: (1) validates `configs/<platform>_config.json` via `port_config_validator.validate_or_raise`, failing immediately with a `pnpm generate-config <platform>` hint if absent; (2) imports `port.platforms.<platform>`; (3) calls `module.process(session_id)`. No changes to `script.py` are needed when adding a platform.

Every module in `port/platforms/` must expose four things: `EXTRACTOR_REGISTRY` (`dict[str, Callable]` mapping function-name strings to extractor callables), `extraction(zip_path, validation)` (builds `ZipArchiveReader`, loads the config, delegates to `run_extraction`), a `FlowBuilder` subclass wiring `validate_file` and `extract_data`, and `process(session_id)` returning `flow.start_flow()`.

Config files are generated and can be changed by hand to change texts for example, or omit extractors that are not needed for a study. Run `pnpm generate-config <platform>`: the script reads `port/platforms/<platform>.py`, pulls `Table config::` blocks from each extractor's docstring, and writes `port/configs/<platform>_config.json`. Each platform gets its own file; generating one never touches another. All files in `port/configs/` are included in the wheel via `pyproject.toml`.

## Working with the config

Config files live in `port/configs/<platform>_config.json`. The typical lifecycle is:

1. **Generate** — `pnpm generate-config <platform>` reads `Table config::` docstring blocks and writes the JSON file. This is the authoritative source of the initial config.
2. **Edit by hand** — the generated file is intentionally human-editable. Common reasons to hand-edit:
   - Change UI text (`title`, `description`, `headers`) without touching Python.
   - Remove a table entry entirely to exclude it from the study.
   - Add or reorder entries in `variables` to restrict which columns participants see.
   - Add or tune `visualizations` descriptors.
3. **Validate** — ``port_config_validator.validate_or_raise("<platform>")` validates the changes made by hand

### Required fields per table entry

| Field | Type | Description |
|---|---|---|
| `id` | string | Unique identifier used internally by the consent form. |
| `extractor` | string | Name of the callable in `EXTRACTOR_REGISTRY` to run. |
| `title` | `{lang: text}` | Translatable table title shown in the UI. |
| `description` | `{lang: text}` | Translatable table description shown in the UI. |
| `headers` | `{column: {lang: text}}` | Per-column translatable labels used as table headers. |

### Optional fields per table entry

| Field | Type | Default | Description |
|---|---|---|---|
| `variables` | list of strings | `null` (keep all) | Column names to **include** in the final DataFrame shown to the participant. Columns not present in the extractor's output are silently ignored. Use this to restrict a study to a subset of available columns without changing Python code. Example: `["date", "category"]` shows only those two columns. |
| `extractor_kwargs` | object | `{}` | Extra keyword arguments forwarded verbatim to the extractor function beyond the mandatory `reader` and `errors` parameters. Use this to parameterise a shared extractor for different table variants. |
| `visualizations` | list of objects | `[]` | Chart descriptors passed to `PropsUIPromptConsentFormTableViz`. Each object specifies `type` (`"bar"`, etc.), `group` (x-axis column), `title`, and `values` (list of `{label, column}` pairs). |
| `documentation` | object | absent | Human-readable metadata for data managers. Not used at runtime. Typical keys: `summary`, `source_file`, `columns` (per-column explanations). Populated automatically by `generate-config` from the extractor's docstring. |

### Example: restricting columns with `variables`

```json
{
  "id": "my_table",
  "extractor": "my_extractor",
  "title": {"en": "My table"},
  "description": {"en": "..."},
  "headers": {
    "date":     {"en": "Date"},
    "category": {"en": "Category"},
    "detail":   {"en": "Detail"}
  },
  "variables": ["date", "category"]
}
```

The extractor still runs in full; only the `date` and `category` columns are kept before the table is shown to the participant. The `detail` column is dropped without any Python change.

## Workflow

* **Dev:** `VITE_PLATFORM=<platform> pnpm start`
* **Single platform release:** `VITE_PLATFORM=<platform> pnpm release`
* **Full release:** `bash release.sh` — discovers platforms by globbing `port/configs/*_config.json`; no list is maintained in `release.sh`

## See also

`Table config::` docstring format and config generation details: [AD0012](AD0012-declarative-tableconfig-for-platform-extraction-scripts.md). `VITE_PLATFORM` wiring from `release.sh` through Vite to `script.py`: [fork-governance/AD0005](../fork-governance/AD0005-per-platform-release-builds-via-vite_platform-env-var.md). `FlowBuilder` pattern: [AD0006](AD0006-consolidate-donation_flows-and-platforms-into-single-extraction-architecture.md).
