---
adr_id: "0014"
status: accepted
date: 2026-05-22
tags:
    - configuration
    - lifecycle
    - generator
title: Config lifecycle and generator overwrite policy
links:
    supersedes: []
---

## Context and Problem Statement

`configs/<platform>_config.json` is initially generated from extractor docstrings via `pnpm generate-config <platform>`. After generation, researchers routinely customize the file: editing titles and descriptions, removing tables they don't want, restricting variables to a subset of available columns, reordering for participant-facing display.

This creates a fundamental question: is the docstring the source of truth (in which case the JSON should be regenerated freely), or is the JSON the source of truth (in which case generation should be a one-time bootstrap)?

## Considered Options

1. Docstring is source of truth (Model A). Generator always overwrites the JSON. Researchers re-edit after every regeneration, or maintain edits out-of-tree.
2. JSON is source of truth after initial generation (Model B). Generator refuses to overwrite existing files. Researchers must explicitly `rm` to re-bootstrap.
3. Merge-on-regenerate (Model B with merge). Generator detects existing file, adds new tables (from new extractors) but preserves edited entries.

## Decision Drivers

- Researcher curation represents the study design — protocols, IRB-approved content, hand-translated text. It must not be silently destroyed.
- The bootstrap workflow must remain frictionless for first-time platform setup.
- The selector and hand-editing paths produce JSON that should be treated identically by the runtime.
- Merge logic (Option 3) is complex and introduces subtle failure modes (how do we know which entries are "the researcher's" vs "the generator's"?).

## Decision Outcome

Chosen: Option 2 — JSON is the source of truth after generation; generator refuses to overwrite.

`scripts/generate_port_config.py`:

```python
if output_path.exists():
    print(f"ERROR: Config already exists: {output_path}", file=sys.stderr)
    sys.exit(1)
```

To re-bootstrap a platform, the researcher must explicitly `rm configs/<platform>_config.json` first. This is deliberately a two-step action.

The hand-editable surface includes: title, description, headers, variables (subset and order), table inclusion/exclusion, visualizations. See `packages/python/port/configs/example_config.json` as the canonical reference for the config schema.

## Consequences

- Good: Researcher edits are protected by default. The Selector workflow and hand-editing workflow produce equally durable artifacts.
- Good: `pnpm generate-config` is safe to run repeatedly — it either creates the file or no-ops with an explicit error.
- Bad: Adding a new extractor to a platform module does not propagate to existing configs without explicit rm-and-regenerate. Researchers must notice the addition manually.
- Bad: Re-bootstrapping a platform requires two steps (`rm`, then regenerate), making "I want to start over" mildly inconvenient.
- Bad: There is no merge tooling. A platform that gains a new extractor cannot be reflected in an existing config without losing all curation in that file.
