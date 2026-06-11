---
adr_id: "0012"
status: accepted
date: 2026-05-20
comments:
    - author: Danielle McCool
      date: "2026-04-23 00:00:00"
      comment: "Rewritten to reflect docstring-driven config generation replacing DEFAULT_TABLE_CONFIG_JSON"
tags:
  - extraction
  - configuration
  - docstrings
title: Docstring-driven UI metadata for extractor functions
links:
  succeeds: []
---

## Context and Problem Statement

Each platform module exposes 5-25 extractor functions, each producing a table shown to participants in the consent UI. Each table needs translatable UI metadata: id, title, description, per-column headers, optional visualizations. That metadata has to live somewhere. Where?

Before this PR, the metadata was inlined in each platform's `extraction()` functions, as a long list of `PropsUIPromptConsentFormTableViz()` constructor calls, duplicating UI strings across platforms and forcing edits to Python code for any text change. As the platform count grew, this became unmaintainable, and it blocked external tooling (selector, builder) from reading metadata without importing Pyodide-dependent code.

## Considered Options
 
1. Separate JSON files per extractor, mantained by hand alongside the Python source.
2. Python constants (DEFAULT_TABLE_CONFIG = {}) at the bottom of each platform module -- the previous in-progress pattern
3. Docstring-embedded JSON blocks parsed via AST by a build-time generator.

## Decision Drivers

- UI metadata must be co-located with the extractor function that produces the table.
- External tooling (dd-script-selector, dd-script-builder) must be able to parse metadata without importing the Python module (port.platforms.* imports Pyodide-only code, which fails outside the browser).
- A single source of truth: editing the docstring should be the only place a developer touches
- The metadata must be human-readable in code review.

## Decision Outcome

Chosen: Option 3 -- Docstring JSON blocks parsed via AST

Each extractor function carries two labelled JSON sections in its docstring: `Table documentation::` (developer-facing summary + column descriptions) and `Table config::` (runtime UI metadata: id, title, description, headers, optional visualizations). The extractor field is omitted from the block -- the generator infers it from the function's key in EXTRACTOR_REGISTRY.

scripts/generate_port_config.py parses these blocks via the ast module, so no Python import is required

## Consequences

- Good: One source of truth per extractor; metadata travels with the code.
- Good: AST-only parsing means desktop tooling (selector, builder, generator) doesn't load Pyodide-dependent modules
- Good: Code review sees both code and metadata in one diff
- Bad: Docstrings become long -- translations x header-columns x N extractors. Some platform modules cross 80KB.
- Bad: JSON syntax errors in docstrings don't surface using Python's type system
- Bad: Refactoring an extractor's column names does not propagate to the docstring's header keys; nothing cross-checks them.

