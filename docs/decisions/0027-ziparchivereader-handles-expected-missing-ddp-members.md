---
status: accepted
date: "2026-03-20"
tags:
    - extraction-helpers
    - ddp-compatibility
    - error-handling
category: Extraction
applies_to:
    - packages/python/port/helpers/extraction_helpers.py
    - packages/python/port/helpers/validate.py
    - packages/python/port/platforms/**/*.py
priority: default
companions:
    - packages/python/tests/test_zip_archive_reader.py
    - packages/python/tests/test_validate.py
---

# ZipArchiveReader handles expected-missing DDP members

## Decision

Platform extraction reads archive members through `ZipArchiveReader`. `json()` / `csv()` / `raw()` return result objects with `found`; missing expected files are skipped. Member resolution is exact path, then one path-boundary suffix match; ambiguous suffixes count `AmbiguousMemberMatch` and return not found. Validation caches `archive_members` once.

## Guidance

- Use `ZipArchiveReader` and branch on `result.found`; absence of an expected DDP file is not an extraction exception.
- Keep member matching deterministic and path-boundary-aware; do not restore regex or first-match suffix lookup.
- Reuse `ValidateInput.archive_members` instead of re-opening the zip just to list members during extraction.
