---
status: accepted
date: "2026-03-20"
tags:
    - extraction-helpers
    - ddp-compatibility
    - error-handling
category: Extraction
applies_to:
    - packages/python/port/helpers/archive_set.py
    - packages/python/port/helpers/extraction_helpers.py
    - packages/python/port/helpers/validate.py
    - packages/python/port/platforms/**/*.py
priority: default
companions:
    - packages/python/tests/test_zip_archive_reader.py
    - packages/python/tests/test_validate.py
    - packages/python/tests/test_archive_set.py
checks:
    - desc: platforms do not use the legacy extract_file_from_zip helper
      grep: 'extract_file_from_zip'
      in: ["packages/python/port/platforms/**"]
      expect: absent
    - desc: platforms do not use the legacy json_dumper helper
      grep: 'json_dumper'
      in: ["packages/python/port/platforms/**"]
      expect: absent
    - desc: platforms do not use the legacy read_json_from_file helper
      grep: 'read_json_from_file'
      in: ["packages/python/port/platforms/**"]
      expect: absent
---

# ZipArchiveReader handles expected-missing DDP members

## Decision

Platform extraction reads archive members through `ZipArchiveReader`. `json()` / `json_all()` / `csv()` / `raw()` return result objects with `found`; a missing expected file is skipped, not raised. `ZipArchiveReader` consumes an `ArchiveSource` (`SingleArchiveSource` for one archive, `ArchiveSet` for N uploaded parts — ADR-0040) rather than opening a zip itself; expected-missing semantics are unchanged regardless of which source backs it.

## Guidance

- Use `ZipArchiveReader` and branch on `result.found`; absence of an expected DDP file is not an extraction exception and must not increment the error counter. This holds identically for a single-archive upload and a multi-part `ArchiveSet`.
- The converse holds too: a member that is *found* but fails to read or parse is a real failure and must count in the shared `errors` counter — for `json()`, `csv()`, and `raw()` alike, so researchers can tell a broken file from an absent one.
- Keep member matching deterministic and path-boundary-aware: exact path first, then one path-boundary suffix match; ambiguous suffixes count `AmbiguousMemberMatch` and return not found. Do not restore regex or first-match suffix lookup.
- Inventory discovery — resolving the member list a `ZipArchiveReader` matches against — lives in `ArchiveSet`/`SingleArchiveSource` (`archive_set.py`), not in `ZipArchiveReader`; the reader only resolves member paths and delegates reads to `self._source.read_member()`. Reuse the already-discovered member list (`ValidateInput.archive_members` / `ArchiveSet.members`) instead of re-opening any part just to list members during extraction.
- Do not reach for the legacy path-era helpers (`extract_file_from_zip`, `json_dumper`, `read_json_from_file`) in extraction code — they are the cascade this decision removed and survive only for backward compatibility.
- WhatsApp is the standing exception: its input is a single chat export pre-parsed into a DataFrame, not a multi-file DDP, so it has no member inventory to consult.

## Why

DDPs vary by version, language, and download options, so expected files are routinely absent — absence is normal, not an error. The old helpers cascaded ~4 error lines per missing file (a three-year Facebook DDP: ~31K console lines, 559 inflated error counts), burying real failures and hiding broken-vs-absent from researchers. Found/not-found results kill the cascade at the source; a sentinel alone would still cascade through the parsers, and pre-filtering put existence checks in the wrong layer. Caching the member list reuses validation's walk (previously 25+ zip re-opens per Facebook extraction), and exact-then-path-boundary resolution replaced a regex match that could extract the wrong file. Layering: discovery lives in `validate.py` for a single archive or `ArchiveSet` for a multi-part upload (ADR-0040), the reader resolves member paths against whichever `ArchiveSource` it is given, the platform parses.
