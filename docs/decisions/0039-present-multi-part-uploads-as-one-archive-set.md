---
status: accepted
date: "2026-08-27"
category: Extraction
applies_to:
    - packages/python/port/helpers/archive_set.py
    - packages/python/port/helpers/extraction_helpers.py
    - packages/python/port/helpers/flow_builder.py
    - packages/python/port/helpers/uploads.py
priority: invariant
companions:
    - packages/python/tests/test_archive_set.py
    - packages/python/tests/test_zip_archive_reader.py
---

# Present multi-part uploads as one archive-set

## Decision

A multi-file upload (`PayloadFiles`) is treated as one logical archive-set, not N
independent zips: `ArchiveSet` orders parts canonically by `(name, size)`, builds
one union member inventory with per-member provenance, and serves per-part reads
on demand behind the same `ArchiveSource` protocol single-file uploads already
satisfy via `SingleArchiveSource`.

## Guidance

- Order parts by `(name, size)` — both metadata, never a byte read, and never
  upload/selection order — so member resolution and duplicate-winner selection
  are deterministic across the whole set regardless of how parts were passed in.
  Parts tying on both name and size are indistinguishable without reading
  bytes (forbidden pre-validation), so their relative order is unspecified.
- Build the member inventory as the union of all parts' namelists in canonical
  order; the first part (by canonical order) to declare a path owns it — a
  later part's same-path member is a duplicate, not a shadowing member.
- Count exact-duplicate members in a `Counter`, keeping across-part and
  within-part duplicates distinct so neither inflates the other:
  `DuplicateMemberAcrossParts` for a path an earlier part already owns;
  `DuplicateMemberWithinPart` for the zip format's legal same-path-twice
  inside one part (resolves to that part's *last* entry — plain `zipfile`
  semantics, not reimplemented). `ZipArchiveReader` merges the whole
  `duplicates` counter into the extraction `errors` counter (ADR-0024).
- Never open all parts eagerly or hold them all in memory at once —
  `ArchiveSet.read_member()` opens only the owning part's `zipfile.ZipFile`, on
  demand, mirroring the streaming invariant (ADR-0026) at the set level.
- Enforce `MAX_MEMBER_UNCOMPRESSED_BYTES` at read/materialization time
  (`ArchiveSet.read_member` / `SingleArchiveSource.read_member`), from the zip's
  central-directory `file_size`, before decompressing — never after.
- Process parts sequentially: one part's `zipfile.ZipFile` context closes before
  the next opens; there is no concurrent multi-part extraction.
- `PayloadFiles` (ADR-0017) is `ArchiveSet`'s only transport. There is no separate
  `ArchiveSource` protocol record — `ArchiveSource`, `SingleArchiveSource`, and
  `ArchiveSet` all live in `archive_set.py`.

## Why

A multi-file DDP (e.g. a Google Takeout export split into numbered parts) is the
same logical archive as a single-zip DDP to the researcher and to `ZipArchiveReader`
— a platform module should not need to know whether it is reading one zip or five.
Without a canonical part order, member resolution and duplicate-winner selection
would depend on browser file-picker / upload order, which is neither deterministic
across attempts nor meaningful DDP structure. First-part-wins-with-a-counter (rather
than raising or silently overwriting) keeps chunked-export duplicates — e.g. an
overlap file repeated across two consecutive Takeout parts — visible to researchers
without treating them as fatal. Within-part duplicates get their own counter for
the same reason, without reimplementing `zipfile`'s central-directory resolution
to force first-entry semantics — real DDP exports show zero such duplicates, so
this is a defensive observability path, not a load-bearing one. Guarding size at
materialization, not at inventory time, keeps the earlier metadata-only upload
checks (ADR-0018) honest: inventory
discovery only reads each part's central directory, never member bytes, while the
guard still bounds decompression-bomb risk before any single member is read fully
into memory.
