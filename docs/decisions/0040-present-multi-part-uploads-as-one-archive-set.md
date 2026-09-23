---
status: accepted
date: "2026-08-27"
category: Extraction
applies_to:
    - packages/python/port/helpers/archive_set.py
    - packages/python/port/helpers/extraction_helpers.py
    - packages/python/port/helpers/flow_builder.py
    - packages/python/port/helpers/uploads.py
    - packages/python/port/platforms/google.py
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

- Order parts by `(name, size)` — metadata only, never upload/selection order — so
  member resolution and duplicate-winner selection are deterministic. Parts tying
  on both are indistinguishable without a forbidden byte read, so their order is
  unspecified.
- The member inventory is the union of all parts' namelists, sorted by member path
  (separate from canonical part order). Canonical part order instead governs
  duplicate-winner resolution: the first part to declare a path owns it; a later
  part's same-path member is a duplicate, not a shadowing member.
- Count exact-duplicate members in a `Counter`, distinct per cause so neither
  inflates the other: `DuplicateMemberAcrossParts` (a path an earlier part already
  owns) vs. `DuplicateMemberWithinPart` (the zip format's legal same-path-twice
  inside one part, resolved to that part's *last* entry — plain `zipfile`
  semantics). `ZipArchiveReader` merges `duplicates` into the extraction `errors`
  counter.
- Never open all parts eagerly or hold them all in memory — `ArchiveSet.read_member()`
  opens only the owning part's `zipfile.ZipFile`, on demand, mirroring the
  streaming invariant at the set level.
- Enforce `MAX_MEMBER_UNCOMPRESSED_BYTES` at read/materialization time
  (`ArchiveSet.read_member` / `SingleArchiveSource.read_member`) from the zip's
  central-directory `file_size`, before decompressing — never after.
- `ArchiveSource` also carries `open_member(path)`, an additive streaming
  counterpart to `read_member`: it yields the owning part's decompression stream
  (a context manager over `IO[bytes]`) without materializing the member.
  Deliberately unguarded — the size guard bounds full-buffer decompression, while
  a streaming consumer bounds its own memory. `read_member`'s contract is
  unchanged; `open_member` is a second way in, never a replacement.
- Process parts sequentially: one part's `zipfile.ZipFile` context closes before
  the next opens; there is no concurrent multi-part extraction.
- `PayloadFiles` is `ArchiveSet`'s only transport; `ArchiveSource`,
  `SingleArchiveSource`, and `ArchiveSet` all live in `archive_set.py`.

## Why

A multi-file DDP (e.g. a Google Takeout export split into numbered parts) is the
same logical archive as a single-zip DDP to the researcher and to `ZipArchiveReader`
— a platform module should not need to know whether it is reading one zip or five.
Without canonical ordering, member and duplicate-winner resolution would depend on
nondeterministic upload order. Counting rather than raising or overwriting keeps
chunked-export duplicates (e.g. an overlap file repeated across two Takeout parts)
visible without treating them as fatal; within-part duplicates get their own
counter for the same reason, though real DDP exports show none — a defensive path,
not a load-bearing one. Guarding size at materialization, not inventory time,
keeps upload-time checks metadata-only while still bounding decompression-bomb
risk before a member is read fully into memory.
