---
status: accepted
date: "2026-04-30"
tags:
    - uploads
    - streaming
    - memory-safety
    - file-api
category: Extraction
applies_to:
    - packages/python/port/main.py
    - packages/python/port/api/file_utils.py
    - packages/python/port/helpers/flow_builder.py
    - packages/python/port/helpers/uploads.py
    - packages/python/port/helpers/validate.py
    - packages/python/port/helpers/extraction_helpers.py
    - packages/python/port/helpers/archive_set.py
    - packages/python/tests/test_uploads.py
    - packages/python/tests/test_flow_builder.py
    - packages/python/tests/test_main_queue.py
priority: invariant
companions:
    - packages/python/tests/test_validate.py
    - packages/python/tests/test_zip_archive_reader.py
    - packages/python/tests/test_archive_set.py
checks:
    - desc: no materialize_file resurrection anywhere in the Python package
      grep: 'materialize_file'
      in: ["packages/python/**"]
      expect: absent
    - desc: ScriptWrapper still handles the singular PayloadFile case (word-boundary, excludes PayloadFiles)
      grep: '\bPayloadFile\b'
      in: ["packages/python/port/main.py"]
      expect: present
    - desc: ScriptWrapper handles the PayloadFiles (multi-file) case
      grep: '\bPayloadFiles\b'
      in: ["packages/python/port/main.py"]
      expect: present
---

# Stream PayloadFile uploads without materializing

## Decision

The upload pipeline passes browser `PayloadFile` / `PayloadFiles` readers directly to validators and extractors, never a materialized path or bytes object. `ScriptWrapper` wraps every upload reader it receives — one for a `PayloadFile`, one per file of a `PayloadFiles` set — once in its own `AsyncFileAdapter` at that boundary; consumers stay typed against `SeekableBinaryReader`. `PayloadString` / WORKERFS upload support stays retired.

## Guidance

- Do not add `materialize_file()` or any whole-upload read on the upload path; never call `read()` with no argument or `-1` on an upload adapter — this applies per file when handling a `PayloadFiles` set, not just once per upload. Never collapse a `PayloadFiles` set into one adapter or one bulk in-memory copy — each file gets its own `AsyncFileAdapter`.
- Pass the adapter directly to `zipfile.ZipFile`, `validate_zip()`, and `ZipArchiveReader`; read size from `adapter.size` (JS metadata, no bytes) — for a `PayloadFiles` set, each element carries its own size independently.
- The same no-materialize, bounded-read rule binds `archive_set.py`'s per-part reads (`ArchiveSet.read_member` / `SingleArchiveSource.read_member`): each opens its owning part's `zipfile.ZipFile` on demand and reads one guarded member at a time, never the whole part.
- Type upload consumers against the `SeekableBinaryReader` Protocol in `file_utils.py`, never `str` paths — a path parameter in the upload pipeline implies materialization and is review-rejected.
- Keep the tests proving `zipfile` uses bounded reads (`TestStreamingInvariant`), that `FlowBuilder` accepts `PayloadFile` for single uploads, and `TestPayloadFilesWrapping` (`test_main_queue.py`) proving `ScriptWrapper` wraps each `PayloadFiles` reader into its own `AsyncFileAdapter` rather than one adapter over the whole list.

## Why

Production bug #61: `materialize_file()` read whole uploads, and `FileReaderSync.readAsArrayBuffer()` rejects above the DOM's ~2 GiB cap regardless of RAM — routine multi-GiB takeouts crashed. `zipfile.ZipFile` only needs a seekable file-like and chunks its own reads, so passing the adapter straight through removes the failure class; the rejected `/tmp` copy would still land the file in the worker heap (Pyodide's `/tmp` is in-memory) and keep `materialize_file` alive as a regression target. Deleting the function is the structural enforcement; `TestStreamingInvariant` (no `read(-1)`) is the behavioral one. The 2 GiB upload cap is not a contradiction: streaming removed the *mechanism* ceiling, while the cap is a deliberate *policy* guard that this decision moved upstream to metadata rather than deleting. The change also closed the PayloadString/WORKERFS retirement — forks still on WORKERFS migrate before consuming this version.

Pluralizing to `PayloadFiles` (multi-file uploads) keeps the same invariant per file rather than carving out an exception for the set: each reader is independently on-demand (`readSlice`/`size`/`name`), so wrapping the whole list in a single adapter — or reading one file's bytes to get to the next — would reopen bug #61 one file at a time instead of once. Wrapping per-reader at the `ScriptWrapper` boundary keeps the streaming guarantee uniform regardless of how many files a platform's upload prompt collects.
