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
    - packages/python/tests/test_uploads.py
    - packages/python/tests/test_flow_builder.py
priority: invariant
companions:
    - packages/python/tests/test_validate.py
    - packages/python/tests/test_zip_archive_reader.py
checks:
    - desc: no materialize_file resurrection anywhere in the Python package
      grep: 'materialize_file'
      in: ["packages/python/**"]
      expect: absent
---

# Stream PayloadFile uploads without materializing

## Decision

The upload pipeline passes browser `PayloadFile` / `AsyncFileAdapter` objects directly to validators and extractors. `ScriptWrapper` wraps the incoming JS reader in `AsyncFileAdapter` once at the boundary; consumers pass the adapter to `zipfile.ZipFile` instead of materializing a filesystem path or bytes object. `PayloadString` / WORKERFS upload support stays retired.

## Guidance

- Do not add `materialize_file()` or any whole-upload read on the upload path; never call `read()` with no argument or `-1` on an upload adapter.
- Pass the adapter directly to `zipfile.ZipFile`, `validate_zip()`, and `ZipArchiveReader`; read size from `adapter.size` (JS metadata, no bytes).
- Type upload consumers against the `SeekableBinaryReader` Protocol in `file_utils.py`, never `str` paths — a path parameter in the upload pipeline implies materialization and is review-rejected.
- Keep the tests proving `zipfile` uses bounded reads (`TestStreamingInvariant`) and that `FlowBuilder` accepts `PayloadFile` only.

## Why

Production bug #61: `materialize_file()` read whole uploads, and `FileReaderSync.readAsArrayBuffer()` rejects above the DOM's ~2 GiB cap regardless of RAM — routine multi-GiB takeouts crashed. `zipfile.ZipFile` only needs a seekable file-like and chunks its own reads, so passing the adapter straight through removes the failure class; the rejected `/tmp` copy would still land the file in the worker heap (Pyodide's `/tmp` is in-memory) and keep `materialize_file` alive as a regression target. Deleting the function is the structural enforcement; `TestStreamingInvariant` (no `read(-1)`) is the behavioral one. The 2 GiB upload cap is not a contradiction: streaming removed the *mechanism* ceiling, while the cap is a deliberate *policy* guard that this decision moved upstream to metadata rather than deleting. The change also closed the PayloadString/WORKERFS retirement — forks still on WORKERFS migrate before consuming this version.
