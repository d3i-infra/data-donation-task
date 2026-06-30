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
    - packages/python/tests/test_validate.py
    - packages/python/tests/test_zip_archive_reader.py
    - packages/python/tests/test_flow_builder.py
priority: invariant
---

# Stream PayloadFile uploads without materializing

## Decision

The upload pipeline passes browser `PayloadFile` / `AsyncFileAdapter` objects directly to validators and extractors. `AsyncFileAdapter` exposes `read` / `seek` / `tell` and `size`; consumers pass it to `zipfile.ZipFile` instead of materializing a filesystem path or bytes object. `PayloadString` / WORKERFS upload support stays retired.

## Guidance

- Do not add `materialize_file()` or any whole-upload read on the upload path.
- Pass the adapter directly to `zipfile.ZipFile`, `validate_zip()`, and `ZipArchiveReader`; read size from `adapter.size`.
- Keep tests proving `zipfile` uses bounded reads and `FlowBuilder` accepts `PayloadFile` only.

## Why

`FileReaderSync.readAsArrayBuffer()` fails above the browser Blob / ArrayBuffer limit when asked for the whole file, and Pyodide `/tmp` is still worker heap. `zipfile.ZipFile` only needs a seekable file-like object, so the adapter is the memory-safe contract.
