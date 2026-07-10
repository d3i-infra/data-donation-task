---
status: accepted
date: "2026-03-16"
tags:
    - worker-protocol
    - file-delivery
    - memory-safety
category: Feldspar
applies_to:
    - packages/data-collector/public/py_worker.js
    - packages/python/port/main.py
    - packages/python/port/api/file_utils.py
priority: invariant
forbids:
    - packages/data-collector/public/d3i_py_worker.js
companions:
    - packages/python/tests/test_flow_builder.py
    - packages/python/tests/test_uploads.py
checks:
    - desc: worker delivers files as PayloadFile
      grep: 'PayloadFile'
      in: ["packages/data-collector/public/py_worker.js"]
      expect: present
    - desc: no WORKERFS file copy in the worker scripts
      grep: 'WORKERFS'
      in: ["packages/data-collector/public/**"]
      expect: absent
---

# Worker delivers uploads as PayloadFile, not a WORKERFS path

## Decision

The Pyodide worker (`py_worker.js`) delivers an uploaded file to Python as a `PayloadFile` wrapping an on-demand reader (`readSlice` / `size` / `name`), and `ScriptWrapper` wraps that reader in an `AsyncFileAdapter`. The old WORKERFS path — copying the whole file into Pyodide's filesystem and passing a `PayloadString` path via `d3i_py_worker.js` — is removed.

## Guidance

- Keep file delivery as `PayloadFile`; don't reintroduce a WORKERFS copy, a `PayloadString` upload path, or `d3i_py_worker.js`.
- `ScriptWrapper` (`main.py`) wraps the reader in `AsyncFileAdapter`; consumers stream it (zipfile reads slices) and never materialize it to a path — the consumption contract is `SeekableBinaryReader` in `port/api/file_utils.py`.
- `PayloadString` still exists as a prompt value (e.g. a radio selection); this rule is about file delivery only.

## Why

A multi-GiB DDP copied whole into Pyodide's in-memory filesystem OOMs the worker; the on-demand `PayloadFile` reader avoids the copy and matches upstream eyra/feldspar. The half-migrated state showed why one protocol matters: the worker sent `PayloadFile` while Python still expected `PayloadString`, so every upload silently skipped extraction. `PayloadFile` never crosses the postMessage boundary, so nothing required keeping the old path — the dual-protocol options were rejected as keeping that mismatch class alive indefinitely; forks migrate at the version boundary.
