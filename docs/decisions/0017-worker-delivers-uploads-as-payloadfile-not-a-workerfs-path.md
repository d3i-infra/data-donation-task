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
    - desc: worker delivers single-file uploads as PayloadFile
      grep: 'PayloadFile\b'
      in: ["packages/data-collector/public/py_worker.js"]
      expect: present
    - desc: worker delivers multi-file uploads as PayloadFiles
      grep: 'PayloadFiles'
      in: ["packages/data-collector/public/py_worker.js"]
      expect: present
    - desc: no WORKERFS file copy in the worker scripts
      grep: 'WORKERFS'
      in: ["packages/data-collector/public/**"]
      expect: absent
---

# Worker delivers uploads as PayloadFile, not a WORKERFS path

## Decision

The Pyodide worker (`py_worker.js`) delivers a single-file upload to Python as a `PayloadFile` wrapping one on-demand reader (`readSlice`/`size`/`name`), and a multi-file upload as `PayloadFiles` wrapping one such reader per file. The old WORKERFS path — copying the whole file into Pyodide's filesystem and passing a `PayloadString` path via `d3i_py_worker.js` — is removed.

## Guidance

- Keep file delivery as `PayloadFile` (single) / `PayloadFiles` (multi); don't reintroduce a WORKERFS copy, a `PayloadString` upload path, or `d3i_py_worker.js`.
- `PayloadFiles` unwraps to one reader per file via `Array.map(createAsyncFileReader)` — never a bulk in-memory copy of the set; each reader is the same on-demand `{readSlice, size, name}` shape as the single-file case.
- `ScriptWrapper` (`main.py`) wraps each reader in an `AsyncFileAdapter`; consumers stream it (zipfile reads slices) and never materialize it to a path — the consumption contract is `SeekableBinaryReader` in `port/api/file_utils.py`.
- `PayloadString` still exists as a prompt value (e.g. a radio selection); this rule is about file delivery only.

## Why

A multi-GiB DDP copied whole into Pyodide's in-memory filesystem OOMs the worker; the on-demand `PayloadFile` reader avoids the copy and matches upstream eyra/feldspar. The half-migrated state showed why one protocol matters: the worker sent `PayloadFile` while Python still expected `PayloadString`, so every upload silently skipped extraction. `PayloadFile` never crosses the postMessage boundary, so nothing required keeping the old path — the dual-protocol options were rejected as keeping that mismatch class alive indefinitely; forks migrate at the version boundary. The multi-file case (`PayloadFiles`) extends the same reasoning per file: N files must stay N on-demand readers, not one bulk copy, or the same OOM returns at the set level.
