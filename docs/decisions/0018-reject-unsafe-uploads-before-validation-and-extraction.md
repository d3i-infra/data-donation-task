---
status: accepted
date: "2026-03-17"
tags:
    - safety
    - uploads
    - memory-safety
category: Extraction
applies_to:
    - packages/python/port/helpers/flow_builder.py
    - packages/python/port/helpers/uploads.py
priority: invariant
companions:
    - packages/python/tests/test_uploads.py
    - packages/python/tests/test_flow_builder.py
checks:
    - desc: single-file 2 GiB cap (no exact-size sentinel) is compared only in the PayloadFile branch
      grep: 'if file_result\.__type__ != "PayloadFile":'
      in: ["packages/python/port/helpers/uploads.py"]
      expect: present
    - desc: PayloadFiles sets are guarded (aggregate size + file count) before the single-file branch
      grep: '== "PayloadFiles"'
      in: ["packages/python/port/helpers/uploads.py"]
      expect: present
---

# Reject unsafe uploads before validation and extraction

## Decision

After receiving a `PayloadFile` or `PayloadFiles` upload, `FlowBuilder.start_flow()` runs `uploads.check_payload_size(file_result)` before DDP validation or extraction. For a `PayloadFile` the guard applies only the per-file 2 GiB cap; for a `PayloadFiles` set it instead checks aggregate size and member count. Both read only JS-reported metadata.

## Guidance

- Keep the order upload receipt → `check_payload_size()` → validation → extraction for both payload shapes.
- The size check is metadata-only; never read upload bytes to measure a file or a set.
- `MAX_FILE_SIZE_BYTES` applies only inside the `PayloadFile` branch, as a cap and nothing more — there is no exact-size sentinel; a file exactly at the cap passes. A truncated or split archive fails zip validation downstream into the retry prompt instead of being caught here. A `PayloadFiles` member (e.g. a Google Takeout part) may legitimately exceed the cap, since multi-select is the supported path for chunked exports.
- A `PayloadFiles` set is instead guarded by `MAX_TOTAL_UPLOAD_BYTES` (aggregate bytes) and `MAX_UPLOAD_FILES` (member count) — see `check_payload_size`'s `PayloadFiles` branch.
- `MAX_MEMBER_UNCOMPRESSED_BYTES` caps per-member uncompressed size but is enforced later, at materialization time by the archive-set reader — not inside `check_payload_size`.
- Safety is platform-independent, so do not duplicate any of these guards in platform modules or in a study's `script.py`.

## Why

Files above 2 GiB are rejected as policy for a single upload — streaming removed the read ceiling, but extraction still decompresses and parses members inside the Pyodide worker heap, and the JS-reported size is a free upstream proxy for that risk. There is deliberately no exact-2-GiB sentinel: real Google Takeout parts slice below the chosen size limit (observed parts never reach exactly 2 GiB) and a truncated download can stop at any byte count, so an equality check was never a reliable detector of a split export — and a truncated or corrupted archive already fails `validate_zip()` downstream, routing the participant to the retry prompt, which is better UX than the sentinel's dead-end safety page. Multi-select is the supported path for split exports instead: a `PayloadFiles` set's total bytes and member count are the risk proxies there, since the per-file cap would misfire on an ordinary multi-gigabyte Takeout part. The guard runs once in FlowBuilder, before validation, because it is platform-independent and the validators assume a structurally safe payload. It reads only metadata: reading bytes to measure would defeat the streaming it protects.
