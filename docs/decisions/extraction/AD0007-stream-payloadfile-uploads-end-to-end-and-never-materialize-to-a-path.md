---
adr_id: "0007"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-04-30 19:11:46"
links:
    precedes: []
    succeeds:
        - "0003"
status: decided
tags:
    - uploads
    - streaming
    - memory-safety
    - file-api
title: Stream PayloadFile uploads end-to-end and never materialize to a path
---

## <a name="question"></a> Context and Problem Statement

The previous `materialize_file()` helper called `AsyncFileAdapter.read()` with no size argument before handing a path to `zipfile.ZipFile`. That single `readAsArrayBuffer` call rejects with `NotReadableError` above the DOM File API's ~2 GiB ArrayBuffer cap — independent of available RAM. The bug was reproduced in production (#61). How should the upload pipeline avoid this?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Pass `AsyncFileAdapter` directly to consumers; delete `materialize_file`
2. <a name="option-2"></a> Stream `materialize_file` in chunks to `/tmp`

## <a name="criteria"></a> Decision Drivers
- DOM File API caps `readAsArrayBuffer` at ~2 GiB regardless of RAM.
- Upstream eyra/feldspar PR #482 already designed `AsyncFileAdapter` to avoid full-file reads.
- Multi-GiB takeouts (YouTube, Facebook) are routine; the read path must not impose a ceiling.

### Pros and Cons

**Pass `AsyncFileAdapter` directly; delete `materialize_file`**
* Good, because `zipfile.ZipFile` already accepts any seekable file-like (`read`, `seek`, `tell`); `AsyncFileAdapter` qualifies.
* Good, because `zipfile` chunks reads at its own discretion, well below the API cap.
* Good, because deleting `materialize_file` removes the failure surface — no path-producing function for a future refactor to call.

**Stream `materialize_file` in chunks to `/tmp`**
* Good, because it avoids the per-call cap.
* Bad, because `/tmp` is Pyodide's in-memory filesystem — the file is still copied into the worker heap.
* Bad, because it preserves `materialize_file` as a target for future regressions.

## <a name="outcome"></a> Decision Outcome

Option 1. `AsyncFileAdapter` is passed directly to consumers; the size guard moves upstream to use `adapter.size` (JS metadata, no read). Closes the `PayloadString`/WORKERFS deprecation opened by feldspar/AD0003 — researcher forks still on the WORKERFS path must migrate to `PayloadFile` before consuming this version.

**Enforcement.** The behavioral regression test in `tests/test_uploads.py::TestStreamingInvariant` asserts that `zipfile.ZipFile` against a tracking adapter never issues `read(-1)`, and runs in CI. The structural fact that `materialize_file()` no longer exists means a future regression has to deliberately add a new path-producing function — visible in code review. Pyright catches type violations locally and during review (`pnpm typecheck:py`), but is not currently a CI gate.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-04-30 19:11:46) Danielle McCool: marked decision as decided
