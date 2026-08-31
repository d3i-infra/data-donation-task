---
status: proposed
date: "2026-07-17"
category: Performance
applies_to:
    - packages/data-collector/src/components/consent_form_viz/**
    - scripts/benchmarks/memtest-v3-peak.cjs
    - scripts/benchmarks/gen_takeout_benchmark_set.py
    - scripts/benchmarks/README.md
priority: default
---

# Hold participant-flow peak memory to the reference budget

## Decision

Peak instantaneous renderer RSS across the participant flow (upload → consent → donate) on the 65k-row reference DDP must not exceed ~824 MB in a production-representative build. This record is the budget, not a claim of compliance: as of 2026-07-16 the flow measures above it (989–1029 MB, development-build A/B after the ephemeral-worker fix).

## Guidance

- Before merging memory-relevant changes to the consent/viz flow, run `scripts/benchmarks/memtest-v3-peak.cjs` (donate phase included) on the reference DDP and compare per-phase peaks against the budget; deltas over each run's own idle baseline are the comparable A/B metric.
- The multi-zip iOS-realistic scenario (Google Takeout) is now a standing benchmark variant: generate a 3-part Takeout-shaped set (300 MB activity member ≈ 400k rows) with `scripts/benchmarks/gen_takeout_benchmark_set.py`, then run `memtest-v3-peak.cjs` with `MEMTEST_PLATFORM_LABEL=Google` and a colon-joined `MEMTEST_ZIP` — see `scripts/benchmarks/README.md`.
- The benchmark build must bake in a config with visualizations on the big table (fixture in `scripts/benchmarks/fixtures/`) — under a viz-less config the entire chart pipeline is invisible to the measurement (2026-07-17: a ~4 GB chart-compute burst was undetectable until the profiling config matched the study's).
- Absolute numbers count only from a production-representative build with a non-logging data-submission sink — `NODE_ENV=development` builds run FakeBridge (which logs the full donation) and StrictMode React, inflating every phase; treat those runs as relative evidence only.
- Track `peakTreeMb` alongside renderer RSS (Pyodide's worker heap lives in the same process tree), and treat real iOS hardware/WebKit as the final authority for the absolute gate.
- Measured conclusion (2026-08-31): for multi-source archive platforms (Google Takeout), peak memory is governed by extracted row count and string bytes, not upload size — input-side streaming (ADR-0040's `open_member` feeding lxml `iterparse`) already removes the file-buffer copy. Remaining headroom, ranked: truncate the development payload logging and remove FakeBridge's double-serialization; parse generator-based into per-column builds (no intermediate dict/tuple lists, no chunked concat); `del` + `gc.collect()` before each next large allocation (freed WASM arenas are reused below the high-water mark); and, structurally, future work — incremental consent transport that drops DataFrames after serialization, renderer-side virtualization fed by aggregates, and terminating the Pyodide worker after consent-data transfer (the only change that reclaims the whole WASM heap). Real iOS hardware remains the final authority.

## Why

iOS WebKit kills pages on instantaneous footprint in roughly the 1–1.5 GB band; ~824 MB (the reference flow's upload-phase peak) leaves headroom on smaller devices — the budget is the difference between a completed donation and a participant losing their work mid-flow.

On 2026-08-31, a development build (static-served, FakeBridge) measured the Google Takeout multi-part scenario end to end. A 3-part run (300 MB activity member ≈ 400k rows) peaked, per renderer phase (load / idle / upload+process / render+settle / donate), at 429 / 428 / 3458 / 3997 / 5073 MB (tree peak 5876 MB). Repacking the identical logical dataset as one zip peaked at 429 / 429 / 3479 / 3988 / 5103 MB (tree 5954) — indistinguishable within noise, so multi-part archive-set assembly itself adds no measurable renderer cost. The entire excess over the ~824 MB budget is dataset scale (≈6× the reference DDP's row count) plus the development-build artifacts already named above (FakeBridge's full-payload console log and a second `JSON.stringify` re-embedding the serialized donation string at the donate phase; StrictMode's duplicated render work) — read as deltas over each run's own idle baseline, per the existing Guidance.
