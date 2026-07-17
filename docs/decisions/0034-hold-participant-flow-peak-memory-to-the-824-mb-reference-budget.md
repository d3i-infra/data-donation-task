---
status: proposed
date: "2026-07-17"
category: Performance
applies_to:
    - packages/data-collector/src/components/consent_form_viz/**
    - scripts/benchmarks/memtest-v3-peak.cjs
priority: default
---

# Hold participant-flow peak memory to the ~824 MB reference budget

## Decision

Peak instantaneous renderer RSS across the participant flow (upload → consent → donate) on the 65k-row reference DDP must not exceed ~824 MB in a production-representative build. This record is the budget, not a claim of compliance: as of 2026-07-16 the flow measures above it (989–1029 MB, development-build A/B after the ephemeral-worker fix).

## Guidance

- Before merging memory-relevant changes to the consent/viz flow, run `scripts/benchmarks/memtest-v3-peak.cjs` (donate phase included) on the reference DDP and compare per-phase peaks against the budget; deltas over each run's own idle baseline are the comparable A/B metric.
- The benchmark build must bake in a config with visualizations on the big table (fixture in `scripts/benchmarks/fixtures/`) — under a viz-less config the entire chart pipeline is invisible to the measurement (2026-07-17: a ~4 GB chart-compute burst was undetectable until the profiling config matched the study's).
- Absolute numbers count only from a production-representative build with a non-logging data-submission sink — `NODE_ENV=development` builds run FakeBridge (which logs the full donation) and StrictMode React, inflating every phase; treat those runs as relative evidence only.
- Track `peakTreeMb` alongside renderer RSS (Pyodide's worker heap lives in the same process tree), and treat real iOS hardware/WebKit as the final authority for the absolute gate.

## Why

iOS WebKit kills pages on instantaneous footprint in roughly the 1–1.5 GB band; ~824 MB (the reference flow's upload-phase peak) leaves headroom on smaller devices — the budget is the difference between a completed donation and a participant losing their work mid-flow.
