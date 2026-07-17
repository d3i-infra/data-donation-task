# Memory benchmarks

Playwright harnesses that drive a DDP through the full participant flow
(upload → validate → extract → consent page) and measure browser memory,
isolated to the run's own process tree. Built during the v3.0.0 upstream
sync to compare branches and hunt the React 19.2 peak-memory regression
(see PENDING_ISSUES / release notes).

## Prerequisites

- A running app on `http://localhost:3000` — either the dev server
  (`VITE_PLATFORM=<platform> BROWSER=none pnpm start`) or a production
  build served statically
  (`VITE_PLATFORM=<platform> NODE_ENV=development pnpm run build`,
  then `python3 -m http.server 3000` from `packages/data-collector/dist`).
- A test DDP zip. Real DDPs must never enter version control
  (ADR-0014); point at a local file via env var. Synthetic zips of any
  size: `python3 tests/generate_test_zip.py --size 1900MB --files 4 -o /tmp/big.zip`.
- `MEMTEST_ZIP=/path/to/ddp.zip` — required by all harnesses.

Run from the repo root, e.g.:

    MEMTEST_ZIP=/path/to/tiktok.zip node scripts/benchmarks/memtest-v3-peak.cjs

## The harnesses

| Script | Measures | Use when |
|---|---|---|
| `memtest.cjs` | RSS at checkpoints, example-platform flow | quick smoke: does a huge upload stream without ballooning (ADR-0026)? |
| `tiktok-memtest.cjs` | 1 s RSS timeline, TikTok flow | first-pass profiling of a real DDP |
| `memtest-v2.cjs` | median RSS/PSS over stable windows, per-process-type PSS, forced-GC diagnostic, workload-identity checks | rigorous A/B between builds (use `RUN_LABEL=` to tag runs; emits `RESULT>` JSON lines) |
| `memtest-v3-peak.cjs` | peak instantaneous footprint (250 ms sampling) of the whole tree and of the renderer process alone, broken down per phase (including a `donate` phase that clicks through consent) | threshold questions — e.g. iOS WebKit kills pages around 1–1.5 GB instantaneous, so peak renderer RSS is the metric that matters |

`tiktok-memtest.cjs` and the v2/v3 harnesses expect the TikTok flow
headings; adapt the two `getByRole('heading', …)` selectors to target a
different platform. `memtest-v3-peak.cjs` also clicks the donate button at
the end of the flow to capture the donation-serialization spike as its own
`rendererPeaksByPhase.donate` entry; the button label selector
(`'Yes, share for research'`, the default from `generate_review_data_prompt`)
must be adapted alongside the two heading selectors for non-TikTok flows.

## Methodology notes (hard-won)

- **Deltas over each run's own baseline** are the comparable metric;
  absolute baselines wobble ±200 MB with environment state.
- **Steady-state medians are trustworthy; sub-3 s extraction peaks at
  1 s sampling are not** — use v3's 250 ms sampling for peaks.
- Measurement is scoped to the launched browser's process tree, so
  concurrent browsers don't contaminate results — but don't run two
  harnesses at once anyway (CPU contention skews timings).
- A forced-GC diagnostic (v2) distinguishes retained memory from
  collectable garbage; jetsam-style kills act on instantaneous
  footprint, so both views matter.
- For branch A/Bs: clean-build every artifact with one toolchain, log
  `git write-tree` / tree hashes and dist digests, use one Playwright
  installation (one Chromium build) for all runs, and interleave runs
  in randomized order.

### Visualization-bearing config (required for consent/chart phases)

The peak harness only exercises the chart pipeline if the built config
attaches `visualizations` to the big table — a config where the largest
table has no `visualizations` skips chart compute entirely, and the
`donate`/consent phase peaks read as flat and misleadingly low. Before
building the benchmark artifact, copy
`scripts/benchmarks/fixtures/tiktok_config.with-watchviz.json` over
`packages/python/port/configs/tiktok_config.json`, then build as usual.
2026-07-17 finding: under the viz-less default config, a ~4 GB
chart-compute burst on `tiktok_watch_history` was completely invisible
to the harness — the fixture surfaces it.
