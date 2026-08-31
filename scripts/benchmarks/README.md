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

### Multi-file (`PayloadFiles`) uploads

`MEMTEST_ZIP` accepts a `:`-separated list of paths — a single path keeps
today's single-zip behavior unchanged; two or more make the harness drive
Playwright's multi-file selection (`ArchiveSet`, see ADR-0040) instead. Pair
it with `MEMTEST_PLATFORM_LABEL` (default `TikTok`) to point the harness's
two `getByRole('heading', …)` selectors (`Select your ${label} file` /
`Your ${label} data`) at a different platform's flow — the donate-button
selector (`'Yes, share for research'`, the default from
`generate_review_data_prompt`) is unchanged and still needs adapting by hand
for a flow that customizes it.

#### iOS-realistic Google Takeout scenario

This is the worst case an iPhone participant actually hits: several zip
parts selected at once, one of them containing a several-hundred-MB history
file. `gen_takeout_benchmark_set.py` builds an en-locale, Google-recognizable
3-part Takeout set to drive it — part 1 is a single legitimately-huge member
(the YouTube watch history, sized by `--activity-mb`, default 300 — the case
the 512 MiB per-member guard passes and the streaming HTML parser
(`_parse_activity_html`, ADR-0040) exists for), part 2 a mid-sized My
Activity/Search export plus the subscriptions/comments CSVs, part 3
manifest-only (`archive_browser.html`).

Exact, runnable steps:

    pnpm generate-config google   # if not already generated (ADR-0030)
    # Build and serve a production-representative build:
    VITE_PLATFORM=google NODE_ENV=development pnpm run build
    python3 -m http.server 3000    # from packages/data-collector/dist

    python3 scripts/benchmarks/gen_takeout_benchmark_set.py --out /tmp/bench-set

    MEMTEST_PLATFORM_LABEL=Google \
    MEMTEST_ZIP=/tmp/bench-set/takeout-<stamp>-1-001.zip:/tmp/bench-set/takeout-<stamp>-2-001.zip:/tmp/bench-set/takeout-<stamp>-3-001.zip \
    node scripts/benchmarks/memtest-v3-peak.cjs

(the generator prints the exact three-path `MEMTEST_ZIP` value, `<stamp>`
filled in, at the end of its run — copy it verbatim). `--activity-mb` defaults
to 300; pass a smaller value for a quick smoke run.

Unlike TikTok's default config, `google_config.json` already attaches
`visualizations` to `youtube_watch_history` (the big table) — no fixture
swap needed before building, unlike the TikTok case below.

Absolute numbers still need the production-representative, non-logging-sink
build ADR-0034's Guidance calls for — a `NODE_ENV=development` build runs
`FakeBridge` (which logs the full donation to the console) and StrictMode
React, inflating every phase; treat those runs as relative-only evidence.
One more wrinkle specific to `donate`: `FakeBridge`'s `POST /data-submission`
only gets a real `200` from the new dev-donate-sink (`devDonateSinkPlugin` in
`packages/data-collector/vite.config.ts`, `apply: 'serve'`) when the app is
served by the Vite **dev server** (`pnpm start`) — a production-representative
build served statically (`python3 -m http.server`) has no such route, so that
fetch 404s there instead. Harmless to the harness's own measurement (it
samples for a fixed settle window after the click regardless of the fetch's
outcome), but worth knowing before reading anything into what the browser
shows next. Compare per-phase peaks as **deltas over this run's own idle
baseline** — absolute baselines wobble with environment state — and this run,
like the split-fixture one below, is Danielle's, done outside the agent
sandbox (no Chromium there).

For the older synthetic split-fixture harness (any platform, not
Google-shaped), generate a split fixture and point a harness at the
`e2etest_multifile` test platform instead of a real study platform:

    python3 tests/generate_test_zip.py --size 1900MB --files 4 --split 2 \
        --output /tmp/big-part-1.zip /tmp/big-part-2.zip

`--split N` distributes the generated files round-robin across N zip parts
— each file stays whole in exactly one part, matching how a real
multi-part Takeout export is structured. Run it the same way as above:

    MEMTEST_PLATFORM_LABEL=e2etest_multifile \
    MEMTEST_ZIP=/tmp/big-part-1.zip:/tmp/big-part-2.zip \
    node scripts/benchmarks/memtest-v3-peak.cjs

Build and serve with `VITE_PLATFORM=e2etest_multifile` (dev server or
`build:release`-excluded dev build; `e2etest_multifile` is test-only and never
ships in a release, see ADR-0004). This comparison run is Danielle's too,
done outside the agent sandbox.

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
