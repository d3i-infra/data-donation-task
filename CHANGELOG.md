# Changelog

This project follows [semantic versioning](https://semver.org/) starting from v2.0.0.
Earlier releases used sequential numbering (#1-#5) matching the upstream
[eyra/feldspar](https://github.com/eyra/feldspar) convention.

## [Unreleased]

### Fixed

* Donations are no longer silently fire-and-forget on any deployment.
  `LiveBridge.send()` now always awaits the host's
  `DonateSuccess`/`DonateError` reply, so a failed upload reliably
  reaches Python as a `PayloadResponse` and the donation-failure page
  is shown instead of the participant being told nothing.
* CI now runs the real JS test suite (previously a no-op step),
  triggers on development-branch PRs, and the Pyright glob is fixed
  so type-checking actually covers the files it claims to (#100).

### Changed

* Pyright debt cleanup: upload consumers are typed as
  `SeekableBinaryReader` (ADR-0026), TikTok extractor payloads are
  narrowed before use, and the remaining optional/union type errors
  surfaced by CI are resolved — all 84 errors flagged when type
  checking went live are fixed.
* Pyright is now pinned at `1.1.411` as a root devDependency and
  invoked through `pnpm exec`, rather than fetched unpinned by `npx` on
  every run. `scripts/py-run.sh` asks poetry for the interpreter at run
  time instead of reading a machine-specific `venvPath` out of
  `pyrightconfig.json`, and a missing poetry environment is now a hard
  failure naming the command that fixes it — previously it degraded
  into a wall of bogus unresolved-import errors. The script uses no
  bash-4 builtins, so it runs on macOS's stock `/bin/bash` 3.2.

### Removed

* **`VITE_ASYNC_DONATIONS`** and its `.env.example` documentation. The
  flag existed to keep donations fire-and-forget for a mono that never
  replied; both monos now attempt a `DonateSuccess`/`DonateError` reply
  on every handled path of `donate_via_api`
  (`core/assets/js/feldspar_app.js`), so the flag only risked silently
  disabling the reliability guarantee. Awaiting the acknowledgment is
  now unconditional — remove the variable from any `.env.local`;
  nothing reads it.

  **This sets a minimum host version.** The workflow now requires a
  mono carrying the donate-ack protocol — `d3i-infra/mono` commit
  `bbfcbffbd` (2026-02-02, "[Feldspar] Add error handling to donate
  flow"). Against an older host no acknowledgment is ever sent, the
  awaited donation never settles, and the participant waits on a
  spinner indefinitely. Deploying against a pre-February-2026 mono
  image is unsupported.

## v3.0.0 — 2026-07-16

This release reworks how studies are built and shipped: every bundle
now targets a single platform, platform modules follow a standard
declaration, and their UI config is generated from docstrings. It also
adds a TikTok TXT-format parser, tightens the upload-pipeline API
(breaking for custom extraction code), incorporates upstream
eyra/feldspar Milestone 8 (dependency stack refresh, SafeData helper),
overhauls the documentation site, and replaces the architecture
decision records with a leaner, tool-governed set.

### Breaking

* **One platform per build, one standard module shape.** `script.py`
  is now a fixed orchestrator that loads `port.platforms.<platform>`
  based on the `VITE_PLATFORM` env var, which is required for dev and
  builds. Multi-platform studies build one bundle per platform:
  `release.sh` produces one for each config in `configs/*_config.json`,
  written to a flat output folder. A platform module supplies docstring
  metadata, `DDP_CATEGORIES`, a FlowBuilder subclass with
  `validate_file()` / `extract_data()`, and a `process(session_id)`
  entry point (ADR-0029). `packages/python/port/platforms/example.py`
  is a complete template.
* **Archive consumers take a file-like reader, not a path.**
  `validate.validate_zip` and `ZipArchiveReader` now accept a
  `SeekableBinaryReader` (`read` / `seek` / `tell`) and no longer
  accept path strings. Production code passes the upload's
  `AsyncFileAdapter` straight through; tests wrap fixture bytes in
  `io.BytesIO`. This makes the ADR-0026 streaming invariant checkable
  with a type checker (Pyright locally — CI does not type-check yet).
* **Keyword rename on those same APIs.** `validate_zip(path_to_zip=…)`
  and `ZipArchiveReader(zip_path=…)` are now `archive=…`, and
  `reader.zip_path` is now `reader.archive`. Positional callers are
  unaffected.

### Added

* TikTok TXT-format DDP parser, with extractor integration tests
  (#76; hardened by #86, #87, #93).
* Docstring-driven config generation: each platform's `port_config`
  is generated from its module docstring and validated at startup
  (ADR-0028, ADR-0030). Generated configs are no longer committed —
  only `example_config.json` stays in the repo.
* Extractor integration test framework: `ExtractorSpec` canary tests
  run each extractor against a real DDP placed in the git-ignored
  `tests/ddp/` directory, and skip cleanly when none is present
  (ADR-0027). ChatGPT is the first covered platform.
* An example platform (`packages/python/port/platforms/example.py`)
  demonstrating the standard interface, including a wordcloud
  visualization.
* `SafeData` (`port/helpers/safe_data.py`, from upstream Milestone 8):
  crash-resistant typed access to parsed JSON with per-tree error
  tracking, for extraction code that must survive malformed DDPs.
  Available but not yet used by the built-in platforms. Its
  `get_errors()` output can contain data values — never feed it into
  host-visible log milestones (ADR-0023).
* Tag-push releases: pushing a `v*` tag creates the GitHub release,
  using that version's changelog section as the notes.

### Changed

* Upstream sync: eyra/feldspar Milestone 8 (#961) merged at `29b5440`.
  Dependency stack refresh: Vite 8 (bundling now via Rolldown),
  TypeScript 6, React 19.2, Playwright 1.61, ESLint 10, jest 30,
  Node 24.18, Python 3.14.6. Upstream's demo-script model, log
  forwarding, and telemetry-oriented disclaimer were reviewed and not
  adopted (they conflict with this fork's PII logging boundaries,
  ADR-0022/0023).
* Documentation site overhauled (#72): restructured getting-started
  tutorials (installation, creating your own task, visualizations,
  deployment), architecture pages realigned to the current build and
  extraction model.
* File-too-large messages report sizes in MiB rather than raw
  bytes (#73).
* Small UI improvements for mobile devices.
* The architecture decision records were rewritten into the lean
  format and renumbered into a flat chronological layout
  (`docs/decisions/NNNN-slug.md`), governed by the `adg` tool
  (compiled brief, executable checks, generated index). Code and doc
  references were realigned; merge `60dbce0` records the mapping from
  the old ids.

### Fixed

* Confirm dialogs render their second button again. A d3i change had
  removed the cancel button from the shared confirm component, which
  silently suppressed the retry prompt's "Continue" and the error
  page's "Skip" buttons. Upstream's optional-cancel rendering is
  adopted: dialogs show exactly the buttons they are configured with.
* Facebook HTML-format DDPs no longer misdetect as the `json_en`
  category. `no-data.txt` — Facebook's empty-section placeholder,
  emitted in both JSON and HTML exports — was listed as a `json_en`
  known file, so an HTML package with several empty sections cleared
  the 5% match threshold without containing any `.json` data. It is
  no longer in the known-file list (#74).
* PrimaryButton showed a collapsed button instead of a spinner while
  processing (#78).
* The Playwright e2e suite runs again: its fixture had been deleted
  and its assertions targeted a long-gone UI. Fixture recreated (LFS),
  spec rewritten against the current consent form.

### Removed

* `MIGRATION.md` — migration guidance now ships in each release's
  changelog section, like the notes below.
* `DISCLAIMER.md` — documented upstream Feldspar/Next constraints
  (notably a ~1 MB expected-payload assumption) that do not apply to
  this fork's streaming upload architecture.
* `poetry.lock` — the Python package runs inside Pyodide, whose
  shipped versions dictate the runtime dependencies; the lock file
  pinned nothing real and caused merge conflicts.

### Known issues

* React 19.2 (kept for parity with upstream) raises peak browser
  memory by ~200 MB on consent pages with very large tables
  (measured: ~1,020 MB vs ~824 MB renderer footprint on a 65k-row
  DDP) — relevant for iPhone participants, where WebKit terminates
  pages around 1–1.5 GB. Most of the excess is collectable garbage,
  and a mitigation (trimming what visualization workers receive) is
  planned. Until it lands, be cautious shipping studies with very
  large tables to iOS participants.

### Migration notes for downstream forks (v2.x → v3.0.0)

1. **Platform modules:** move custom flows to the standard
   declaration — a module under `port/platforms/` with docstring
   metadata, `DDP_CATEGORIES`, a FlowBuilder subclass, and
   `process(session_id)`. `script.py` no longer needs per-study
   edits; start from `example.py`.
2. **Builds:** set `VITE_PLATFORM=<platform>` for `pnpm start` and
   builds; `release.sh` produces one bundle per
   `configs/*_config.json`.
3. **Custom validation/extraction:** pass the uploaded file-like
   straight through — `validate_zip(DDP_CATEGORIES, archive)` /
   `ZipArchiveReader(archive, …)`; paths are rejected. Rename keyword
   arguments `path_to_zip=` / `zip_path=` to `archive=`, and the
   `zip_path` attribute to `archive`.
4. **Legacy payloads:** uploads arrive as `PayloadFile` and stream
   end-to-end — any remaining `PayloadString`/WORKERFS handling can
   be deleted.
5. **Confirm prompts:** if your fork relied on the second (cancel)
   button never rendering, note it now renders whenever configured —
   omit the `cancel` argument for single-button dialogs.

## v2.0.1 — 2026-05-04

### Fixed

* 2+ GiB upload `NotReadableError` regression. `FlowBuilder` no longer
  materializes `PayloadFile` uploads to a path; the
  `AsyncFileAdapter` is passed directly to `zipfile.ZipFile`,
  validators, and extractors (extraction/AD0007). Restores the
  streaming behavior of upstream eyra/feldspar PR
  [#482](https://github.com/eyra/feldspar/pull/482), which the
  FlowBuilder rewrite (`68c59d8`) silently reverted by adding a
  full-file `adapter.read()` inside `materialize_file()`. The
  resulting single-`ArrayBuffer` request triggered
  `FileReaderSync.readAsArrayBuffer`'s ~2 GiB cap with
  `NotReadableError`, often after a long apparent hang. Empirical
  reproduction and full diagnosis:
  [#61](https://github.com/d3i-infra/data-donation-task/issues/61).

### Changed

* Upload-path size validation now uses `adapter.size` (JS metadata,
  no read) before any byte transfer, instead of `os.path.getsize`
  on a materialized `/tmp` copy. The new helper is
  `uploads.check_payload_size(file_result)`. The previous
  `materialize_file()` and `check_file_safety(path)` are removed.
* `ZipArchiveReader.__init__` and `validate.validate_zip` now accept
  any seekable binary file-like (`IO[bytes]`) or a path string; the
  upload pipeline passes an `AsyncFileAdapter` directly. Parameter
  names (`zip_path`, `path_to_zip`) are retained for backwards
  compatibility with researcher-fork callers and will be renamed in a
  follow-up release.
* `FlowBuilder.start_flow()` accepts only `PayloadFile` uploads.
  `PayloadString`/WORKERFS support (kept for SURF Research Cloud
  backwards compatibility per `feldspar/AD0003`) is retired; SRC
  consumers must migrate to `PayloadFile`.
* New host log milestones: `[<Platform>] Upload prompt sent`
  (emitted before the file prompt render command goes to the host),
  `[<Platform>] Upload received: size=…` (emitted immediately after
  a `PayloadFile` upload, before the safety check), and
  `[<Platform>] Upload skipped: type=<X>` (emitted when a
  non-`PayloadFile` payload arrives, distinguishing participant-skip
  from unexpected payload types). Replaces the previous
  post-materialize `[<Platform>] File received` message.

### Removed

* `materialize_file()` and `check_file_safety()` from
  `port.helpers.uploads` — see Changed above for replacements.
* The dual-payload-type branch (`PayloadFile` or `PayloadString`)
  in `FlowBuilder` upload handling. Closes the deprecation window
  opened by `feldspar/AD0003`.

### Architectural Decisions

* `extraction/AD0007` — Stream `PayloadFile` uploads end-to-end and
  never materialize to a path. Succeeds `extraction/AD0003` (whose
  ownership decision is preserved; only the size-check placement
  changes).

## v2.0.0 — 2026-03-23

Incorporates upstream eyra/feldspar #6 (2026-02-25) and #7 (2026-03-05), plus
d3i extraction consolidation, platform updates, and bridge alignment.

### Breaking

* File delivery uses PayloadFile (FileReaderSync) instead of WORKERFS/PayloadString
* CommandSystemLog forwarding from Python and JS to host platform
* Donation keys changed from `{session_id}` to `{session_id}-{platform_name}`
* script.py rewritten as FlowBuilder orchestrator — forks using the old script.py pattern must migrate
* ScriptWrapper catches all Python exceptions as PII safety boundary (AD0009)

### Added

* FlowBuilder: standard template for per-platform extraction flows (extraction/AD0001)
* ZipArchiveReader: deterministic archive member resolution with cached inventory (extraction/AD0006)
* ExtractionResult dataclass with Counter[str] error counting
* Chrome platform extraction
* Upload validation — file type and size checks before extraction (extraction/AD0003)
* PII-safe logging boundaries — explicit CommandSystemLog yields for host-visible milestones, local loggers for diagnostics (AD0011)
* Per-platform release builds via VITE_PLATFORM env var (fork-governance/AD0005)
* Verification commands: `pnpm test`, `pnpm typecheck:py`, `pnpm verify:py`, `pnpm doctor`
* 74 Python unit tests
* Dependency update CI workflow
* DISCLAIMER.md (EUPL)
* 20+ architectural decision records in `docs/decisions/`

### Changed

* All 9 existing platforms migrated to FlowBuilder + ZipArchiveReader + bilingual headers
* Font: Finador replaced with Nunito (open-source)
* Tailwind CSS v3 → v4
* Dataframe truncation limits (Python + TypeScript) to prevent UI overload
* Status text shown during data submission
* Error page shows user-friendly message instead of stacktrace
* Async donation responses via PayloadResponse (backward-compatible with PayloadVoid)
* Case-insensitive search in consent table (from eyra #6)
* Lithuanian and Romanian translations (from eyra #6)

### Removed

* `d3i_example_script.py` — superseded by FlowBuilder pattern
* `donation_flows/` extraction system — consolidated into FlowBuilder (AD0006)
* `script_custom_ui.py` — eyra demo script, not used by d3i platforms
* `d3i_py_worker.js` — dead code, all worker traffic through `py_worker.js`
* Dead CI workflows: `_build_release.yml` (Earthly), `playwright.yml`, `release.yml`

### Migration

MIGRATION.md carried the fork upgrade guide for this release (file removed in
v3.0.0; migration notes now ship in each release's changelog section).

## \#5 2025-09-10

* Switched to pnpm for package management
* Switched to Vite for the frontend build system
* Added Spanish language
* Changed: split script.py into a default basic version in script.py and an advanced version script_custom_ui.py
* Added renovate

## \#4 2025-05-02

* Fixed - Explicit loaded event is sent to ensure proper initialization (channel setup)
* Changed: Feldspar is now split into React component and app
* Changed: Allow multiple block-types to interleave on a submission page
* Added: end to end tests using Playwright

## \#3 2025-04-08

* Changed: layout to support mobile screens (enables mobile friendly data donation)
* Added: support for mobile variant of a table using cards (used for data donation consent screen)

## \#2 2024-06-13

* Added: Support for progress prompt
* Added: German translations
* Added: Support for assets available in Python

## \#1 2024-03-15

Initial version
