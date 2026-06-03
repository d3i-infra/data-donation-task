# Changelog

This project follows [semantic versioning](https://semver.org/) starting from v2.0.0.
Earlier releases used sequential numbering (#1-#5) matching the upstream
[eyra/feldspar](https://github.com/eyra/feldspar) convention.

## [Unreleased]

## v3.0.0 — 2026-06-03

### Breaking

* `ZipArchiveReader.__init__` and `validate.validate_zip` narrow their
  archive parameter from `Union[str, IO[bytes]]` (introduced in v2.0.1)
  to a new `SeekableBinaryReader` Protocol (`read | seek | tell`) — path
  strings are no longer accepted. The upload pipeline passes an
  `AsyncFileAdapter` directly; tests construct fixtures via `io.BytesIO`.
  Type narrowing makes the streaming invariant from `extraction/AD0007`
  checkable: passing a `str` path to a consumer fails Pyright.
* Archive parameter rename — `validate_zip(path_to_zip=…)` and
  `ZipArchiveReader(zip_path=…)` keyword arguments are now `archive=…`.
  The `ZipArchiveReader.zip_path` attribute is renamed to
  `ZipArchiveReader.archive`. Positional callers are unaffected.
* `VITE_PLATFORM` is now required in dev mode. Starting `pnpm start`
  without it emits an error in the study UI. Use
  `VITE_PLATFORM=<platform> pnpm start` to target a single platform.
* Release zips are written to a flat `releases/` folder. The previous
  `releases/<timestamp>/` subdirectory layout is gone; scripts that
  consumed the timestamped path must be updated.

### Added

* Config-driven extraction: each platform has a
  `configs/<platform>_config.json` declaring table titles, column
  headers, and visualizations (python-architecture/AD0012).
* `pnpm generate-config <platform>` — generates a config from extractor
  docstrings. The generator refuses to overwrite an existing file; delete
  it first to re-bootstrap (python-architecture/AD0014).
* `packages/python/port/platforms/example.py` — canonical template for
  new platforms. Copy it and generate a config to add a platform without
  touching `script.py` (python-architecture/AD0013).
* `release.sh` auto-discovers platforms by globbing `configs/` — no
  hardcoded platform list. Adding a platform to a release requires only
  generating its config (fork-governance/AD0005 amendment).
* `VITE_PLATFORM=<platform> pnpm release` builds a single-platform zip
  at 1× build time.
* Extractor integration-test framework: `conftest.py` fixtures plus
  `ExtractorSpec` dataclass; ChatGPT is the first platform covered
  (testing/AD0004).

### Architectural Decisions

* `python-architecture/AD0012` — Declarative `TableConfig` for platform
  extraction scripts.
* `python-architecture/AD0013` — Standard platform module interface with
  required config artifacts.
* `python-architecture/AD0014` — Config lifecycle and generator overwrite
  policy.
* `testing/AD0004` — `ExtractorSpec` dataclass for integration tests.

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

See [MIGRATION.md](MIGRATION.md) for a guide to updating downstream forks.

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
