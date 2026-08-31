# Architectural decisions

This index is generated from the ADR frontmatter — do not edit by hand.
Load the ADR(s) whose filename matches the area you are touching.

## Index

### Governance

- [0001 — Record architecture decisions as lean ADRs](./0001-record-architecture-decisions-as-lean-adrs.md)

### Fork governance

- [0002 — Don't modify feldspar for study-specific features](./0002-don-t-modify-feldspar-for-study-specific-features.md)
- [0003 — Keep framework, custom UI, and extraction in separate packages](./0003-keep-framework-custom-ui-and-extraction-in-separate-packages.md)
- [0004 — Per-platform release builds via VITE_PLATFORM](./0004-per-platform-release-builds-via-vite-platform.md)

### Feldspar

- [0005 — Register study UI factories in data-collector, not feldspar defaults](./0005-register-study-ui-factories-in-data-collector-not-feldspar-defaults.md)
- [0006 — Communicate with the host through a swappable Bridge](./0006-communicate-with-the-host-through-a-swappable-bridge.md)
- [0017 — Worker delivers uploads as PayloadFile, not a WORKERFS path](./0017-worker-delivers-uploads-as-payloadfile-not-a-workerfs-path.md)
- [0025 — Flow completion is generator exhaustion, not an explicit exit](./0025-flow-completion-is-generator-exhaustion-not-an-explicit-exit.md)
- [0039 — Exit nonzero on every incomplete ending; 0 means completed](./0039-error-flow-exhaustion-exits-nonzero-exit-0-means-completed.md)

### Python architecture

- [0007 — Layered Python architecture with unidirectional dependencies](./0007-layered-python-architecture-with-unidirectional-dependencies.md)
- [0008 — No cross-layer private imports](./0008-no-cross-layer-private-imports.md)
- [0009 — All UI page and flow-prompt construction goes through port_helpers](./0009-all-ui-page-and-flow-prompt-construction-goes-through-port-helpers.md)
- [0010 — Separate upstream props from D3I-custom props](./0010-separate-upstream-props-from-d3i-custom-props.md)
- [0011 — Python generator protocol for workflow orchestration](./0011-python-generator-protocol-for-workflow-orchestration.md)
- [0021 — Handle structured donation results with legacy PayloadVoid fallback](./0021-handle-structured-donation-results-with-legacy-payloadvoid-fallback.md)
- [0022 — ScriptWrapper exception handling is a PII safety boundary](./0022-scriptwrapper-exception-handling-is-a-pii-safety-boundary.md)
- [0023 — Three logging boundaries for diagnostics, milestones, and consent-gated errors](./0023-three-logging-boundaries-for-diagnostics-milestones-and-consent-gated-errors.md)
- [0028 — Docstring-driven UI metadata for extractor functions](./0028-docstring-driven-ui-metadata-for-extractor-functions.md)
- [0029 — Standard platform module interface](./0029-standard-platform-module-interface.md)
- [0030 — Config lifecycle and generator overwrite policy](./0030-config-lifecycle-and-generator-overwrite-policy.md)

### Extraction

- [0012 — FlowBuilder template for per-platform extraction flows](./0012-flowbuilder-template-for-per-platform-extraction-flows.md)
- [0013 — Validate DDP categories before extraction](./0013-validate-ddp-categories-before-extraction.md)
- [0018 — Reject unsafe uploads before validation and extraction](./0018-reject-unsafe-uploads-before-validation-and-extraction.md)
- [0019 — No-data extraction skips consent and donation](./0019-no-data-extraction-skips-consent-and-donation.md)
- [0020 — Use session-platform donation keys](./0020-use-session-platform-donation-keys.md)
- [0024 — ZipArchiveReader handles expected-missing DDP members](./0024-ziparchivereader-handles-expected-missing-ddp-members.md)
- [0026 — Stream PayloadFile uploads without materializing](./0026-stream-payloadfile-uploads-without-materializing.md)
- [0040 — Present multi-part uploads as one archive-set](./0040-present-multi-part-uploads-as-one-archive-set.md)

### Testing

- [0014 — No real participant data in version control](./0014-no-real-participant-data-in-version-control.md)
- [0015 — Mock the Pyodide js module in conftest before importing port](./0015-mock-the-pyodide-js-module-in-conftest-before-importing-port.md)
- [0027 — ExtractorSpec canary tests for extractor integration](./0027-extractorspec-canary-tests-for-extractor-integration.md)

### Data collector

- [0016 — Prefer standard feldspar prompts; custom only when needed](./0016-prefer-standard-feldspar-prompts-custom-only-when-needed.md)
- [0031 — Consent-page memory work must never shrink the donated dataset](./0031-consent-page-memory-work-must-never-shrink-the-donated-dataset.md)
- [0032 — Visualization workers are ephemeral and column-scoped](./0032-visualization-workers-are-ephemeral-and-column-scoped.md)
- [0033 — Consent-viz donation must not route through DataSubmissionPage's factory data path](./0033-consent-viz-donation-must-not-route-through-datasubmissionpage-s-factory-data-path.md)
- [0035 — Per-row work over participant tables must not allocate](./0035-per-row-work-over-participant-tables-must-not-allocate.md)

### Performance

- [0034 — Hold participant-flow peak memory to the reference budget](./0034-hold-participant-flow-peak-memory-to-the-reference-budget.md)

### Architecture

- [0036 — Await the host's donation acknowledgment before resolving](./0036-await-the-host-s-donation-acknowledgment-before-resolving.md)

### Localization

- [0037 — Resolve text to exact, default, first-available, or sentinel](./0037-resolve-text-to-exact-default-first-available-or-sentinel.md)
- [0038 — Normalize the UI locale once at the data-collector boundary](./0038-normalize-the-ui-locale-once-at-the-data-collector-boundary.md)
