# Architectural decisions

This index is generated from the ADR frontmatter — do not edit by hand.
Load the ADR(s) whose filename matches the area you are touching.

## Index

### Governance

- [0001 — Record architecture decisions as lean ADRs](./0001-record-architecture-decisions-as-lean-adrs.md)

### Fork governance

- [0002 — This study is a fork of d3i-infra data-donation-task](./0002-this-study-is-a-fork-of-d3i-infra-data-donation-task.md)
- [0003 — Keep framework, study UI, and extraction in separate packages](./0003-keep-framework-study-ui-and-extraction-in-separate-packages.md)
- [0004 — Don't modify feldspar for study-specific features](./0004-don-t-modify-feldspar-for-study-specific-features.md)
- [0022 — Per-platform release builds via VITE_PLATFORM](./0022-per-platform-release-builds-via-vite-platform.md)

### Python architecture

- [0005 — Layered Python architecture with unidirectional dependencies](./0005-layered-python-architecture-with-unidirectional-dependencies.md)
- [0006 — No cross-layer private imports](./0006-no-cross-layer-private-imports.md)
- [0007 — All UI page and flow-prompt construction goes through port_helpers](./0007-all-ui-page-and-flow-prompt-construction-goes-through-port-helpers.md)
- [0008 — Separate upstream props from D3I-custom props](./0008-separate-upstream-props-from-d3i-custom-props.md)
- [0009 — Python generator protocol for workflow orchestration](./0009-python-generator-protocol-for-workflow-orchestration.md)
- [0010 — No parallel extraction architecture](./0010-no-parallel-extraction-architecture.md)
- [0011 — Handle structured donation results with legacy PayloadVoid fallback](./0011-handle-structured-donation-results-with-legacy-payloadvoid-fallback.md)
- [0012 — ScriptWrapper exception handling is a PII safety boundary](./0012-scriptwrapper-exception-handling-is-a-pii-safety-boundary.md)
- [0013 — Three logging boundaries for diagnostics, milestones, and consent-gated errors](./0013-three-logging-boundaries-for-diagnostics-milestones-and-consent-gated-errors.md)

### Feldspar

- [0014 — Register study UI factories in data-collector, not feldspar defaults](./0014-register-study-ui-factories-in-data-collector-not-feldspar-defaults.md)
- [0019 — Communicate with the host through a swappable Bridge](./0019-communicate-with-the-host-through-a-swappable-bridge.md)
- [0020 — Worker delivers uploads as PayloadFile, not a WORKERFS path](./0020-worker-delivers-uploads-as-payloadfile-not-a-workerfs-path.md)
- [0021 — Flow completion is generator exhaustion, not an explicit exit](./0021-flow-completion-is-generator-exhaustion-not-an-explicit-exit.md)

### Data collector

- [0015 — Prefer standard feldspar prompts; custom only when needed](./0015-prefer-standard-feldspar-prompts-custom-only-when-needed.md)

### Extraction

- [0016 — Validate DDP categories before extraction](./0016-validate-ddp-categories-before-extraction.md)
- [0023 — FlowBuilder template for per-platform extraction flows](./0023-flowbuilder-template-for-per-platform-extraction-flows.md)
- [0024 — Reject unsafe uploads before validation and extraction](./0024-reject-unsafe-uploads-before-validation-and-extraction.md)
- [0025 — No-data extraction skips consent and donation](./0025-no-data-extraction-skips-consent-and-donation.md)
- [0026 — Use session-platform donation keys](./0026-use-session-platform-donation-keys.md)
- [0027 — ZipArchiveReader handles expected-missing DDP members](./0027-ziparchivereader-handles-expected-missing-ddp-members.md)
- [0028 — Stream PayloadFile uploads without materializing](./0028-stream-payloadfile-uploads-without-materializing.md)

### Testing

- [0017 — No real participant data in version control](./0017-no-real-participant-data-in-version-control.md)
- [0018 — Mock the Pyodide js module before importing port in tests](./0018-mock-the-pyodide-js-module-before-importing-port-in-tests.md)
