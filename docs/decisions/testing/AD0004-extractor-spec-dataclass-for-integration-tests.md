---
status: accepted
date: "2026-05-08"
tags:
    - integration-testing
    - extractors
    - fixtures
category: Testing
applies_to:
    - packages/python/tests/extractor_integration_helpers.py
    - packages/python/tests/**/test_extractor_integration_*.py
    - packages/python/port/platforms/**/*.py
priority: default
---

# ExtractorSpec canary tests for extractor integration

## Decision

Extractor integration tests list a platform's extractors as `ExtractorSpec` entries (name, callable, kwargs) in the test layer and assert exactly one thing per extractor: running it against a real local DDP yields a non-empty DataFrame. When no fixture matches `tests/ddp/<platform>_*.zip`, the file skips cleanly.

## Guidance

- Keep the single canary assertion (`not df.empty`) — don't add expected-column/dtype/nullability contracts to specs; a failing canary means "re-run the extractor manually with an `errors` Counter to diagnose", not "read the diff from a schema".
- Specs live in the test layer only; production extractors stay plain `(reader, errors, **kwargs)` functions with no test imports or embedded test methods.
- Missing fixture → `pytest.skip()`, never a failure; CI must stay green with an empty `tests/ddp/`.
- When adding or reworking a platform's extractors, add or extend its `test_extractor_integration_<platform>.py` spec list in the same change.

## Why

Real DDPs are the only inputs that catch real format drift, and they cannot enter the repository — so integration tests have to run opportunistically against whatever a developer has locally, and skip without complaint otherwise. A thin test-layer dataclass keeps that machinery out of production code (extractors stay plain functions), and the deliberately minimal assertion reflects what the signal is actually worth: extracted data is messy, so dtype/nullability contracts would be a maintenance burden that mostly fails on harmless variation, while an empty DataFrame is the one externally observable symptom that an extractor or a platform's export format broke. The test is a canary, not a diagnostic — when it fires, the follow-up is always manual inspection. The cost is honest: a green run proves nothing when fixtures are absent or stale, and a red run says only "something changed here".
