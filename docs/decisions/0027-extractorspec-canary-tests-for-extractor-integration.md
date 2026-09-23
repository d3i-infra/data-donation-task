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

Extractor integration tests assert one thing per extractor: running it against a real local DDP fixture yields a non-empty DataFrame. A single-fixture platform lists its extractors as `ExtractorSpec` entries; a multi-set/multi-account platform (e.g. Google's directory-of-zips Takeout exports) instead pins per-set expectations in an expectation map, keyed by glob-discovered fixture-set directories. Either shape skips cleanly when fixtures are absent.

## Guidance

- Keep the single canary assertion (`not df.empty`) in both shapes — don't add expected-column/dtype/nullability contracts to specs or expectation-map entries; a failing canary means "re-run the extractor manually with an `errors` Counter to diagnose", not "read the diff from a schema".
- A single-fixture platform lists `ExtractorSpec` entries (name, callable, kwargs) in the test layer, against `tests/ddp/<platform>_*.zip`. A multi-set/multi-account platform — whose DDP arrives as several zip parts per export, with local fixtures spanning multiple real/scrubbed accounts — instead discovers `tests/ddp/<platform>_set_*/` directories and pins per-set expectations in a map (`EXPECT_NON_EMPTY: dict[str, set[str]]`) keyed by set name.
- Multi-set platforms: every extractor in the platform's registry must appear in the union of at least one fixture set's expectation-map pins, enforced by a static test that does not require fixtures present — a new extractor added with no pin must fail loudly in CI, never pass silently by omission.
- Specs (or expectation maps) live in the test layer only; production extractors stay plain `(reader, errors, **kwargs)` functions with no test imports or embedded test methods.
- Missing fixture(s) → `pytest.skip()`, never a failure; CI must stay green with an empty `tests/ddp/` (ADR-0014: fixtures are never committed).
- When adding or reworking a platform's extractors, add or extend its `test_extractor_integration_<platform>.py` spec list (or expectation-map entries) in the same change.

## Why

Real DDPs are the only inputs that catch real format drift, and they cannot enter the repository — so integration tests have to run opportunistically against whatever a developer has locally, and skip without complaint otherwise. A thin test-layer dataclass (or, for a multi-set platform, a thin per-set expectation map) keeps that machinery out of production code — extractors stay plain functions — and the deliberately minimal assertion reflects what the signal is actually worth: extracted data is messy, so dtype/nullability contracts would be a maintenance burden that mostly fails on harmless variation, while an empty DataFrame is the one externally observable symptom that an extractor or a platform's export format broke. A multi-set platform's real export legitimately leaves some extractors empty per account (a product the participant never used) — a flat "must be non-empty" list would either force false failures or force weakening the assertion for every platform; per-set pins keep the assertion exact without discarding the one signal that matters: a previously-pinned extractor gone silently empty is real drift. The registry-completeness pin exists for the same reason applied to omission itself — an unpinned extractor is invisible to every per-set assertion, so it needs its own static, fixture-independent tripwire. The test is a canary, not a diagnostic — when it fires, the follow-up is always manual inspection. The cost is stated plainly: a green run proves nothing when fixtures are absent or stale, and a red run says only "something changed here".
