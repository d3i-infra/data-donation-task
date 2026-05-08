---
adr_id: "0004"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-05-08 00:00:00"
links:
    precedes: []
    succeeds: []
status: accepted
date: 2026-05-08
tags:
    - integration-testing
    - extractors
    - fixtures
title: ExtractorSpec dataclass for extractor integration tests
---

## <a name="question"></a> Context and Problem Statement

Each platform in `packages/python/port/platforms/` exposes extractors as plain functions `(reader: ZipArchiveReader, errors: Counter, **kwargs) -> pd.DataFrame`. There is currently no systematic way to verify that a given extractor still produces output when an underlying platform format changes. How should extractor correctness be tested against real DDP data, given that real DDPs are sensitive and cannot enter the repository?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Wrap extractors in a production class with embedded test methods and a test aggregator
2. <a name="option-2"></a> ExtractorSpec dataclass in the test layer, one assertion: not df.empty
3. <a name="option-3"></a> Full schema validation per extractor (expected columns, dtypes, nullability)

## <a name="criteria"></a> Decision Drivers

* Real DDPs must not enter version control (see testing/AD0001)
* Extractors are plain functions — production code must not be contaminated with test concerns
* Extracted data is inherently messy; dtype and nullability constraints add maintenance burden without catching real bugs
* An empty DataFrame is the only externally observable signal of a broken extractor — further diagnosis always requires manual inspection of the errors Counter
* Tests must skip gracefully when real DDP data is absent, not fail

## <a name="outcome"></a> Decision Outcome
We decided for [Option 2](#option-2) because: a thin test-layer dataclass keeps production extractors untouched, avoids duplicating column contracts already declared in `TableConfig`, and the single assertion `not df.empty` captures the only actionable failure signal. When a test fails, the appropriate response is manual investigation — the test is a canary, not a diagnostic.

### Consequences

* Good: Production extractors remain plain functions with no test imports or test logic
* Good: Adding coverage for a new extractor requires only one `ExtractorSpec` instance and one local ZIP file
* Good: CI without real DDP data skips integration tests cleanly via `pytest.skip()`
* Bad: A failing test only signals "something is wrong" — it does not indicate whether the extractor crashed, found no matching file, or the DDP format changed
* Bad: Tests are only as useful as the real DDP files available locally — stale or missing fixtures mean silent gaps in coverage

### Confirmation

Each platform gets a test file `tests/test_extractor_integration_<platform>.py`. A missing fixture at `~/data/d3i/test_packages/` causes `pytest.skip()`, not a failure. CI runs without real data and skips all extractor integration tests. `CLAUDE.md` documents the local fixture path and onboarding steps for obtaining sample DDPs.

## More Information

Real DDP storage location and the policy against committing participant data are established in [testing/AD0001](AD0001-no-real-participant-data-in-version-control.md). `TableConfig` is defined in `packages/python/port/table_extractor.py` and carries the extractor callable and runtime kwargs used by `ExtractorSpec`.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-05-08 00:00:00) Danielle McCool: marked decision as decided
