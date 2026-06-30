---
adr_id: "0001"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-13 13:41:56"
links:
    precedes: []
    succeeds: []
status: accepted
date: 2026-03-13
tags:
    - test-data
    - privacy
    - synthetic-fixtures
title: No real participant data in version control
---

## <a name="question"></a> Context and Problem Statement

Testing extraction logic ideally uses real donated data packages (DDPs), which are the most realistic inputs. But real DDPs contain personal data. How should test data be managed?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Commit anonymised or scrubbed DDPs to the repository
2. <a name="option-2"></a> Synthetic fixtures in the repository, real DDPs stored externally
3. <a name="option-3"></a> No file-level test data — unit test extraction logic only

## <a name="criteria"></a> Decision Drivers

* Real DDPs are ZIP archives of social media exports and contain personal data — committing them would be a privacy violation regardless of consent
* Anonymisation is imperfect and hard to audit; the risk of residual personal data in a "scrubbed" DDP is not worth the marginal realism improvement
* Extraction tests primarily verify structural handling (column names, row counts, data types) — synthetic fixtures cover this without real content

## <a name="outcome"></a> Decision Outcome
We decided for [Option 2](#option-2) because: Real DDPs contain personal data that must not enter version control; synthetic fixtures cover the structural cases that matter for extraction correctness, while real DDPs stored outside the repo at ~/data/d3i/test_packages/ are available for manual integration testing.

### Consequences

* Good: The repository can be shared or made public without privacy risk
* Good: Synthetic fixtures are controlled — tests are stable and reproducible across machines
* Bad: Tests cannot catch format changes in real platform exports until a developer runs manual validation with real data
* Bad: `validate_received.py` must be run separately with real data to catch real-world divergence

### Confirmation

`.gitignore` blocks `tests/data/`, `tests/fixtures/received_files/`, and `received_files/` from being committed. `CLAUDE.md` lists committed DDP files under "Forbidden". Code review must reject any commit adding `.zip` files or real export data.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-03-13 13:41:56) Danielle McCool: marked decision as decided
