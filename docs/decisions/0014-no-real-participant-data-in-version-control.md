---
status: accepted
date: "2026-03-13"
tags:
    - test-data
    - privacy
    - synthetic-fixtures
category: Testing
applies_to:
    - .gitignore
    - packages/python/tests/extractor_integration_helpers.py
priority: invariant
---

# No real participant data in version control

## Decision

Real participant data — real DDPs and social-media exports — never enters version control; tests use synthetic fixtures only. Real DDPs for local integration testing go in the git-ignored `packages/python/tests/ddp/` and never leave the developer's machine.

## Guidance

- Never commit real DDPs or participant exports. The `.gitignore` entries for `packages/python/tests/ddp/*` (everything except `.gitkeep`) are the enforcement — don't remove or weaken them.
- Synthetic archives are the only committable fixtures: built in-test (`io.BytesIO` + `zipfile`) or generated with the repo-root `tests/generate_test_zip.py` (e2e fixtures — a different directory from the Python tests); binary fixtures go through Git LFS.
- Integration tests `pytest.skip()` when `packages/python/tests/ddp/` holds no fixture — absence of real data is never a test failure.

## Why

A real DDP is a personal-data archive, so committing one is a privacy breach regardless of participant consent — and a single leaked archive in git history cannot be undone. Anonymization was rejected because it is imperfect and hard to audit: the residual-data risk outweighs the marginal realism. Synthetic fixtures cover what extraction tests actually verify — structural handling, member resolution, parser behavior — and keep the repo shareable or publishable with zero privacy exposure, with stable, reproducible tests as a side effect. The accepted cost is that real-world export-format drift is invisible to CI: it surfaces only when a developer drops a real DDP into the git-ignored `packages/python/tests/ddp/` locally and runs the integration suite. (Deliberately no `forbids` glob on that directory: real fixtures are *supposed* to exist there locally, and a lint that fires during sanctioned use is noise — git-tracking, not file existence, is the enforcement point, and `.gitignore` owns it.)
