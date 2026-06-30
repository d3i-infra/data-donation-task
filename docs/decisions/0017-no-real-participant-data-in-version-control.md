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
priority: invariant
forbids:
    - structure_donations/**/Raw/**
    - structure_donations/**/Input_test/**
    - pytests/private_testfiles/**
    - pytests/scenarios/**/*PRIVATE.json
---

# No real participant data in version control

## Decision

Real participant data — real DDPs and social-media exports — never enters version control; tests use synthetic fixtures only, and real DDPs live outside the repo (e.g. `~/data/d3i/test_packages/`).

## Guidance

- Never commit real DDPs or real participant data, including real-export `.zip` files (synthetic/invalid zip fixtures under `tests/` are fine). `.gitignore` blocks — and `forbids` now flags new files under — the real-data paths (`structure_donations/**/Raw`, `structure_donations/**/Input_test`, `pytests/private_testfiles/`, `pytests/scenarios/*PRIVATE.json`); don't remove those entries.
- Tests run against synthetic fixtures; real-world divergence is caught separately by running `validate_received.py` against out-of-repo data.

## Why

A real DDP is a personal-data archive, so committing one is a privacy breach regardless of consent, and anonymization is too imperfect to audit. Keeping the repo synthetic-only lets it be shared or made public with no privacy risk — a single leaked archive can't be undone.
