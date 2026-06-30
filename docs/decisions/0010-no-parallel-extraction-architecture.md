---
status: accepted
date: "2026-03-16"
tags:
    - extraction
    - flowbuilder
    - consolidation
source: 68c59d8
category: Python architecture
priority: invariant
forbids:
    - packages/python/port/donation_flows/**
    - packages/python/port/extraction/**
    - packages/python/port/flows/**
    - packages/python/port/runners/**
---

# No parallel extraction architecture

## Decision

There is one extraction architecture. Do not add a second extraction runner, auto-extraction registry, or parallel flow package under `packages/python/port/`.

## Guidance

- Do not create `donation_flows/`, `extraction/`, `flows/`, or `runners/` under `packages/python/port`; those paths are forbidden.
- Add or change platforms through the FlowBuilder/template and validation ADRs, not by introducing an auto-extract-everything path.
- Keep extraction curated to study needs; broad auto-extraction is a data-minimization violation.

## Why

The removed `donation_flows/` path auto-extracted large platform surfaces, including far more Facebook tables than the study needed. A second extraction architecture would bypass FlowBuilder's shared consent/retry/donation controls and reintroduce excessive collection risk.
