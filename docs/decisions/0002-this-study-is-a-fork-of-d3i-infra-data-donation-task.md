---
status: accepted
date: "2026-03-13"
tags:
    - upstream
    - lineage
category: Fork governance
priority: default
---

# This study is a fork of d3i-infra data-donation-task

## Decision

This repo is a researcher fork of `d3i-infra/data-donation-task` (itself downstream of `eyra/feldspar`), not a from-scratch build or a direct `eyra/feldspar` fork — d3i's base already ships the multi-platform Python infrastructure (FlowBuilder, `AsyncFileAdapter`, PayloadFile) the study needs.

## Guidance

- Track two upstreams independently: `eyra/feldspar` (framework, flowing through d3i-infra) and `d3i-infra/data-donation-task` (the multi-platform base); pull improvements from both, and treat divergence as a deliberate choice.
