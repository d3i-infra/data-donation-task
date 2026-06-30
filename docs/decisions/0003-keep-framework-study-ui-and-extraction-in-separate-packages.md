---
adr_id: "0004"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-13 13:26:44"
links:
    precedes: []
    succeeds:
        - "0002"
status: accepted
date: 2026-03-13
tags:
    - monorepo
    - package-boundaries
title: Three-package monorepo with distinct modification policies
---

## <a name="question"></a> Context and Problem Statement

The upstream feldspar repository uses three packages: feldspar (framework), data-collector (demo app), and python. This fork has substantially extended the Python package and added study-specific UI. Should we preserve the three-package structure, merge packages, or split further?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Keep three packages with distinct roles
2. <a name="option-2"></a> Merge into a single package
3. <a name="option-3"></a> Split into more packages (e.g. per-platform Python packages)

## <a name="criteria"></a> Decision Drivers

* The three packages have different audiences and different rates of change: framework decisions are rare, study UI changes are frequent, Python extraction changes are constant
* Preserving the upstream package structure reduces friction when pulling upstream changes
* Merging packages would obscure the boundary between framework code and application code

## <a name="outcome"></a> Decision Outcome
We decided for [Option 1](#option-1) because: Each package has a distinct modification frequency and audience: feldspar is framework (rarely modified, upstreamable), data-collector is study UI (modified for new features), python is extraction logic (most active development); keeping them separate makes the boundaries explicit.

### Consequences

* Good: Package boundaries make it obvious where new code belongs (framework, study UI, or extraction)
* Good: Upstream merges into feldspar stay independent of data-collector and python changes
* Bad: Cross-package dependencies require explicit wiring in the monorepo (pnpm workspace)

## More Information

See [python-architecture/AD0001](../python-architecture/AD0001-layered-python-architecture-with-unidirectional-dependencies.md) for how the Python package is structured internally.
See [feldspar/AD0001](../feldspar/AD0001-factory-pattern-for-ui-extensibility.md) for the framework's internal architecture.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-03-13 13:26:44) Danielle McCool: marked decision as decided
