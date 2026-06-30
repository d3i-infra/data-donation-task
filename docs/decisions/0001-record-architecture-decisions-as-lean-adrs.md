---
adr_id: "0001"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-13 13:22:32"
links:
    precedes: []
    succeeds: []
status: accepted
date: 2026-03-13
tags:
    - process
    - documentation
title: Use MADR for architectural decision records
---

## <a name="question"></a> Context and Problem Statement

The dd-vu-2026 codebase has grown to 7 platforms, custom UI components, and a layered Python architecture. Implicit architectural assumptions are causing drift — contributors add code in the wrong layer, modify feldspar for study-specific features, or duplicate logic that belongs in shared helpers. How should the project document its architectural decisions?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> MADR format managed with adg
2. <a name="option-2"></a> Prose decisions in ARCHITECTURE.md only
3. <a name="option-3"></a> Inline comments in code
4. <a name="option-4"></a> No formal decision records

## <a name="criteria"></a> Decision Drivers

* Architectural drift is already occurring — the feat/facebook-ddp-error-handling branch added feldspar modifications and cross-layer imports that violate the intended structure
* ARCHITECTURE.md captures rules but not rationale; contributors need to understand *why* the rules exist to apply them in novel situations
* Decisions need to be discoverable at the point of violation, not just in a prose document
* The project is small enough that lightweight tooling is appropriate

## <a name="outcome"></a> Decision Outcome
We decided for [Option 1](#option-1) because: MADR's structured format (context, options, outcome, consequences) captures the why behind decisions in a form that survives contributor turnover; adg provides CLI tooling for creation and indexing without imposing a heavyweight process.

### Consequences

* Good: Decisions are documented with their rationale, surviving contributor turnover and the passage of time
* Good: adg provides a CLI for creating and listing decisions without requiring external services
* Good: Decisions are co-located with code in the repository, visible in PRs and code review
* Bad: Requires discipline to create ADRs at decision time — retroactive documentation misses real context

### Confirmation

When a significant architectural choice is made (new package boundary, new pattern, divergence from upstream), an ADR should be created before or alongside the implementing PR. CLAUDE.md references this process. Code review can ask "is this decision documented?"

## More Information

MADR specification: https://adr.github.io/madr/
adg tool: https://github.com/opinionated-digital-center/architecture-decision-graph
MADR-0010 (use subfolders for large ADR collections) motivates the 6-model structure used here.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-03-13 13:22:32) Danielle McCool: marked decision as decided
