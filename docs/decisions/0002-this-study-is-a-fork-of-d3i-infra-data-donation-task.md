---
adr_id: "0002"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-13 13:25:38"
links:
    precedes:
        - "0003"
        - "0004"
    succeeds: []
status: accepted
date: 2026-03-13
tags:
    - upstream
    - lineage
title: Fork D3I data-donation-task as the study base
---

## <a name="question"></a> Context and Problem Statement

The VU 2026 study needs a data donation workflow supporting 7 platforms (LinkedIn, Instagram, Chrome, Facebook, YouTube, TikTok, X) with per-platform Python extraction, FlowBuilder-based orchestration, and custom UI components. Eyra provides feldspar as a workflow framework; D3I maintains a fork with multi-platform support already added. How should we acquire and customise this base?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Build workflow from scratch
2. <a name="option-2"></a> Fork eyra feldspar directly
3. <a name="option-3"></a> Fork D3I data-donation-task
4. <a name="option-4"></a> Use feldspar as npm dependency without forking

## <a name="criteria"></a> Decision Drivers

* The study requires deep Python customisation (7 platforms, FlowBuilder, custom validation) that goes far beyond what an npm dependency relationship supports
* D3I's fork already contains multi-platform Python infrastructure that would otherwise need to be built
* Forking preserves the ability to pull upstream improvements from both eyra/feldspar and d3i-infra/data-donation-task

## <a name="outcome"></a> Decision Outcome
We decided for [Option 3](#option-3) because: D3I's fork already includes multi-platform support, PayloadFile/AsyncFileAdapter, and Python patterns not in upstream feldspar; forking from it gives these capabilities immediately while preserving the ability to pull future upstream improvements.

### Consequences

* Good: Multi-platform Python infrastructure (FlowBuilder, AsyncFileAdapter, PayloadFile) is available from day one
* Good: D3I maintains the upstream relationship with eyra/feldspar, so improvements flow through d3i-infra
* Bad: Two upstream sources to track (eyra/feldspar and d3i-infra/data-donation-task), each evolving independently
* Bad: Creating a researcher fork creates a maintenance obligation — the project must decide how much drift to allow

## More Information

See [AD0003](AD0003-maintain-upstream-alignment-for-the-feldspar-package.md) for the policy on how much the feldspar package may diverge from upstream.
See [AD0004](AD0004-three-package-monorepo-with-distinct-modification-policies.md) for how the three packages are organized.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-03-13 13:25:38) Danielle McCool: marked decision as decided
