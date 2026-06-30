---
adr_id: "0003"
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
    - upstream-alignment
    - feldspar
title: Maintain upstream alignment for the feldspar package
---

## <a name="question"></a> Context and Problem Statement

As a fork we could modify feldspar freely or track upstream. Eyra's feldspar has a clean layered architecture (framework/types, processing, visualization/react with factories, ui/elements, pages, prompts) worth preserving. When the study needs new UI capabilities, should we modify feldspar or add them elsewhere?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Diverge freely — modify feldspar as needed for the study
2. <a name="option-2"></a> Strict alignment — never modify feldspar at all
3. <a name="option-3"></a> Pragmatic alignment — avoid modifications, upstream any that are necessary

## <a name="criteria"></a> Decision Drivers

* Study-specific modifications to feldspar create merge conflicts on every upstream pull
* Eyra's feldspar architecture is well-designed and worth preserving; gratuitous divergence reduces the value of future upstream improvements
* feldspar may be a candidate for upstreaming improvements back to d3i-infra; study-specific patches block this

## <a name="outcome"></a> Decision Outcome
We decided for [Option 3](#option-3) because: Strict alignment prevents merge conflicts and keeps feldspar eligible for upstreaming to d3i-infra, while the pragmatic qualifier acknowledges that genuine framework improvements should flow back upstream rather than being blocked entirely.

### Consequences

* Good: Upstream feldspar pulls are clean — no study-specific conflicts to resolve
* Good: feldspar remains eligible for contribution back to d3i-infra/data-donation-task
* Bad: Study-specific UI needs must be handled by adding components in data-collector rather than modifying feldspar in place
* Bad: Genuine framework gaps cannot be patched immediately; they require upstream contribution or a data-collector workaround

### Confirmation

Any PR modifying files under `packages/feldspar/` must explain why the change cannot live in `packages/data-collector/`. The `CLAUDE.md` note "feldspar/ is upstream infrastructure — almost never modify it" is the standing rule. Code review should reject feldspar modifications for study-specific functionality.

### Evidence of violation

The `feat/facebook-ddp-error-handling` branch added a TextArea component and modified the Confirm and DonateButtons components directly in `packages/feldspar/` for a single feature. These should have been new components in `packages/data-collector/`.

## More Information

See [data-collector/AD0001](../data-collector/AD0001-custom-ui-components-belong-in-data-collector-not-feldspar.md) for the component placement rule that follows from this policy.
See [python-architecture/AD0004](../python-architecture/AD0004-separate-upstream-props-from-d3i-custom-props.md) for how the same alignment principle applies to Python prop types.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-03-13 13:26:44) Danielle McCool: marked decision as decided
