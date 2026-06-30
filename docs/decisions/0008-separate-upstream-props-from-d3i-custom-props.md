---
adr_id: "0004"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-13 13:33:17"
    - author: Danielle McCool
      comment: "2"
      date: "2026-03-17 13:20:55"
date: 2026-03-13T00:00:00Z
links:
    precedes: []
    succeeds: []
status: accepted
tags:
    - props
    - upstream-alignment
    - types
title: Separate upstream props from D3I-custom props
---

## <a name="question"></a> Context and Problem Statement

The fork needs Python prop types not present in upstream feldspar: PropsUIPromptQuestionnaire, PropsUIPromptFileInputMultiple, and others. Should these be added to props.py (which mirrors Eyra's types) or kept in a separate file?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Add all prop types to props.py regardless of origin
2. <a name="option-2"></a> Separate: props.py mirrors upstream, d3i_props.py contains additions
3. <a name="option-3"></a> Separate package for D3I prop types

## <a name="criteria"></a> Decision Drivers

* Upstream merges into `props.py` should be clean — D3I additions would create merge conflicts on every upstream sync
* The underscore convention and two-file structure make D3I additions immediately visible to anyone reading the codebase
* `props.py` mirrors Eyra's TypeScript props types; adding D3I types would make the mirror relationship opaque

## <a name="outcome"></a> Decision Outcome
We decided for [Option 2](#option-2) because: Keeping props.py as a clean mirror of upstream Eyra types means upstream merges into that file are clean and the diff clearly shows what D3I has added; d3i_props.py makes D3I additions explicit and auditable.

### Consequences

* Good: `props.py` can be updated by dropping in Eyra's upstream version — no conflict resolution needed
* Good: Any reviewer can see exactly what D3I has added to the type system by looking at `d3i_props.py` alone
* Bad: Platform code and helpers must import from two files (`props` and `d3i_props`) rather than one

### Confirmation

Code review: any new Python prop type that is D3I-specific (not present in upstream `eyra/feldspar`) and appears in `props.py` is a violation. The fix: move it to `d3i_props.py`.

## More Information

See [fork-governance/AD0003](../fork-governance/AD0003-maintain-upstream-alignment-for-the-feldspar-package.md) — upstream alignment policy that motivates keeping `props.py` as a clean mirror.

## <a name="comments"></a> Comments
<a name="comment-2"></a>2. (2026-03-17 13:20:55) Danielle McCool: Exception (2026-03-17): PropsUIPageError is in props.py (line 352) but is not an upstream Eyra type — it should be in d3i_props.py per this ADR. However it is already shipped and registered with ErrorPageFactory in App.tsx. The extraction consolidation design depends on it for safety-error and donate-failure pages. Exempted as existing debt: moving it to d3i_props.py is a separate cleanup that requires updating both Python and TypeScript imports. Custom page-body types already shipped in this fork are grandfathered.
