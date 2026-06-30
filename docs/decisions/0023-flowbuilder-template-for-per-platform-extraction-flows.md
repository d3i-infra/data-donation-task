---
adr_id: "0001"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-13 13:41:17"
links:
    precedes: []
    succeeds:
        - "0002"
status: accepted
date: 2026-03-13
tags:
    - flowbuilder
    - platform-structure
    - template-method
title: FlowBuilder template for per-platform extraction flows
---

## <a name="question"></a> Context and Problem Statement

Each of 7 platforms needs to implement a common flow: validate the uploaded file, extract data, show a consent prompt, handle donation. Writing this loop independently per platform would duplicate the control flow 7 times. How should per-platform flows be structured?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Duplicate the full flow in each platform module
2. <a name="option-2"></a> Template method — FlowBuilder base class, subclasses override validate_file() and extract_data()
3. <a name="option-3"></a> Composition via strategy objects passed to a shared runner
4. <a name="option-4"></a> Shared helper functions called from each platform module

## <a name="criteria"></a> Decision Drivers

* With 7 platforms, any logic in the flow loop (retry, consent, donation) would be duplicated 7 times without a shared base
* Platform differences are concentrated in two operations: deciding whether a zip is valid, and transforming it into a DataFrame — everything else is identical
* The template method pattern is simpler to extend than a strategy composition: a new platform subclasses FlowBuilder and implements two methods

## <a name="outcome"></a> Decision Outcome
We decided for [Option 2](#option-2) because: The template method gives each platform a minimal implementation surface (validate_file + extract_data) while the shared loop handles file receipt, retry prompts, consent, and donation — eliminating duplication across 7 platforms without requiring each to compose a strategy object.

### Consequences

* Good: Adding an eighth platform requires only a new subclass with two method overrides
* Good: The retry loop, consent prompt, and donation logic are maintained in one place
* Bad: The FlowBuilder base class becomes load-bearing — changes to the shared loop affect all platforms simultaneously

### Confirmation

Code review: any per-platform module that re-implements file receipt, retry, consent, or donation logic is a violation. These belong in `FlowBuilder.start_flow()`. The platform module should only contain `validate_file()`, `extract_data()`, and platform-specific helpers.

## More Information

See [python-architecture/AD0001](../python-architecture/AD0001-layered-python-architecture-with-unidirectional-dependencies.md) — layering determines that FlowBuilder lives in `helpers/`, not in the platform modules.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-03-13 13:41:17) Danielle McCool: marked decision as decided
