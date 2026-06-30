---
adr_id: "0002"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-13 13:33:16"
links:
    precedes: []
    succeeds:
        - "0001"
status: accepted
date: 2026-03-13
tags:
    - imports
    - encapsulation
    - coupling
title: No cross-layer private imports
---

## <a name="question"></a> Context and Problem Statement

As the Python package grows, functions in one layer may be useful in another. Some functions are marked private with a leading underscore to signal they are internal to their module. Should private functions be importable across architectural layers?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Allow any import across layers — no restrictions
2. <a name="option-2"></a> Restrict cross-layer imports to public functions only (no _ prefix)
3. <a name="option-3"></a> Enforce explicit __all__ exports in each module

## <a name="criteria"></a> Decision Drivers

* The underscore convention is Python's established signal for "internal implementation detail"; importing across layers ignores the author's stated contract
* Cross-layer private imports create tight coupling that makes refactoring and testing harder
* The fix is always the same: if a function is needed across layers, make it public and move it to the appropriate layer

## <a name="outcome"></a> Decision Outcome
We decided for [Option 2](#option-2) because: A private function (underscore prefix) signals the author's intent that it not be used externally; importing it from another layer violates that contract and creates hidden coupling between layers that should be independent.

### Consequences

* Good: Private functions remain genuinely private — the underscore convention is meaningful
* Good: When a function is needed across layers, moving it forces a deliberate decision about the right layer
* Bad: Requires discipline in code review to catch cross-layer private imports

### Confirmation

Code review: any `from port.helpers.flow_builder import _something` in script.py or platforms/ is a violation. The fix: rename without underscore and move to helpers/ if cross-layer use is intentional.

### Evidence of violation

The `feat/facebook-ddp-error-handling` branch imports `_build_error_payload` from `flow_builder.py` into `script.py`.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-03-13 13:33:16) Danielle McCool: marked decision as decided
