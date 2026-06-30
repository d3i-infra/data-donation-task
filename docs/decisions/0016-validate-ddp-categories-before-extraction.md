---
adr_id: "0002"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-13 13:41:49"
date: 2026-03-13T00:00:00Z
links:
    precedes:
        - "0001"
        - "0003"
    succeeds: []
status: accepted
tags:
    - validation
    - ddp-categories
    - fail-fast
title: Validation before extraction using DDP_CATEGORIES
---

## <a name="question"></a> Context and Problem Statement

Users upload zip files that may be the wrong platform, wrong format, corrupt, or missing expected files. Extraction is expensive and produces misleading results on bad input. At what point should file validity be checked?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Extract first, catch errors during extraction
2. <a name="option-2"></a> Validate with DDP_CATEGORIES before extraction runs
3. <a name="option-3"></a> On-demand validation triggered only when extraction fails

## <a name="criteria"></a> Decision Drivers

* Extraction errors from malformed input are cryptic — "KeyError: 'likes'" is not a useful message to show a participant
* Validation can fail fast with a clear "wrong file" message before any parsing work is done
* `DDP_CATEGORIES` already exists as the platform's contract for what a valid zip looks like — it should be enforced before use, not after

## <a name="outcome"></a> Decision Outcome
We decided for [Option 2](#option-2) because: Validating first with the platform's DDP_CATEGORIES specification gives the user a meaningful retry prompt before expensive extraction runs, rather than a confusing extraction error after the work is done.

### Consequences

* Good: Participants see "this doesn't look like a [Platform] file — try again" rather than a Python traceback message
* Good: Extraction code can assume the zip is structurally valid; no defensive error handling needed for structural issues
* Bad: `DDP_CATEGORIES` must be kept in sync with what `extract_data()` actually expects — a mismatch lets invalid files through

### Confirmation

Code review: any platform that calls `extract_data()` before `validate_file()` is a violation. `FlowBuilder.start_flow()` enforces this order; platforms should not bypass it.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-03-13 13:41:49) Danielle McCool: marked decision as decided
