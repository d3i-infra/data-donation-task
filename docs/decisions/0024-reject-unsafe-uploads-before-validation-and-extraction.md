---
adr_id: "0003"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-17 13:23:34"
links:
    precedes:
        - "0007"
    succeeds:
        - "0002"
status: decided
tags:
    - safety
    - uploads
    - flowbuilder
title: Reject unsafe uploads before DDP validation and extraction
---

## <a name="question"></a> Context and Problem Statement

FlowBuilder receives uploaded files from the browser. Some files are too large to process in the Pyodide WebWorker (>2GB) or are incomplete chunked exports (exactly 2GB). These runtime safety constraints are distinct from platform-specific DDP validation (extraction/AD0002). Where and when should upload safety be enforced?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Check safety in FlowBuilder before DDP validation
2. <a name="option-2"></a> Check safety in script.py before delegating to FlowBuilder
3. <a name="option-3"></a> Check safety in main.py / ScriptWrapper

## <a name="criteria"></a> Decision Drivers
Large files cause OOM in the browser WebWorker — must be rejected before extraction attempts
Chunked exports (exactly 2GB) from Google Takeout and other platforms are incomplete — processing them gives wrong results
Safety checks are platform-independent — they apply to all uploads regardless of DDP category
DDP validation (extraction/AD0002) assumes a structurally safe file — safety must come first
### Pros and Cons

**Check safety in FlowBuilder before DDP validation**
* Good, because every platform gets safety checks without per-platform code
* Good, because safety runs before extraction — no wasted work on unsafe files
* Good, because clear separation: safety (uploads.py) then validity (validate.py)
* Neutral, because adds a step to the flow — participant sees safety error before retry

**Check safety in script.py before delegating to FlowBuilder**
* Good, because script.py already has safety checks in dd-vu-2026
* Bad, because safety logic is study-level not platform-level — wrong layer
* Bad, because every script.py must remember to add safety checks


## <a name="outcome"></a> Decision Outcome
We decided for [Option 1](#option-1) because: Safety checks are per-upload concerns that apply to every platform. FlowBuilder owns the per-platform flow and is the natural place: materialize → safety check → DDP validate → extract. helpers/uploads.py provides check_file_safety(path) which raises FileTooLargeError or ChunkedExportError. FlowBuilder catches these and renders a safety error page via port_helpers.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-03-17 13:23:34) Danielle McCool: marked decision as decided
