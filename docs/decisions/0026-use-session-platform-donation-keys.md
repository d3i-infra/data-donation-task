---
adr_id: "0005"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-17 13:24:03"
    - author: Danielle McCool
      comment: "2"
      date: "2026-03-17 13:24:03"
links:
    precedes: []
    succeeds: []
status: decided
tags:
    - donation
    - multi-platform
    - data-pipeline
title: Use session-platform donation keys for multi-platform studies
---

## <a name="question"></a> Context and Problem Statement

When a study collects data from multiple platforms the donation key must be unique per platform per participant. The current FlowBuilder uses only session_id as the donation key which would cause key collisions in multi-platform studies. What format should donation keys use?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> session_id-platform_name format
2. <a name="option-2"></a> Keep plain session_id
3. <a name="option-3"></a> UUID per donation

## <a name="criteria"></a> Decision Drivers
Multi-platform studies donate multiple times per session — keys must not collide
Downstream data pipelines parse donation keys to identify platform origin
Single-platform builds should still produce predictable keys
### Pros and Cons

**session_id-platform_name format**
* Good, because unique per platform per session
* Good, because downstream pipelines can parse platform from key
* Good, because predictable: 'abc123-facebook' is immediately readable
* Bad, because changes existing key format — pipelines expecting plain session_id need updating

**Keep plain session_id**
* Good, because no pipeline changes needed
* Bad, because key collision in multi-platform studies


## <a name="outcome"></a> Decision Outcome
We decided for [Option 1](#option-1) because: Multi-platform studies are the standard use case (dd-vu-2026 runs 7 platforms). The key format f'{session_id}-{platform_name.lower()}' is unique, readable, and parseable. This is a behavioral change: downstream storage and data pipelines that expect plain session_id keys will need updating. Single-platform builds produce keys like 'abc123-facebook'.

## <a name="comments"></a> Comments
<a name="comment-2"></a>2. (2026-03-17 13:24:03) Danielle McCool: More Information:
See python-architecture/AD0006 for the FlowBuilder consolidation design. This change is implemented in FlowBuilder.start_flow() step 9.
