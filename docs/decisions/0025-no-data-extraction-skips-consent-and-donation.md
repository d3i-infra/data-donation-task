---
adr_id: "0004"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-17 13:23:55"
    - author: Danielle McCool
      comment: "2"
      date: "2026-03-17 13:23:55"
links:
    precedes: []
    succeeds: []
status: decided
tags:
    - extraction
    - flowbuilder
    - ux
title: Empty extraction renders no-data acknowledgment and skips donation
---

## <a name="question"></a> Context and Problem Statement

When a participant uploads a valid DDP but the curated extraction functions produce no data rows (all tables are empty DataFrames) the current FlowBuilder shows an empty consent form. This is confusing — the participant sees a donate button with nothing to review. What should happen when extraction produces no tables?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Render a no-data acknowledgment page and skip donation
2. <a name="option-2"></a> Show empty consent form as today
3. <a name="option-3"></a> Treat as extraction failure and route to retry

## <a name="criteria"></a> Decision Drivers
Participants should understand what happened — showing an empty consent form with a donate button is misleading
An empty extraction is not an error — the DDP was valid but contained no data matching the curated tables
The outcome must be distinguishable from extraction bugs and from the retry flow
### Pros and Cons

**Render a no-data acknowledgment page and skip donation**
* Good, because the participant gets a clear explanation before the flow moves on
* Good, because no empty donation is submitted — cleaner data pipeline
* Good, because distinguishable from errors (safety/validation) and retries
* Neutral, because changes current behavior — previously showed empty consent form

**Show empty consent form as today**
* Good, because no behavior change
* Bad, because participants see a donate button with nothing to donate
* Bad, because confusing UX


## <a name="outcome"></a> Decision Outcome
We decided for [Option 1](#option-1) because: A valid DDP with no relevant data is an expected outcome — not an error. The participant should see a clear message ('We found a valid [Platform] file but it didn't contain the specific data relevant to this study') and acknowledge it before the flow moves to the next platform. No donation is submitted. This is implemented as ph.render_no_data_page(platform_name) in port_helpers, yielded and awaited by FlowBuilder.

## <a name="comments"></a> Comments
<a name="comment-2"></a>2. (2026-03-17 13:23:55) Danielle McCool: More Information:
See extraction/AD0001 for FlowBuilder template. See python-architecture/AD0006 for the consolidation design where this behavior was decided.
