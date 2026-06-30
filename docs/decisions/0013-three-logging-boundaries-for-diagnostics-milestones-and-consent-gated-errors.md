---
adr_id: "0011"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-20 15:02:40"
    - author: Danielle McCool
      comment: "2"
      date: "2026-03-20 15:02:40"
    - author: Danielle McCool
      comment: "3"
      date: "2026-03-20 15:40:11"
links:
    precedes: []
    succeeds: []
status: decided
tags:
    - logging
    - pii-safety
    - observability
    - bridge
title: Use explicit host milestone commands for flow observability — keep local diagnostics local — reserve full error details for consent-gated donation
---

## <a name="question"></a> Context and Problem Statement

How should the system expose extraction-flow observability to Eyra without leaking PII while preserving future support for full error-detail donation with participant consent? Three different kinds of logging are in play: local diagnostics for developers (may contain PII) — safe flow milestones for host-side observability — full error details that may contain PII and must only leave the iframe with explicit participant consent. The confusion comes from conflating those into one mechanism.

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Forward the whole port logger tree
2. <a name="option-2"></a> Hidden Python logging handler boundary (LogForwardingHandler on port.bridge)
3. <a name="option-3"></a> Use JS framework logger as the application milestone channel
4. <a name="option-4"></a> Forward full errors automatically with scrubbing
5. <a name="option-5"></a> Explicit CommandSystemLog yields for milestones — local loggers for diagnostics — consent-gated path for full errors

## <a name="criteria"></a> Decision Drivers
Participant data is intentionally processed in-browser before consent — only the participant-reviewed donation payload should leave the iframe by default
Researchers still need host-side observability about progress through the flow
Full error strings are useful but are not safe for automatic export
Hidden forwarding via logging handlers is harder to reason about than explicit command emission
Duplicate or parallel observability pipelines create ambiguity and operational noise
### Pros and Cons

**Forward the whole port logger tree**
* Bad, because too easy to leak helper/platform diagnostics and PII

**Hidden Python logging handler boundary (LogForwardingHandler on port.bridge)**
* Good, because preserves policy separation
* Bad, because hides the host-boundary crossing
* Bad, because creates a parallel implicit pipeline alongside the JS LogForwarder

**Use JS framework logger as the application milestone channel**
* Bad, because JS logger does not know domain-level flow semantics
* Neutral, because better treated as framework/runtime observability

**Forward full errors automatically with scrubbing**
* Bad, because sanitization is too fragile for PII safety

**Explicit CommandSystemLog yields for milestones — local loggers for diagnostics — consent-gated path for full errors**
* Good, because host-bound milestone emission is explicit at callsites
* Good, because local developer diagnostics remain available via __name__ loggers
* Good, because future consent-based error donation remains cleanly separable
* Good, because fewer hidden interactions in ScriptWrapper
* Neutral, because generator code has yield from emit_log noise
* Neutral, because milestone discipline depends on explicit callsite review


## <a name="outcome"></a> Decision Outcome
We decided for [Option 5](#option-5) because: Three clear boundaries: (1) local diagnostics via module loggers stay in the browser — never forwarded. (2) Host-visible milestones are emitted explicitly as CommandSystemLog through the command protocol — PII-free constrained vocabulary. (3) Full error details belong to a separate consent-gated donation path (future). CommandSystemLog is already the built-in Python-to-host mechanism — using it directly is not extra infrastructure. The hidden handler/queue setup was the extra infrastructure.

## <a name="comments"></a> Comments
<a name="comment-3"></a>3. (2026-03-20 15:40:11) Danielle McCool: AD0008 and AD0010 (intermediate logging scope decisions) were removed — they were decided and superseded within the same session. AD0011 is the canonical decision. The rejected alternatives section already captures the reasoning from those intermediate steps.
