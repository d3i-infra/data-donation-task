---
adr_id: "0007"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-17 13:23:49"
    - author: Danielle McCool
      comment: "2"
      date: "2026-03-17 13:23:49"
    - author: Danielle McCool
      comment: "3"
      date: "2026-03-17 14:00:34"
links:
    precedes: []
    succeeds: []
status: decided
tags:
    - donation
    - host-compatibility
    - protocol
title: Handle structured donation results with legacy PayloadVoid fallback
---

## <a name="question"></a> Context and Problem Statement

When FlowBuilder yields a donate command the host may return different response types depending on the deployment: Eyra Next returns PayloadResponse with success/failure while D3I mono returns PayloadVoid (fire-and-forget). The current FlowBuilder ignores the donate result entirely. How should donation results be handled across host platforms?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Interpret result with PayloadVoid as legacy success
2. <a name="option-2"></a> Ignore donate results entirely
3. <a name="option-3"></a> Require all hosts to return PayloadResponse

## <a name="criteria"></a> Decision Drivers
Eyra Next (async donations) returns PayloadResponse with success boolean — failures must be surfaced to the participant
D3I mono (fire-and-forget) returns PayloadVoid or None — treating this as failure would break all D3I deployments
The handling must be uniform across all platforms — implemented once in shared infrastructure not per-platform
### Pros and Cons

**Interpret result with PayloadVoid as legacy success**
* Good, because works with both Eyra Next and D3I mono without configuration
* Good, because failures are surfaced to participants on hosts that support it
* Good, because implemented once in port_helpers.handle_donate_result()
* Neutral, because D3I mono participants never see donation failures even if they occur

**Ignore donate results entirely**
* Good, because simplest implementation
* Bad, because Eyra Next failures are silently swallowed
* Bad, because participants think their data was donated when it was not


## <a name="outcome"></a> Decision Outcome
We decided for [Option 1](#option-1) because: The protocol must work across both Eyra Next (structured responses) and D3I mono (fire-and-forget). PayloadResponse(success=True) means continue; PayloadResponse(success=False) means render failure page; PayloadVoid/None means legacy success; anything else is treated as failure with a warning log. This is implemented in port_helpers.handle_donate_result() and called by FlowBuilder after every donation.

## <a name="comments"></a> Comments
<a name="comment-3"></a>3. (2026-03-17 14:00:34) Danielle McCool: Protocol detail: PayloadResponse wraps the result in a value field. Python receives result.__type__ == 'PayloadResponse' with result.value containing {success: bool, key: str, status: int, error?: str}. So the correct access pattern is result.value.success — not result.success. This matches eyra/feldspar develop (commit 94ed016 — Feb 2026) where CommandRouter awaits Bridge.send() for donate commands and wraps ResponseSystemDonate in PayloadResponse. FakeBridge returns void (no pending donation tracking) so dev mode still gets PayloadVoid.
