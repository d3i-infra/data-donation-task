---
status: accepted
date: "2026-08-05"
tags:
    - donation
    - bridge
    - reliability
source: docs/superpowers/plans/2026-08-05-locale-translation-consolidation.md (Stage 1, replay R1)
category: Architecture
applies_to:
    - packages/feldspar/src/live_bridge.ts
    - packages/feldspar/src/fake_bridge.ts
    - packages/feldspar/src/framework/command_router.ts
    - packages/feldspar/src/framework/types/modules.ts
priority: invariant
---

# Await the host's donation acknowledgment before resolving

## Decision

`LiveBridge.send()` awaits the host's `DonateSuccess`/`DonateError` reply for every donate command before resolving, unconditionally — no environment flag gates it and no timeout abandons it — so `CommandRouter` always returns a `PayloadResponse` the Python layer can inspect.

## Guidance

- Keep the await unconditional: a donate command resolves only when the host acknowledges it. Don't reintroduce an env flag, a build-time switch, or a "fire-and-forget" branch — a `send()` that returns before the acknowledgment silently discards the donation outcome.
- Don't add a timeout or a synthetic success/failure on the waiting path. Both monos attempt a reply on every handled path of `donate_via_api` in `core/assets/js/feldspar_app.js` — network error, non-`ok` response, success — so a failed upload arrives as `DonateError` rather than silence, and a channel replaced mid-donation is resolved explicitly by `updatePort()`.
- **Minimum host**: a mono carrying the donate-ack protocol — `d3i-infra/mono` commit `bbfcbffbd` (2026-02-02, "[Feldspar] Add error handling to donate flow"). Deploying this workflow against an older host is unsupported: it never sends `DonateSuccess`/`DonateError`, so the awaited promise never settles and the participant waits on a spinner indefinitely. Note also that the host's own `sendDonateResponse` is guarded (`if (this.channel && this.channel.port1)`) — a reply is attempted, not guaranteed to be delivered.
- `FakeBridge.handleDataSubmission` must keep returning a `ResponseSystemDonate` too, so dev and hosted runs exercise the same router branch.
- The structured result is what makes the donate-failure page reachable: keep routing it through `CommandRouter`'s `PayloadResponse` into Python's `handle_donate_result()`, which normalizes it (cross-ref ADR-0021's donation-result contract); don't collapse it back to `PayloadVoid`.

## Why

This is a deliberate divergence from upstream `eyra/feldspar`, whose `send()` returns `void` and drops the acknowledgment: without the awaited result a participant whose upload failed is told nothing and walks away believing they donated, and the retry/error flow has no signal to fire on. It was originally flag-gated for a pre-ack mono that never replied; that host is now below the supported minimum, so the flag was pure risk — a misconfigured deployment would have silently reverted the reliability guarantee. The cost of the trade is explicit: the workflow now depends on the host actually acking, which is why the minimum-host requirement is part of the rule rather than a footnote.
