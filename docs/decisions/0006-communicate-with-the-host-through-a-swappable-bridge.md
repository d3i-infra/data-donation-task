---
status: accepted
date: "2026-03-13"
tags:
    - bridge
    - iframe
    - postmessage
    - donate-protocol
category: Feldspar
applies_to:
    - packages/feldspar/src/framework/types/modules.ts
    - packages/feldspar/src/live_bridge.ts
    - packages/feldspar/src/fake_bridge.ts
    - packages/feldspar/src/framework/command_router.ts
    - packages/feldspar/src/framework/assembly.ts
    - packages/feldspar/src/components/script_host_component.tsx
priority: default
---

# Communicate with the host through a swappable Bridge

## Decision

The workflow reaches its host only through a `Bridge` — `LiveBridge` (postMessage to the Eyra/mono iframe host) in production, `FakeBridge` in development — chosen in `ScriptHostComponent` and injected via `Assembly` into `CommandRouter`, so command-routing code never names a transport.

## Guidance

- Route host commands through the injected `Bridge` via `CommandRouter`; don't call `postMessage` from workflow/command code or hard-code a transport.
- Select the transport in `ScriptHostComponent` (prod `LiveBridge`, dev `FakeBridge`) and thread it through `Assembly` — `ScriptHostComponent` takes no `bridge` prop.
- `Bridge.send()` returns `Promise<ResponseSystemDonate | void>`: `CommandRouter` returns a `PayloadResponse` for a resolved donate and `PayloadVoid` otherwise. Every bridge resolves a donate with a `ResponseSystemDonate` — no flag gates it: `LiveBridge` awaits the host's acknowledgment, `FakeBridge` (which has no host) synthesizes one from its own POST result. (The Python side reads that payload through `handle_donate_result()` — its own rule.)

## Why

The workflow runs in two contexts — the Eyra/mono iframe in production, standalone in dev — and workflow logic must not know which: the same Python generator drives both, and `FakeBridge` makes local development possible without a running host. The seam has proven itself: the awaited donation protocol (pending-donation tracking, `ResponseSystemDonate`, port replacement) landed entirely inside the bridge/router, `FakeBridge` evolved in lockstep, and no workflow code changed.
