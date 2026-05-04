# Architecture

How the data donation task is built and how its parts work together.

---

## Reading order

If you're new to the codebase, read these in order:

1. [System overview](01-overview.md) — what it is, how it fits with Eyra mono
2. [The run cycle](02-run-cycle.md) — the Python↔JS co-routine that drives everything
3. [Command protocol](03-command-protocol.md) — the four command types and their responses
4. [FlowBuilder](04-flowbuilder.md) — the per-platform flow lifecycle
5. [Extraction](05-extraction.md) — zip reading, validation, ExtractionResult
6. [Logging](06-logging.md) — two paths to the host, PII rules
7. [Error handling](07-error-handling.md) — ScriptWrapper, error_flow, the PII boundary
8. [Rendering](08-rendering.md) — how commands become React components, the factory system, consent form round-trip

---

## Quick answers

| Question | Document |
|---|---|
| What is data-donation-task and how does it talk to Eyra? | [Overview](01-overview.md) |
| How does Python run in a browser? | [Run cycle](02-run-cycle.md) |
| What is a Command? What types exist? | [Command protocol](03-command-protocol.md) |
| What does FlowBuilder do? How do I extend it? | [FlowBuilder](04-flowbuilder.md) |
| How does extraction work? | [Extraction](05-extraction.md) |
| How do log messages reach the host? | [Logging](06-logging.md) |
| What happens when Python throws an exception? | [Error handling](07-error-handling.md) |
| Where is the PII safety boundary? | [Error handling § PII safety](07-error-handling.md#pii-safety-boundary) |
| What is the difference between LiveBridge and FakeBridge? | [Overview § The bridge](01-overview.md#the-bridge) |
| How do Python commands turn into React UI? | [Rendering](08-rendering.md) |
| What is the factory system? How do I add a custom component? | [Rendering § Prompt factories](08-rendering.md#prompt-factory-reference) |
| What is data-collector vs feldspar? | [Rendering § The two packages](08-rendering.md#the-two-packages) |
| Can the participant edit data before donating? | [Rendering § Consent form round-trip](08-rendering.md#the-consent-form-data-round-trip) |
| How does the iframe communicate with Eyra mono? | [Rendering § Iframe lifecycle](08-rendering.md#iframe-lifecycle) |
