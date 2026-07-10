---
status: accepted
date: "2026-03-17"
tags:
    - donation
    - multi-platform
    - data-pipeline
category: Extraction
applies_to:
    - packages/python/port/helpers/flow_builder.py
    - packages/python/port/platforms/**/*.py
priority: invariant
companions:
    - packages/python/tests/test_flow_builder.py
---

# Use session-platform donation keys

## Decision

`FlowBuilder.start_flow()` donates with key `f"{session_id}-{platform_key}"`, where the platform segment is a key-safe machine identifier (currently derived as `platform_name.lower()`, so the name each platform passes to `FlowBuilder.__init__` becomes part of the data contract).

## Guidance

- Use the session-platform key for every extraction donation; never donate under plain `session_id`.
- Treat `{session_id}-{platform_key}` as a downstream data contract; coordinate parser and storage changes before changing the format.
- The platform segment is a machine key, never UI display text: lowercase, no spaces, no hyphens. Session ids may themselves contain hyphens, so parsers take the platform from the substring after the *final* hyphen — a platform key containing a separator breaks every downstream parser.
- The consent-gated error report (`error_flow` in `main.py`) donates under its own fixed `error-report` key; it is not an extraction donation and stays outside this scheme.

## Why

Multi-platform studies are the standard case: one session donates once per platform, so plain `session_id` keys collide in storage. `{session_id}-{platform}` is unique, readable (`abc123-facebook`), and lets pipelines parse the platform back out — a UUID would be unique but opaque. It was a deliberate breaking change, so the key is a contract: storage and parsers expect it, and format changes must be coordinated, not made unilaterally in FlowBuilder. Session ids can contain hyphens (`abc-123`), so parsers read the platform from the final hyphen — which is why the platform segment must be a constrained machine key, not whatever the UI happens to call the platform.
