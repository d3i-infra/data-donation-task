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
priority: default
companions:
    - packages/python/tests/test_flow_builder.py
---

# Use session-platform donation keys

## Decision

`FlowBuilder.start_flow()` donates with key `f"{session_id}-{platform_name.lower()}"`. This prevents collisions when one session donates multiple platform datasets and gives downstream code a stable platform marker.

## Guidance

- Use the session-platform key for every extraction donation; never donate under plain `session_id`.
- Treat `{session_id}-{platform}` as a downstream data contract; coordinate parser and storage changes before changing it.
