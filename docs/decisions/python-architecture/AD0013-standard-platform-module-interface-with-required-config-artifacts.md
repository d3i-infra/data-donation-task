---
status: accepted
date: "2026-05-20"
tags:
    - platform-dispatch
    - platform-interface
    - script
    - config
category: Python architecture
applies_to:
    - packages/python/port/platforms/**/*.py
    - packages/python/port/script.py
priority: default
---

# Standard platform module interface

## Decision

`script.py` stays platform-agnostic: it validates the platform's config, imports `port.platforms.<platform>`, and dispatches through `module.process(session_id)` alone — never naming a platform. Around that seam, each platform module follows a common authoring convention — `EXTRACTOR_REGISTRY`, `extraction(...)`, a `<Platform>Flow(FlowBuilder)` subclass, and `process(session_id)` — with documented signature exceptions.

## Guidance

- `script.py`'s only dispatch dependency is `module.process(session_id)`: it calls `validate_or_raise(platform)`, imports `port.platforms.<platform>`, and calls `process()`. No platform names, no `PLATFORM_REGISTRY`.
- Platform-authoring convention (used inside the module, not by the dispatcher): each exposes `EXTRACTOR_REGISTRY` (ordered `dict[str, Callable[..., pd.DataFrame]]`), `extraction(...)`, a `<Platform>Flow(FlowBuilder)` subclass, and `process(session_id)` returning `<Platform>Flow(session_id).start_flow()`. `example.py` is the canonical template.
- A runnable/released platform needs a generated `configs/<platform>_config.json`, but those are generated on demand (config lifecycle is AD0014) and validated at runtime by `script.py`; only `example_config.json` is committed. Adding a platform still requires no change to `script.py`.
- Documented signature exceptions, both still exposing all four convention symbols: **Netflix** keeps `run_extraction` but with a different `extraction(reader, selected_user)` shape; **WhatsApp** has an `extraction(df)` shape and still calls `load_port_config`, but *bypasses* `run_extraction`, building its tables in its own loop.

## Why

`script.py`'s only dependency on a platform is `module.process(session_id)`, so adding a platform never touches it and no `PLATFORM_REGISTRY` list has to be maintained (that list was a real burden on master). The authoring convention around the seam (`EXTRACTOR_REGISTRY`, `extraction`, `<Platform>Flow`) is deliberately conventional rather than enforced: Netflix (`extraction(reader, selected_user)`) and WhatsApp (pre-parsed DataFrame, own table loop) genuinely need different shapes. Costs: the `"<platform>"` string is duplicated across filename, module path, and `load_port_config` with nothing cross-checking them, and conformance rests on review and the `example.py` template, not a gate.

## Checks

- Confirm each *platform* module under `port/platforms/` (excluding `__init__.py` and any non-platform support files) exposes `EXTRACTOR_REGISTRY`, `extraction`, a `<Platform>Flow(FlowBuilder)` subclass, and `process`; allowlist Netflix/WhatsApp for signature divergence only.
- Confirm `script.py` dispatches only via `validate_or_raise` + `import_module("port.platforms.<platform>")` + `process()`, with no per-platform names or `PLATFORM_REGISTRY`.
