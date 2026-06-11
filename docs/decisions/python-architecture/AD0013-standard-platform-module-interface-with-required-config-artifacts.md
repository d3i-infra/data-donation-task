---
adr_id: "0013"
comments: []
links:
    precedes:
        - "0012"
    succeeds: []
status: accepted
date: 2026-05-20
tags:
    - platform-dispatch
    - platform-interface
    - script
    - config
title: Standard platform module interface and config file contract
---

## Context and Problem Statement

`script.py` dispatches to one of N platform modules based on VITE_PLATFORM. To keep script.py platform-agnostic, every module in port/platforms must expose a uniform interface. What should that interface be, and how strictly should it be enforced?

## Considered Options

1. Loose contract -- let each module export whatever it needs; dispatch via convention
2. Strict contract with signature enforcement at startup (runtime introspection of each module).
3. Conventional contract with documented exceptions for modules that genuinely need different shapes

## Decision drivers

- New platforms should be addable without modifying `script.py`
- `script.py` can't reference all platforms by name (PLATFORM_REGISTRY on master was a maintenance burden)
- Existing platforms with diverging signatures (WhatsApp, Netflix) must keep working

## Decision Outcome

Chosen: Option 3 -- Conventional contract with documented exceptions

Every module in `port/platforms` exposes:

- `EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]]` -- string named extractor functions, in declaration order (preserved by Python dict)
- `extraction(...)` -- the module's entry point that orchestrates ``load_port_config(EXTRACTOR_REGISTRY, "<platform>"` and `run_extraction(reader, errors, config)`. Signature varies
- `<Platform>Flow(FlowBuilder)` -- flow subclass wiring `validate_file` and `extract_data`
- `process(session_id)` -- entry point invoked by `script.py`; returns `<Platform>Flow(session_id).start_flow()`.

## Consequences

- Good: `script.py` is fully platform-agnostic; adding a platform requires only a new module + config file
- Good: `example.py` serves as the canonical template for new platforms
- Bad: The string "<platform>" is duplicated in three places per module (folename, module path, load_port_config argument) and nothing cross-checks them.
- Bad: The exceptions create a contract that's strict-in-principle but loose-in-practice
