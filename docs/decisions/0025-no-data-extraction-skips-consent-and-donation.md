---
status: accepted
date: "2026-03-17"
tags:
    - extraction
    - flowbuilder
    - ux
category: Extraction
applies_to:
    - packages/python/port/helpers/flow_builder.py
    - packages/python/port/helpers/port_helpers.py
priority: default
companions:
    - packages/python/tests/test_flow_builder.py
    - packages/python/tests/test_port_helpers.py
---

# No-data extraction skips consent and donation

## Decision

When extraction returns no tables (`not result.tables`), `FlowBuilder.start_flow()` renders `ph.render_no_data_page(platform_name)` and returns without consent or donation. A valid archive with no study-relevant data is an expected outcome, not an error.

## Guidance

- Do not show empty consent or donation UI for no-data results.
- Do not send no-data results through retry, safety-error, or validation-error flows.
- Build the acknowledgement through `port_helpers.render_no_data_page()`.
