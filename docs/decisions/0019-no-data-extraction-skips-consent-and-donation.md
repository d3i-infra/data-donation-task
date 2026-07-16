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
    - packages/python/port/helpers/table_extractor.py
priority: default
companions:
    - packages/python/tests/test_flow_builder.py
    - packages/python/tests/test_port_helpers.py
---

# No-data extraction skips consent and donation

## Decision

When extraction returns no tables (`not result.tables`), `FlowBuilder.start_flow()` renders `ph.render_no_data_page(platform_name)`, awaits acknowledgment, and returns without consent or donation.

## Guidance

- On empty `result.tables`, render `ph.render_no_data_page(platform_name)` and return — never show a consent form or submit a donation for a no-data result.
- Do not route no-data results through the retry, safety-error, or validation-error flows; the participant gets an acknowledgment, not an error or another upload prompt.
- Build the acknowledgment through `port_helpers.render_no_data_page()`, not an inline page in FlowBuilder or a platform module.
- `run_extraction()` already drops empty DataFrames from `tables`, so "every table came back empty" and "no tables" are the same case at this check.
- No-data means a *clean* empty extraction. Zero tables with a nonempty `result.errors` is an extraction failure, not a no-data outcome — do not present it to the participant as "no relevant data was found".

## Why

A valid DDP with no study-relevant data is a normal outcome, but the old flow showed an empty consent form — a donate button with nothing to review — misleading the participant and putting empty donations in the pipeline. An explicit acknowledgment gives closure and keeps empty payloads out of the dataset; routing to retry instead would loop participants on a file that is already correct. The separation cuts both ways: no-data must stay distinguishable from extraction bugs, so zero tables *with* extraction errors must not borrow the no-data message — that tells the participant their data wasn't relevant when the extractors actually broke.
