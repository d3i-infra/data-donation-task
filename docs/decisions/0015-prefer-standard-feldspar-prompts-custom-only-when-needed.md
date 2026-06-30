---
adr_id: "0002"
comments: []
links:
    precedes: []
    succeeds: []
status: open
tags:
    - prompt-components
    - feldspar-compatibility
    - ux
title: Standard feldspar prompts vs D3I custom prompt components
---

## <a name="question"></a> Context and Problem Statement

D3I has custom prompt components (PropsUIPromptRetry, PropsUIPromptConsentFormViz, PropsUIPromptFileInputMultiple) registered via the factory pattern in data-collector. These require custom React components and factory registration. Standard feldspar provides PropsUIPromptConfirm, PropsUIPromptConsentForm, PropsUIPromptFileInput which work without custom code. When should we use standard feldspar components vs D3I custom components? PropsUIPromptRetry was found to only render one button (no cancel/continue) — exposed when FlowBuilder's control flow was corrected to actually check the retry response.

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Prefer standard feldspar components — use custom only when standard cannot provide the UX
2. <a name="option-2"></a> Maintain D3I custom component suite — invest in quality and testing
3. <a name="option-3"></a> Hybrid — use standard for flow control prompts and custom for data display

## <a name="criteria"></a> Decision Drivers
Standard feldspar components work on any feldspar host without custom factories
D3I custom components require factory registration in data-collector — they break on vanilla feldspar
PropsUIPromptRetry has a UX bug (single button) that went undetected because old code ignored the response
Upstream feldspar may add or change standard components — custom D3I components must track these changes
Some D3I features genuinely need custom UI (visualizations in consent forms) that standard components cannot provide
### Pros and Cons

**Prefer standard feldspar components — use custom only when standard cannot provide the UX**
* Good, because works on any feldspar host
* Good, because upstream changes benefit us automatically
* Bad, because standard components may not match desired UX exactly

**Maintain D3I custom component suite — invest in quality and testing**
* Good, because full control over UX
* Bad, because maintenance burden and factory registration overhead
* Bad, because custom components can have undetected bugs like PropsUIPromptRetry

**Hybrid — use standard for flow control prompts and custom for data display**
* Good, because flow prompts (retry, confirm, file input) are simple enough for standard
* Good, because data display (consent with visualizations) genuinely needs custom
* Neutral, because requires clear policy on when to use which

