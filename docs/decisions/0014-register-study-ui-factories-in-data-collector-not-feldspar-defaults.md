---
adr_id: "0001"
comments:
    - author: Danielle McCool
      comment: "1"
      date: "2026-03-13 13:30:02"
links:
    precedes: []
    succeeds: []
status: accepted
date: 2026-03-13
tags:
    - factory
    - extensibility
    - rendering
title: Factory pattern for UI extensibility
---

## <a name="question"></a> Context and Problem Statement

Python yields CommandUIRender(page) containing typed props (PropsUIPage with a body list of PropsUIPrompt subtypes). TypeScript must render the matching React component for each prop type. New prop types will be added as the study grows. How should prop types be mapped to React components in an extensible way?

## <a name="options"></a> Considered Options
1. <a name="option-1"></a> Hard-coded switch statement on prop __type__
2. <a name="option-2"></a> Factory chain — each PromptFactory checks __type__ and returns element or null
3. <a name="option-3"></a> Registry map from __type__ string to component constructor

## <a name="criteria"></a> Decision Drivers

* New prompt types will be added as the study evolves; the mapping mechanism must be extensible without modifying feldspar
* Researchers should be able to add their own prompt components by only modifying data-collector's App.tsx
* The design is inherited from Eyra and worth preserving for upstream compatibility

## <a name="outcome"></a> Decision Outcome
We decided for [Option 2](#option-2) because: The factory chain is Eyra's design: DataSubmissionPageFactory accepts a list of PromptFactory instances, each with create(body, context) returning JSX or null; first match wins. Researchers add factories in App.tsx without touching feldspar's factory list, which is the extensibility point the design is built around.

### Consequences

* Good: Adding a new prompt type requires only creating a new factory in data-collector and registering it in App.tsx — no feldspar changes needed
* Good: Feldspar ships a default factory list covering its own prompt types; study-specific factories are appended in data-collector
* Bad: The "first match wins" semantics means factory ordering in App.tsx matters; incorrect ordering silently skips components

### Evidence of violation

The `feat/facebook-ddp-error-handling` branch added a `TextAreaFactory` to feldspar's default factory list rather than registering it in data-collector's App.tsx.

## More Information

See [data-collector/AD0001](../data-collector/AD0001-custom-ui-components-belong-in-data-collector-not-feldspar.md) for where custom factories are registered.
See [fork-governance/AD0004](../fork-governance/AD0004-three-package-monorepo-with-distinct-modification-policies.md) for the package boundary that makes this separation possible.

## <a name="comments"></a> Comments
<a name="comment-1"></a>1. (2026-03-13 13:30:02) Danielle McCool: marked decision as decided
