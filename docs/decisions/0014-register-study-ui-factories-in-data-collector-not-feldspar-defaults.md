---
status: accepted
date: "2026-03-13"
tags:
    - factory
    - extensibility
    - rendering
category: Feldspar
applies_to:
    - packages/feldspar/src/framework/visualization/react/ui/prompts/factory.ts
    - packages/feldspar/src/framework/visualization/react/factory.tsx
    - packages/data-collector/src/App.tsx
    - packages/data-collector/src/components/**/factory.tsx
    - packages/data-collector/src/factories/**/*.tsx
priority: default
checks:
    - desc: no study-specific TextAreaFactory in feldspar's default factories
      grep: 'TextAreaFactory'
      in: ["packages/feldspar/src/**"]
      expect: absent
---

# Register study UI factories in data-collector, not feldspar defaults

## Decision

Feldspar renders prompts through a first-match-wins `PromptFactory` chain (`ReactFactory.createPage`); study-specific UI — the component and its factory — lives in `packages/data-collector/`, with factories registered in `App.tsx` (appended ahead of feldspar's defaults), never added to feldspar's own default list.

## Guidance

- Add a new prompt by writing it in data-collector as `src/components/<name>/{types,component,factory}.tsx` and registering its factory in `App.tsx`; don't add it to feldspar's default list (`createPromptFactoriesWithDefaults` / `ReactFactory`) and don't put a `PromptFactory` in `packages/feldspar/`.
- Order matters — first match wins, so a study factory must precede the feldspar default that would otherwise claim the page.
- Feldspar's defaults cover only its generic prompt types; study types live in data-collector. (The branch that added a `TextAreaFactory` to feldspar's default list instead of data-collector is the violation this prevents.)
