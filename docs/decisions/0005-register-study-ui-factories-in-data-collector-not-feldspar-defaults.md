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

Feldspar renders prompts through a first-match-wins `PromptFactory` chain (`createPromptFactoriesWithDefaults`, iterated in `DataSubmissionPage`); study-specific UI — the component and its factory — lives in `packages/data-collector/`, with factories registered in `App.tsx` (appended ahead of feldspar's defaults), never added to feldspar's own default list.

## Guidance

- Add a new prompt by writing it in data-collector as `src/components/<name>/` (`types.ts`, `<name>.tsx`, `factory.tsx`) and registering its factory in `App.tsx`; don't add it to feldspar's default list (`createPromptFactoriesWithDefaults` / `ReactFactory`) and don't put a `PromptFactory` in `packages/feldspar/`.
- Order matters — first match wins, so a study factory must precede the feldspar default that would otherwise claim the page.
- Feldspar's defaults cover only its generic prompt types; study types live in data-collector.

## Why

New prompt types keep arriving as studies evolve, so the type→component mapping must extend without modifying feldspar — that separation is what keeps feldspar a stable library whose upgrades never collide with study code, and it keeps upstream merges cheap (the chain is Eyra's inherited design). A researcher adds UI by touching only their own app's `App.tsx`. The costs are real: first-match ordering means a mis-ordered factory silently loses to a default, and the boundary has been violated before (a study `TextAreaFactory` added to feldspar's defaults — now a standing check).
