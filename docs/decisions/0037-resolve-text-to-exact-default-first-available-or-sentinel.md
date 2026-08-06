---
status: accepted
date: "2026-08-05"
tags:
    - translation
    - locale
    - i18n
source: docs/superpowers/plans/2026-08-05-locale-translation-consolidation.md (Stage 1, Task 8)
category: Localization
applies_to:
    - packages/feldspar/src/framework/translator.ts
    - packages/feldspar/src/framework/text_bundle.ts
    - packages/feldspar/src/components/script_host_component.tsx
    - packages/feldspar/src/framework/processing/worker_engine.ts
    - packages/data-collector/public/py_worker.js
    - packages/python/port/main.py
    - packages/python/port/helpers/ui_locale.py
priority: invariant
companions:
    - packages/feldspar/src/framework/translator.test.ts
    - packages/feldspar/src/framework/text_bundle.test.ts
    - packages/python/tests/test_ui_locale.py
---

# Resolve text to exact, default, first-available, or sentinel

## Decision

`Translator.translate` is the sole production resolution path, falling through exact-locale match → `Translator`'s host-configurable default locale (`setDefaultLocale`; `ScriptHostComponent`'s `defaultLocale` prop) → first available translation → the `MISSING_TRANSLATION` sentinel (`'?text?'`), with `resolve()` never returning `undefined`. `TextBundle.resolve` mirrors the same chain but is test-/upstream-parity-only — no production caller reaches it directly.

## Guidance

- New text-resolving code must keep `resolve()`'s chain total — exact → `defaultLocale` → first available → `'?text?'` sentinel, never `undefined` — guarding with `typeof text === 'string'` checks and null-safe `?.`/`?? {}` access, matching `translator.ts`. `translate()`'s `TypeError` on non-string/non-`Translatable` input is separate entry-point junk-guarding, not part of the total chain (Stage 2 owns hardening that boundary further).
- The host-configurable default locale governs `Translator` only, via `ScriptHostComponent`'s `defaultLocale` prop (`Translator.setDefaultLocale`); don't add a second hardcoded fallback elsewhere. `TextBundle`'s own `defaultLocale` field is a separate, hardcoded `'nl'` value that no production code ever sets — it is not a second host-configured path.
- UI locale rides the `firstRunCycle` `data` dict (`{sessionId, locale, platform}`) into Python via `main.py`'s `start`, which stores it with `ui_locale.set_ui_locale`. Platform code reads it only through `ui_locale.get_ui_locale()` (default `"en"`) — never widen `module.process(session_id)`'s signature to take a locale (cross-ref ADR-0029's platform-dispatch contract).
- `ui_locale.py`'s UI locale is a distinct concept from `helpers/validate.py`'s DDP-export `Language` enum (parsing language of exported data) — the two must never be synced or conflated.

## Why

Malformed or partial translation bundles are routine (a platform config missing a locale, UI text authored in only one language) — participant-facing UI must never blank out or crash on a missing key mid-flow, so the resolver is defined as total (always returns a displayable string) rather than partial, with the sentinel as a visible-but-safe last resort. Centralizing the fallback chain in one place, rather than each caller null-checking its own bundle, keeps behavior consistent across the UI and the visualization layer. Keeping UI locale (a rendering choice) separate from Python's file-parsing language enum prevents a Python change from silently altering what language the frontend renders in, or vice versa.

## Checks

- Confirm `translator.ts`'s `resolve` and `text_bundle.ts`'s `resolve` never have a code path returning `undefined`: every branch ends in a `typeof === 'string'` return or the `MISSING_TRANSLATION` sentinel.
- Confirm `main.py`'s `start` calls `ui_locale.set_ui_locale` before delegating to `process()` and that `process(session_id)` itself takes no locale argument.
