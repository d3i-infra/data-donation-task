---
status: accepted
date: "2026-08-07"
tags:
    - locale
    - i18n
    - translation
category: Localization
applies_to:
    - packages/data-collector/src/locale/policy.ts
    - packages/data-collector/src/locale/ui_locales.json
    - packages/data-collector/src/App.tsx
    - packages/python/port/helpers/ui_locale.py
    - packages/python/port/helpers/ui_locales.json
    - packages/python/tests/test_ui_locales_sync.py
priority: invariant
companions:
    - packages/data-collector/src/locale/policy.test.ts
    - packages/python/tests/test_ui_locale.py
    - tests/localization.spec.ts
    - packages/python/port/helpers/port_config_validator.py
checks:
    - desc: the supported-locale list is declared only in the two policy modules
      grep: 'SUPPORTED_UI_LOCALES[^=\n]*='
      in: ["packages/data-collector/src/**", "packages/python/port/**"]
      except: ["packages/data-collector/src/locale/policy.ts", "packages/python/port/helpers/ui_locale.py", "**/*.test.ts", "**/__pycache__/**"]
      expect: absent
    - desc: normalizeLocale is handed to the host boundary, never called a second time
      grep: 'normalizeLocale\('
      in: ["packages/data-collector/src/**"]
      except: ["**/locale/policy.test.ts"]
      expect: absent
---

# Normalize the UI locale once at the data-collector boundary

## Decision

The supported participant-facing UI locales (`en`, `nl`, `de`, `it`, `es`; default `en`) are declared once in `ui_locales.json` — the data-collector copy is canonical, the packaged Python copy is a byte-identical mirror — and a requested locale is normalized into that set exactly once, at the host boundary, by passing `normalizeLocale` as `ScriptHostComponent`'s `mapLocale` prop.

## Guidance

- Adding or dropping a UI locale is an edit to `packages/data-collector/src/locale/ui_locales.json` copied byte-for-byte into `packages/python/port/helpers/ui_locales.json`; nothing else may hardcode a locale list, and `tests/test_ui_locales_sync.py` hard-fails (never skips) when the two drift.
- Normalize once, at the boundary: `App.tsx` hands `normalizeLocale` to `mapLocale`, and everything downstream — both engines, the worker handshake, Python — receives an already-normalized value. `ui_locale.normalize_ui_locale` is defense-in-depth for a caller that bypasses the host, never a second policy source; if it ever disagrees with `policy.ts`, `policy.ts` is right.
- One carrier: the locale rides the existing prop/handshake chain (`App.tsx` → `ScriptHostComponent` → `Assembly` → `WorkerProcessingEngine` → `port.start`'s context dict → `ui_locale.set_ui_locale`). Don't add a React context, a module-level singleton, or a second carrier alongside it.
- `en` is both default and fallback: every participant-facing text bundle must carry `en`. `de`, `it`, and `es` are provisional — machine-translated, pending native-speaker review — listed in the `provisional` key and marked `*` in the coverage report; treat them as shippable chrome, not as reviewed copy.
- Researcher-facing coverage is gated by `validate_port_config.py --report`: chained after generation in `scripts/gen_port_config.sh`, run per platform in `release.sh` before anything is built, and run over every regenerated config (`--all --report`) in the `dependency-updates.yml` workflow. A bundle missing the default locale is an error there; an unsupported locale key is a warning. `validate_or_raise` at startup is the last line of defense, not the gate.
- The UI locale never syncs with `helpers/validate.py`'s DDP-export `Language` enum or a config's `platform_info.languages` — those name the language of the participant's exported data, not the language the UI renders in.
- Locale *policy* stays out of `packages/feldspar/`: the framework may gain only generic, policy-free host hooks (`mapLocale`, `defaultLocale`), and the set, the default, and the normalization rule live in `data-collector` (ADR-0002).

## Why

Every place that decides "is this locale supported?" is a place the answer can differ. The live failure this rule was written for was a host-supplied `ro`: feldspar's own bundles carry Romanian strings, study config carries none, so an unnormalized locale produced a half-Romanian page no researcher had ever seen. One declaration and one normalization point keep the rendered locale, the locale Python is told about, and the locale the coverage report grades against the same value.
