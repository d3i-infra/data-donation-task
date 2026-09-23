// Test-only stand-in for the `@eyra/feldspar` package specifier.
//
// Jest cannot resolve the built package: `@eyra/feldspar`'s exports map
// declares only an "import" (ESM) condition and no "require" one, so the
// CommonJS resolution jest performs for a bare specifier fails outright
// ("Cannot find module '@eyra/feldspar'"). Building the package first would
// not help — the condition, not the artifact, is what is missing.
//
// So `jest.config.js` maps the specifier here and this module re-exports the
// same symbols straight from feldspar *source*. Tests therefore exercise the
// real resolver, not a mock: behaviour asserted here is the behaviour that
// ships. Only pure modules are re-exported (no React, no worker bridge), so
// the import graph stays small and side-effect free.
//
// Add a symbol here only when a `*.test.ts` needs it.

export { Translator, MISSING_TRANSLATION } from '../../../feldspar/src/framework/translator'
export { default } from '../../../feldspar/src/framework/text_bundle'
