import { Translator } from '@eyra/feldspar'
import { DEFAULT_UI_LOCALE } from '../../locale/policy'
import { resolvePlaceholder } from './placeholder'

// Mirror the fork's real wiring (App.tsx -> ScriptHostComponent) so this test
// sees the same fallback chain as the running app (ADR-0037).
beforeAll(() => {
  Translator.setDefaultLocale(DEFAULT_UI_LOCALE)
})

describe('resolvePlaceholder', () => {
  it('prefers the protocol-supplied example when present', () => {
    const example = {
      translations: {
        en: 'Example: takeout-...-1-001.zip, takeout-...-2-001.zip',
        nl: 'Voorbeeld: takeout-...-1-001.zip, takeout-...-2-001.zip',
      },
    }
    expect(resolvePlaceholder(example, 'en')).toBe('Example: takeout-...-1-001.zip, takeout-...-2-001.zip')
    expect(resolvePlaceholder(example, 'nl')).toBe('Voorbeeld: takeout-...-1-001.zip, takeout-...-2-001.zip')
  })

  it('falls back to the built-in per-locale placeholder copy when example is absent', () => {
    // No current caller (including e2etest_multifile) actually sends a
    // PropsUIPromptFileInputMultiple with no `example` key — port_helpers
    // sets it unconditionally for multiple=True — so this covers the
    // fallback as defense-in-depth, not a live platform's real payload.
    expect(resolvePlaceholder(undefined, 'en')).toBe('E.g. data.zip')
    expect(resolvePlaceholder(undefined, 'nl')).toBe('Voorbeeld: data.zip')
    expect(resolvePlaceholder(undefined, 'de')).toBe('Z.B. data.zip')
    expect(resolvePlaceholder(undefined, 'it')).toBe('Esempio: data.zip')
    expect(resolvePlaceholder(undefined, 'es')).toBe('Ejemplo: data.zip')
  })

  it('resolves the example through the same locale-fallback chain as other UI text', () => {
    // Only en+nl present on the example -> de falls back to default locale (en).
    const example = { translations: { en: 'Example: a.zip, b.zip', nl: 'Voorbeeld: a.zip, b.zip' } }
    expect(resolvePlaceholder(example, 'de')).toBe('Example: a.zip, b.zip')
  })
})
