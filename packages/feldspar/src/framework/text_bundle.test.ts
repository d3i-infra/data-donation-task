import TextBundle from './text_bundle'
import { MISSING_TRANSLATION } from './translator'

describe('TextBundle.resolve', () => {
  let bundle: TextBundle

  beforeEach(() => {
    bundle = new TextBundle()
  })

  it('returns exact-hit translation for requested locale', () => {
    bundle.add('en', 'Hello')
    bundle.add('nl', 'Hallo')
    expect(bundle.resolve('en')).toBe('Hello')
  })

  it('returns empty-string as valid translation (not a fallback trigger)', () => {
    bundle.add('en', '')
    bundle.add('nl', 'Hallo')
    expect(bundle.resolve('en')).toBe('')
  })

  it('falls back to defaultLocale when requested locale missing', () => {
    bundle.add('nl', 'Hallo')
    bundle.add('es', 'Hola')
    expect(bundle.resolve('en')).toBe('Hallo')
  })

  it('returns first available translation when both locale and defaultLocale missing', () => {
    bundle.add('es', 'Hola')
    bundle.add('fr', 'Bonjour')
    // Should return one of the available translations (order depends on Object.values)
    const result = bundle.resolve('en')
    expect(['Hola', 'Bonjour']).toContain(result)
  })

  it('returns MISSING_TRANSLATION sentinel when translations empty', () => {
    expect(bundle.resolve('en')).toBe(MISSING_TRANSLATION)
  })

  it('translate method escapes the resolved text', () => {
    bundle.add('en', '<script>alert("xss")</script>')
    const result = bundle.translate('en')
    expect(result).toBe('&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;')
  })

  it('handles defaultLocale field override', () => {
    bundle.defaultLocale = 'es'
    bundle.add('es', 'Hola')
    bundle.add('nl', 'Hallo')
    expect(bundle.resolve('en')).toBe('Hola')
  })

  it('prefers exact locale match over defaultLocale', () => {
    bundle.add('en', 'Hello')
    bundle.add('nl', 'Hallo')
    expect(bundle.resolve('en')).toBe('Hello')
  })
})
