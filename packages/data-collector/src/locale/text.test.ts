import { Translator, MISSING_TRANSLATION } from '@eyra/feldspar'
import TextBundle from '@eyra/feldspar'
import { DEFAULT_UI_LOCALE } from './policy'
import { resolveAll, resolveFlatText, resolveText } from './text'

// App.tsx hands the fork's default locale to ScriptHostComponent, which calls
// Translator.setDefaultLocale before anything renders. Mirror that wiring here
// so these tests see the same fallback chain as the running app.
beforeAll(() => {
  Translator.setDefaultLocale(DEFAULT_UI_LOCALE)
})

describe('resolveText', () => {
  it('passes strings through unchanged', () => {
    expect(resolveText('already text', 'nl')).toBe('already text')
  })

  it('resolves a nested translatable for an exact locale hit', () => {
    const text = { translations: { en: 'Delete', nl: 'Verwijder' } }
    expect(resolveText(text, 'nl')).toBe('Verwijder')
  })

  it('falls back to the default locale (en) when the locale is missing', () => {
    const text = { translations: { en: 'Delete', nl: 'Verwijder' } }
    expect(resolveText(text, 'de')).toBe('Delete')
  })

  it('falls back to the first available translation when en is missing too', () => {
    const text = { translations: { nl: 'Verwijder' } }
    expect(resolveText(text, 'de')).toBe('Verwijder')
  })

  it('resolves a TextBundle string table', () => {
    const bundle = new TextBundle().add('en', 'Search').add('nl', 'Zoeken')
    expect(resolveText(bundle, 'nl')).toBe('Zoeken')
    expect(resolveText(bundle, 'it')).toBe('Search')
  })

  it('treats an empty translation as a deliberate hit, not a miss', () => {
    const text = { translations: { en: 'Note', nl: '' } }
    expect(resolveText(text, 'nl')).toBe('')
  })

  it('returns the sentinel for an empty translations map', () => {
    expect(resolveText({ translations: {} }, 'en')).toBe(MISSING_TRANSLATION)
  })

  it.each([
    ['a number', 42],
    ['null', null],
    ['undefined', undefined],
    ['an empty object', {}],
    ['an array', ['en', 'nl']],
    ['a non-object translations field', { translations: 'oops' }],
    ['a null translations field', { translations: null }]
  ])('returns the sentinel for %s without throwing', (_label, junk) => {
    expect(() => resolveText(junk, 'en')).not.toThrow()
    expect(resolveText(junk, 'en')).toBe(MISSING_TRANSLATION)
  })

  it('uses the shared feldspar sentinel', () => {
    expect(MISSING_TRANSLATION).toBe('?text?')
  })
})

describe('resolveFlatText', () => {
  it('resolves a flat config record for an exact locale hit', () => {
    expect(resolveFlatText({ en: 'a', nl: 'b' }, 'nl')).toBe('b')
  })

  it('falls back to the default locale (en) for an unlisted locale', () => {
    expect(resolveFlatText({ en: 'a', nl: 'b' }, 'de')).toBe('a')
  })

  it('falls back to the first available entry when en is missing', () => {
    expect(resolveFlatText({ nl: 'b', it: 'c' }, 'de')).toBe('b')
  })

  it('passes strings through unchanged (config labels may be plain strings)', () => {
    expect(resolveFlatText('Timestamp', 'nl')).toBe('Timestamp')
  })

  it('treats an empty entry as a deliberate hit', () => {
    expect(resolveFlatText({ en: 'a', nl: '' }, 'nl')).toBe('')
  })

  it.each([
    ['a number', 42],
    ['null', null],
    ['undefined', undefined],
    ['an empty record', {}]
  ])('returns the sentinel for %s without throwing', (_label, junk) => {
    expect(() => resolveFlatText(junk, 'en')).not.toThrow()
    expect(resolveFlatText(junk, 'en')).toBe(MISSING_TRANSLATION)
  })
})

describe('resolveAll', () => {
  it('maps every key of a string table', () => {
    const translations = {
      delete: new TextBundle().add('en', 'Delete').add('nl', 'Verwijder'),
      undo: new TextBundle().add('en', 'Undo').add('nl', 'Herstel')
    }
    expect(resolveAll(translations, 'nl')).toEqual({ delete: 'Verwijder', undo: 'Herstel' })
    expect(resolveAll(translations, 'de')).toEqual({ delete: 'Delete', undo: 'Undo' })
  })

  it('keeps every key even when a value is junk', () => {
    expect(resolveAll({ good: 'ok', bad: 42 }, 'en')).toEqual({
      good: 'ok',
      bad: MISSING_TRANSLATION
    })
  })

  it('returns an empty record for an empty table', () => {
    expect(resolveAll({}, 'en')).toEqual({})
  })

  it.each([
    ['null', null],
    ['undefined', undefined],
    ['a string', 'not a table'],
    ['a number', 42],
    ['an array', ['en', 'nl']]
  ])('returns an empty record for %s without throwing', (_label, junk) => {
    expect(() => resolveAll(junk, 'en')).not.toThrow()
    expect(resolveAll(junk, 'en')).toEqual({})
  })
})
