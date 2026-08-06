import { Translator, MISSING_TRANSLATION } from './translator'

const bundle = (t: Record<string, string>) => ({ translations: t })

describe('Translator.resolve chain', () => {
  afterEach(() => Translator.setDefaultLocale('nl'))
  it('returns exact locale match', () =>
    expect(Translator.translate(bundle({ en: 'Hi', nl: 'Hoi' }), 'nl')).toBe('Hoi'))
  it('falls back to default locale on missing key (the #112 regression)', () =>
    expect(Translator.translate(bundle({ nl: 'Hoi' }), 'es')).toBe('Hoi'))
  it('setDefaultLocale changes the fallback (en-first)', () => {
    Translator.setDefaultLocale('en')
    expect(Translator.translate(bundle({ en: 'Hi', nl: 'Hoi' }), 'de')).toBe('Hi')
  })
  it('en-only bundle under nl returns English, not blank (questionnaire bug)', () => {
    Translator.setDefaultLocale('en')
    expect(Translator.translate(bundle({ en: 'Continue' }), 'nl')).toBe('Continue')
  })
  it('empty string is a hit, not a miss (netflix contract)', () =>
    expect(Translator.translate(bundle({ en: '', nl: '' }), 'en')).toBe(''))
  it('first available when default locale absent', () =>
    expect(Translator.translate(bundle({ ro: 'Salut' }), 'es')).toBe('Salut'))
  it('sentinel on empty translations', () =>
    expect(Translator.translate(bundle({}), 'es')).toBe(MISSING_TRANSLATION))
  it('never returns undefined for any miss', () =>
    expect(typeof Translator.translate(bundle({ nl: 'x' }), 'zz')).toBe('string'))
  it('plain string passes through', () =>
    expect(Translator.translate('literal', 'es')).toBe('literal'))
  it('null translations object yields sentinel, not throw', () =>
    expect(Translator.translate({ translations: null as any }, 'en')).toBe(MISSING_TRANSLATION))
})
