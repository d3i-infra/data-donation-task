import { normalizeLocale, SUPPORTED_UI_LOCALES, DEFAULT_UI_LOCALE } from './policy'

describe('normalizeLocale', () => {
  it('should normalize es-ES to es', () => {
    expect(normalizeLocale('es-ES')).toBe('es')
  })

  it('should normalize EN to en', () => {
    expect(normalizeLocale('EN')).toBe('en')
  })

  it('should normalize nl_NL to nl', () => {
    expect(normalizeLocale('nl_NL')).toBe('nl')
  })

  it('should return default locale for unsupported locale ro', () => {
    expect(normalizeLocale('ro')).toBe(DEFAULT_UI_LOCALE)
  })

  it('should return default locale for empty string', () => {
    expect(normalizeLocale('')).toBe(DEFAULT_UI_LOCALE)
  })

  it('should return default locale for undefined', () => {
    expect(normalizeLocale(undefined)).toBe(DEFAULT_UI_LOCALE)
  })

  it('should return default locale for non-string input (number)', () => {
    expect(normalizeLocale(42)).toBe(DEFAULT_UI_LOCALE)
  })

  it('should only use SUPPORTED_UI_LOCALES for validation', () => {
    // Verify en is in the supported list
    expect(SUPPORTED_UI_LOCALES).toContain('en')
    // Verify ro is NOT in the supported list
    expect(SUPPORTED_UI_LOCALES).not.toContain('ro')
  })
})
