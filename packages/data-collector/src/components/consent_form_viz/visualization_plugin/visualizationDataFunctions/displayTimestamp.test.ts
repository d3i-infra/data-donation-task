import { displayTimestamp, isValidTimeZone } from './util'

describe('displayTimestamp', () => {
  it('formats an instant in the display zone and locale', () => {
    const out = displayTimestamp({ status: 'instant', epochMs: Date.UTC(2026, 5, 15, 18, 30, 41) }, 'Europe/Amsterdam', 'nl')
    expect(out).toContain('20:30:41')
    expect(out).toMatch(/15 jun/i)
  })
  it('formats a local wall clock as written', () => {
    const out = displayTimestamp({ status: 'local', wallClockMs: Date.UTC(2026, 5, 15, 20, 30, 41) }, 'Pacific/Auckland', 'en')
    expect(out).toContain('8:30:41')
  })
  it('an empty value shows an empty cell, never the raw None/null text', () => {
    expect(displayTimestamp({ status: 'empty' }, 'Europe/Amsterdam', 'en')).toBe('')
  })
  it('returns null for statuses that cannot be shown, so the raw cell is used', () => {
    for (const status of ['invalid', 'ambiguous', 'unsupported'] as const) {
      expect(displayTimestamp({ status } as any, 'Europe/Amsterdam', 'en')).toBeNull()
    }
  })
  it('constructs one formatter per locale and zone (ADR-0035)', () => {
    const ctor = jest.spyOn(Intl, 'DateTimeFormat')
    for (let i = 0; i < 5000; i++) displayTimestamp({ status: 'instant', epochMs: 1781548241000 + i }, 'Europe/Madrid', 'es')
    expect(ctor.mock.calls.length).toBeLessThanOrEqual(1)
    ctor.mockRestore()
  })
})

describe('isValidTimeZone (final review S1)', () => {
  it('accepts a real IANA zone', () => {
    expect(isValidTimeZone('Europe/Amsterdam')).toBe(true)
    expect(isValidTimeZone('UTC')).toBe(true)
  })
  it('rejects a zone the browser does not recognise, without throwing', () => {
    expect(() => isValidTimeZone('Not/AZone')).not.toThrow()
    expect(isValidTimeZone('Not/AZone')).toBe(false)
    expect(isValidTimeZone('')).toBe(false)
  })
})
