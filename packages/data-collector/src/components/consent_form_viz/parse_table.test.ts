import { parseTable } from './parse_table'
import { PropsUIPromptConsentFormTableViz } from './types'

// Minimal, valid table config; each test overrides only the field it exercises.
function baseTable(overrides: Partial<PropsUIPromptConsentFormTableViz> = {}): PropsUIPromptConsentFormTableViz {
  return {
    __type__: 'PropsUIPromptConsentFormTableViz',
    id: 't1',
    title: 'Title',
    description: 'Description',
    data_frame: { col_a: { '0': 'value' } },
    visualizations: [],
    folded: false,
    delete_option: false,
    ...overrides,
  }
}

describe('parseTable date_columns/date_locale/display_timezone validation (final review S1)', () => {
  it('passes a valid date_columns declaration through, normalising encoding to an array (ts-idiom T9)', () => {
    const table = baseTable({ date_columns: { col_a: { encoding: 'epoch-seconds' } } })
    const result = parseTable(table, 'en')
    expect(result.dateColumns).toEqual({ col_a: { encoding: ['epoch-seconds'] } })
  })

  it('drops an unknown encoding rather than letting it reach table.tsx, and never throws', () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
    const table = baseTable({ date_columns: { col_a: { encoding: 'epoch-millis' } } as any })

    let result: ReturnType<typeof parseTable> | undefined
    expect(() => { result = parseTable(table, 'en') }).not.toThrow()

    expect(result!.dateColumns).toBeUndefined()
    expect(errorSpy).toHaveBeenCalledTimes(1)
    errorSpy.mockRestore()
  })

  it('drops a date_columns value that is not an object at all, and never throws', () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
    const table = baseTable({ date_columns: 'col_a' as any })

    expect(() => parseTable(table, 'en')).not.toThrow()
    expect(parseTable(table, 'en').dateColumns).toBeUndefined()
    errorSpy.mockRestore()
  })

  it('passes through a valid date_locale and display_timezone', () => {
    const table = baseTable({ date_locale: 'nl', display_timezone: 'Europe/Amsterdam' })
    const result = parseTable(table, 'en')
    expect(result.dateLocale).toBe('nl')
    expect(result.displayTimezone).toBe('Europe/Amsterdam')
  })

  it('drops a non-string date_locale or display_timezone without throwing, logging each drop (bug fix: N4/C7)', () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
    const table = baseTable({ date_locale: 123 as any, display_timezone: {} as any })
    let result: ReturnType<typeof parseTable> | undefined
    expect(() => { result = parseTable(table, 'en') }).not.toThrow()
    expect(result!.dateLocale).toBeUndefined()
    expect(result!.displayTimezone).toBeUndefined()
    // One log per dropped declaration, consistent with date_columns' existing policy.
    expect(errorSpy).toHaveBeenCalledTimes(2)
    errorSpy.mockRestore()
  })

  it('a table with no date declarations at all parses cleanly (no log, all undefined)', () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {})
    const result = parseTable(baseTable(), 'en')
    expect(result.dateColumns).toBeUndefined()
    expect(result.dateLocale).toBeUndefined()
    expect(result.displayTimezone).toBeUndefined()
    expect(errorSpy).not.toHaveBeenCalled()
    errorSpy.mockRestore()
  })
})

describe('parseTable data_frame shape validation (ts-idiom-merged T18/B3)', () => {
  it('accepts a data_frame already shaped as columns-of-rows, as an object', () => {
    const table = baseTable({ data_frame: { col_a: { '0': 'value', '1': 'other' } } })
    const result = parseTable(table, 'en')
    expect(result.head.cells).toEqual(['col_a'])
    expect(result.body.rows).toEqual([{ id: '0', cells: ['value'] }, { id: '1', cells: ['other'] }])
  })

  it('accepts a data_frame given as a JSON string, parsing it once', () => {
    const table = baseTable({ data_frame: JSON.stringify({ col_a: { '0': 'value' } }) })
    const result = parseTable(table, 'en')
    expect(result.head.cells).toEqual(['col_a'])
  })

  // A malformed data_frame is not recoverable the way an invalid date_columns/date_locale
  // declaration is: rendering an empty table instead would silently donate less data than the
  // participant saw (ADR-0031). The old code already failed loudly (crashed on Object.keys); this
  // throws the same way, with a descriptive message, instead of trading the crash for silent
  // data loss (ts-fix2-review check 2 / item 3).
  it('throws a descriptive error for a null data_frame, rather than rendering an empty table', () => {
    const table = baseTable({ data_frame: null as any })
    expect(() => parseTable(table, 'en')).toThrow(/malformed data_frame/)
  })

  it('throws a descriptive error for an array data_frame, rather than rendering an empty table', () => {
    const table = baseTable({ data_frame: [] as any })
    expect(() => parseTable(table, 'en')).toThrow(/malformed data_frame/)
  })

  it('throws a descriptive error for a data_frame that is not an object at all', () => {
    const table = baseTable({ data_frame: 42 as any })
    expect(() => parseTable(table, 'en')).toThrow(/malformed data_frame/)
  })

  it('throws a descriptive error when a column value is not itself an object', () => {
    const table = baseTable({ data_frame: { col_a: 'value' } as any })
    expect(() => parseTable(table, 'en')).toThrow(/malformed data_frame/)
  })

  it('throws a descriptive error for a data_frame string that is not valid JSON', () => {
    const table = baseTable({ data_frame: 'not json' })
    expect(() => parseTable(table, 'en')).toThrow(/not valid JSON/)
  })

  it('a valid data_frame still passes through unaffected by the guard', () => {
    const table = baseTable({ data_frame: { col_a: { '0': 'value' } } })
    expect(() => parseTable(table, 'en')).not.toThrow()
    expect(parseTable(table, 'en').head.cells).toEqual(['col_a'])
  })
})
