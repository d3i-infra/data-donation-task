import { formatDate } from './util'
import { DateFormat } from '../types'

function makeWallClocks (count: number): number[] {
  // spread ~1000 wall-clock instants across a few months, at varying hours, so
  // month/weekday/hour formatting all see a range of distinct values
  const start = Date.UTC(2024, 0, 1)
  const hourMs = 1000 * 60 * 60
  return Array.from({ length: count }, (_, i) => start + i * hourMs * 7)
}

describe('formatDate construction-count regression (memory tripwire)', () => {
  const cyclicFormats: DateFormat[] = [
    'month_cycle',
    'weekday_cycle',
    'hour_cycle',
    'month',
    'day',
    'hour'
  ]

  cyclicFormats.forEach((format) => {
    it(`does not construct one Intl.DateTimeFormat per row for format="${format}"`, () => {
      const dates = makeWallClocks(1000)
      const spy = jest.spyOn(Intl, 'DateTimeFormat')
      try {
        formatDate(dates, format)
        expect(spy.mock.calls.length).toBeLessThan(10)
      } finally {
        spy.mockRestore()
      }
    })
  })
})

describe('formatDate output equivalence', () => {
  it('formats "year"', () => {
    const dates = [Date.UTC(2020, 2, 1), Date.UTC(2021, 5, 15)]
    const [formatted, sortable] = formatDate(dates, 'year')
    const expected = dates.map((d) => new Date(d).getUTCFullYear().toString())
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })

  it('formats "quarter"', () => {
    const dates = [Date.UTC(2020, 0, 15), Date.UTC(2020, 4, 15), Date.UTC(2020, 10, 15)]
    const [formatted, sortable] = formatDate(dates, 'quarter')
    const expected = dates.map((d) => {
      const date = new Date(d)
      const year = date.getUTCFullYear().toString()
      const quarter = Math.floor(date.getUTCMonth() / 3) + 1
      return `${year}-Q${quarter}`
    })
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })

  it('formats "month"', () => {
    const dates = [Date.UTC(2020, 0, 15), Date.UTC(2020, 6, 4)]
    const [formatted, sortable] = formatDate(dates, 'month')
    const monthFormatter = new Intl.DateTimeFormat('default', { month: 'short', timeZone: 'UTC' })
    const expected = dates.map((d) => {
      const date = new Date(d)
      const year = date.getUTCFullYear().toString()
      const month = monthFormatter.format(date)
      return `${year}-${month}`
    })
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })

  it('formats "day"', () => {
    const dates = [Date.UTC(2020, 0, 15), Date.UTC(2020, 6, 4)]
    const [formatted, sortable] = formatDate(dates, 'day')
    const monthFormatter = new Intl.DateTimeFormat('default', { month: 'short', timeZone: 'UTC' })
    const expected = dates.map((d) => {
      const date = new Date(d)
      const year = date.getUTCFullYear().toString()
      const month = monthFormatter.format(date)
      const day = date.getUTCDate().toString()
      return `${year}-${month}-${day}`
    })
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })

  it('formats "hour"', () => {
    const dates = [Date.UTC(2020, 0, 15, 8), Date.UTC(2020, 6, 4, 23)]
    const [formatted, sortable] = formatDate(dates, 'hour')
    const monthFormatter = new Intl.DateTimeFormat('default', { month: 'short', timeZone: 'UTC' })
    const expected = dates.map((d) => {
      const date = new Date(d)
      const year = date.getUTCFullYear().toString()
      const month = monthFormatter.format(date)
      const day = date.getUTCDate().toString()
      const hour = date.getUTCHours()
      return `${year}-${month}-${day} ${hour}:00`
    })
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })

  it('formats "month_cycle"', () => {
    const dates = [Date.UTC(2020, 0, 15), Date.UTC(2020, 6, 4), Date.UTC(2020, 11, 25)]
    const [formatted, sortable] = formatDate(dates, 'month_cycle')
    const intlFormatter = new Intl.DateTimeFormat('default', { month: 'long', timeZone: 'UTC' })
    const expected = dates.map((d) => intlFormatter.format(new Date(d)))
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })

  it('formats "weekday_cycle"', () => {
    const dates = [Date.UTC(2023, 10, 6), Date.UTC(2023, 10, 9), Date.UTC(2023, 10, 12)]
    const [formatted, sortable] = formatDate(dates, 'weekday_cycle')
    const intlFormatter = new Intl.DateTimeFormat('default', { weekday: 'long', timeZone: 'UTC' })
    const expected = dates.map((d) => intlFormatter.format(new Date(d)))
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })

  it('formats "hour_cycle"', () => {
    const dates = [Date.UTC(2020, 0, 15, 8), Date.UTC(2020, 0, 15, 23)]
    const [formatted, sortable] = formatDate(dates, 'hour_cycle')
    const intlFormatter = new Intl.DateTimeFormat('default', { hour: 'numeric', hour12: false, timeZone: 'UTC' })
    const expected = dates.map((d) => intlFormatter.format(new Date(d)))
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })
})

describe('formatDate on wall-clock milliseconds (ADR-0043)', () => {
  it('formats by the wall clock it is given, not the runner\'s zone', () => {
    const [out] = formatDate([Date.UTC(2026, 5, 15, 23, 30, 0)], 'hour')
    expect(out[0]).toMatch(/2026-.*-15 23:00$/)
  })
  it('a null entry formats to an empty string and does not stretch the domain', () => {
    const [out, sortable] = formatDate([Date.UTC(2026, 5, 15), null, Date.UTC(2026, 5, 16)], 'day')
    expect(out[1]).toBe('')
    expect(Object.keys(sortable ?? {}).length).toBeLessThanOrEqual(2)
  })
})
