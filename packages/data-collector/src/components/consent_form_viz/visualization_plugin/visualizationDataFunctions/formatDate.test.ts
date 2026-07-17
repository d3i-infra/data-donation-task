import { formatDate } from './util'
import { DateFormat } from '../types'

function makeIsoDates (count: number): string[] {
  // spread ~1000 dates across a few months, at varying hours, so month/weekday/hour
  // formatting all see a range of distinct values
  const dates: string[] = []
  const start = new Date('2024-01-01T00:00:00.000Z').getTime()
  const hourMs = 1000 * 60 * 60
  for (let i = 0; i < count; i++) {
    dates.push(new Date(start + i * hourMs * 7).toISOString())
  }
  return dates
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
      const dates = makeIsoDates(1000)
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
    const dates = ['2020-03-01T00:00:00.000Z', '2021-06-15T00:00:00.000Z']
    const [formatted, sortable] = formatDate(dates, 'year')
    const expected = dates.map((d) => new Date(d).getFullYear().toString())
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })

  it('formats "quarter"', () => {
    const dates = ['2020-01-15T00:00:00.000Z', '2020-05-15T00:00:00.000Z', '2020-11-15T00:00:00.000Z']
    const [formatted, sortable] = formatDate(dates, 'quarter')
    const expected = dates.map((d) => {
      const date = new Date(d)
      const year = date.getFullYear().toString()
      const quarter = Math.floor(date.getMonth() / 3) + 1
      return `${year}-Q${quarter}`
    })
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })

  it('formats "month"', () => {
    const dates = ['2020-01-15T00:00:00.000Z', '2020-07-04T00:00:00.000Z']
    const [formatted, sortable] = formatDate(dates, 'month')
    const monthFormatter = new Intl.DateTimeFormat('default', { month: 'short' })
    const expected = dates.map((d) => {
      const date = new Date(d)
      const year = date.getFullYear().toString()
      const month = monthFormatter.format(date)
      return `${year}-${month}`
    })
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })

  it('formats "day"', () => {
    const dates = ['2020-01-15T00:00:00.000Z', '2020-07-04T00:00:00.000Z']
    const [formatted, sortable] = formatDate(dates, 'day')
    const monthFormatter = new Intl.DateTimeFormat('default', { month: 'short' })
    const expected = dates.map((d) => {
      const date = new Date(d)
      const year = date.getFullYear().toString()
      const month = monthFormatter.format(date)
      const day = date.getDate().toString()
      return `${year}-${month}-${day}`
    })
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })

  it('formats "hour"', () => {
    const dates = ['2020-01-15T08:00:00.000Z', '2020-07-04T23:00:00.000Z']
    const [formatted, sortable] = formatDate(dates, 'hour')
    const monthFormatter = new Intl.DateTimeFormat('default', { month: 'short' })
    const expected = dates.map((d) => {
      const date = new Date(d)
      const year = date.getFullYear().toString()
      const month = monthFormatter.format(date)
      const day = date.getDate().toString()
      const hour = date.getHours()
      return `${year}-${month}-${day} ${hour}:00`
    })
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })

  it('formats "month_cycle"', () => {
    const dates = ['2020-01-15T00:00:00.000Z', '2020-07-04T00:00:00.000Z', '2020-12-25T00:00:00.000Z']
    const [formatted, sortable] = formatDate(dates, 'month_cycle')
    const intlFormatter = new Intl.DateTimeFormat('default', { month: 'long' })
    const expected = dates.map((d) => intlFormatter.format(new Date(d)))
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })

  it('formats "weekday_cycle"', () => {
    const dates = ['2023-11-06T00:00:00.000Z', '2023-11-09T00:00:00.000Z', '2023-11-12T00:00:00.000Z']
    const [formatted, sortable] = formatDate(dates, 'weekday_cycle')
    const intlFormatter = new Intl.DateTimeFormat('default', { weekday: 'long' })
    const expected = dates.map((d) => intlFormatter.format(new Date(d)))
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })

  it('formats "hour_cycle"', () => {
    const dates = ['2020-01-15T08:00:00.000Z', '2020-01-15T23:00:00.000Z']
    const [formatted, sortable] = formatDate(dates, 'hour_cycle')
    const intlFormatter = new Intl.DateTimeFormat('default', { hour: 'numeric', hour12: false })
    const expected = dates.map((d) => intlFormatter.format(new Date(d)))
    expect(formatted).toEqual(expected)
    expect(sortable).not.toBeNull()
  })
})
