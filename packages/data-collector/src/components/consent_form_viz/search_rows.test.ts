import { searchRows } from './search_rows'
import { buildCellDisplay } from './cell_display'
import { zDateColumnSpec } from './visualization_plugin/types'

const table = {
  head: { cells: ['Title', 'Date'] },
  dateColumns: { Date: zDateColumnSpec.parse({ encoding: 'epoch-seconds' }) }
}
const rows = [
  { id: '0', cells: ['first', '1781548241'] },
  { id: '1', cells: ['second', ''] },
  { id: '2', cells: ['third', 'not a date'] }
]
const display = buildCellDisplay(table, 'UTC', 'en-US')

describe('searchRows', () => {
  it('matches the date the participant sees', () => {
    expect(searchRows(rows, '2026', display)).toEqual(new Set(['0']))
    expect(searchRows(rows, 'jun 15', display)).toEqual(new Set(['0']))
  })
  it('still matches the raw value', () => {
    expect(searchRows(rows, '1781548241', display)).toEqual(new Set(['0']))
    expect(searchRows(rows, 'not a date', display)).toEqual(new Set(['2']))
  })
  it('matches columns that have no date declaration', () => {
    expect(searchRows(rows, 'second', display)).toEqual(new Set(['1']))
  })
  it('an empty search filters nothing', () => {
    expect(searchRows(rows, '  ', display)).toBeUndefined()
  })
})
