import { prepareChartData } from './prepareChartData'
import { Table, ChartVisualization, DateColumnSpec, DateEncoding, zDateColumnSpec } from '../types'

function makeTable (rows: Array<[string, string]>): Table {
  return {
    id: 't1',
    head: { cells: ['group', 'val'] },
    body: {
      rows: rows.map(([group, val], i) => ({ id: String(i), cells: [group, val] }))
    }
  }
}

// `DateColumnSpec.encoding` is zod output: a non-empty tuple the `.transform` builds once, at
// the schema boundary (visualization_plugin/types.ts). This test builds a `Table` directly (no
// zod parse in the row loop, per ADR-0035), so it calls the real schema (`zDateColumnSpec.parse`)
// for the spec, rather than reimplementing its normalisation by hand (ts-fix2-review check 1).
function makeDateTable (
  cells: string[],
  spec: { encoding: DateEncoding | DateEncoding[], utcOffsetMinutes?: number },
  extra: Partial<Table> = {}
): Table {
  const normalizedSpec: DateColumnSpec = zDateColumnSpec.parse(spec)
  return {
    id: 't2',
    head: { cells: ['Date', 'val'] },
    body: { rows: cells.map((c, i) => ({ id: String(i), cells: [c, '1'] })) },
    dateColumns: { Date: normalizedSpec },
    ...extra
  }
}
const byDay: ChartVisualization = { title: {}, type: 'bar', group: { column: 'Date', dateFormat: 'day' }, values: [{ column: 'val', aggregate: 'sum' }] }

// Pins the fix for the no-constant-binary-expression lint findings at
// prepareChartData.ts:120 and :138: `Number(yValue) ?? 0` never falls back,
// because Number() never returns null/undefined (only NaN for unparsable
// input) -- so a non-numeric cell used to poison the running sum with NaN
// instead of being treated as 0.
describe('prepareChartData non-numeric value handling', () => {
  it('treats a non-numeric cell as 0 for a sum aggregation instead of poisoning the group with NaN', async () => {
    const table = makeTable([
      ['a', '10'],
      ['a', 'not-a-number'],
      ['b', '5']
    ])
    const visualization: ChartVisualization = {
      title: {},
      type: 'bar',
      group: { column: 'group' },
      values: [{ column: 'val', aggregate: 'sum' }]
    }

    const result = await prepareChartData(table, visualization)
    const groupA = result.data.find((d) => d.group === 'a')
    const groupB = result.data.find((d) => d.group === 'b')

    expect(groupA?.val).toBe(10)
    expect(groupB?.val).toBe(5)
  })

  it('treats a non-numeric cell as 0 in the pct aggregation denominator instead of poisoning every percentage with NaN', async () => {
    const table = makeTable([
      ['a', '10'],
      ['a', 'not-a-number'],
      ['b', '5']
    ])
    const visualization: ChartVisualization = {
      title: {},
      type: 'bar',
      group: { column: 'group' },
      values: [{ column: 'val', aggregate: 'pct' }]
    }

    const result = await prepareChartData(table, visualization)
    const groupA = result.data.find((d) => d.group === 'a')
    const groupB = result.data.find((d) => d.group === 'b')

    expect(Number.isFinite(groupA?.val)).toBe(true)
    expect(Number.isFinite(groupB?.val)).toBe(true)
    // createVisualizationData rounds values to 2 decimals.
    expect(groupA?.val).toBeCloseTo((100 * 10) / 15, 1)
    expect(groupB?.val).toBeCloseTo((100 * 5) / 15, 1)
  })
})

describe('date grouping through interpretTimestamp (ADR-0043)', () => {
  it('epoch seconds bucket in the display zone', async () => {
    // 23:30Z on 15 June is 01:30 on 16 June in Amsterdam
    const table = makeDateTable([String(Date.UTC(2026, 5, 15, 23, 30) / 1000)], { encoding: 'epoch-seconds' }, { displayTimezone: 'Europe/Amsterdam' })
    const result = await prepareChartData(table, byDay)
    expect(result.data[0].Date).toMatch(/16$/)
    expect(result.unplotted).toBe(0)
  })
  it('a local wall clock buckets by its own clock regardless of zone', async () => {
    const table = makeDateTable(['2026-06-15 23:30:00'], { encoding: 'iso-8601' }, { displayTimezone: 'Pacific/Auckland' })
    const result = await prepareChartData(table, byDay)
    expect(result.data[0].Date).toMatch(/15$/)
  })
  it('rows that cannot be placed are counted and excluded, not thrown', async () => {
    const table = makeDateTable([String(Date.UTC(2026, 5, 15) / 1000), 'garbage', ''], { encoding: 'epoch-seconds' })
    const result = await prepareChartData(table, byDay)
    expect(result.unplotted).toBe(1)          // 'garbage'; the empty cell is empty, not an error
    expect(result.data.length).toBe(1)
  })
  it('an invalid display timezone falls back to the default instead of throwing (ts-idiom-merged §5 items 1/2)', async () => {
    // Same wall clock / expected day as the valid-Amsterdam-zone test above: an unrecognised
    // zone must fall back to resolveDisplayTimezone's default, not throw inside the worker.
    const table = makeDateTable([String(Date.UTC(2026, 5, 15, 23, 30) / 1000)], { encoding: 'epoch-seconds' }, { displayTimezone: 'Not/AZone' })
    const result = await prepareChartData(table, byDay)
    expect(result.data[0].Date).toMatch(/16$/)
    expect(result.unplotted).toBe(0)
  })
  it('a date column with no declared encoding is unplotted entirely, never parsed by guess', async () => {
    const table: Table = {
      id: 't3',
      head: { cells: ['Date', 'val'] },
      body: {
        rows: [
          { id: '0', cells: ['2026-06-15T00:00:00Z', '1'] },
          { id: '1', cells: ['', '1'] }
        ]
      }
    }
    const result = await prepareChartData(table, byDay)
    expect(result.unplotted).toBe(1)          // the ISO string; the empty cell is empty, not unplotted
    expect(result.data.length).toBe(0)
  })
})
