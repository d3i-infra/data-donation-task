import { prepareChartData } from './prepareChartData'
import { Table, ChartVisualization } from '../types'

function makeTable (rows: Array<[string, string]>): Table {
  return {
    id: 't1',
    head: { cells: ['group', 'val'] },
    body: {
      rows: rows.map(([group, val], i) => ({ id: String(i), cells: [group, val] }))
    }
  }
}

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
