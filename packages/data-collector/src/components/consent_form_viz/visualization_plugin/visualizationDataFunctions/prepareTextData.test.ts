import { prepareTextData } from './prepareTextData'
import { Table, TextVisualization } from '../types'

function makeTable (rows: Array<[string, string]>): Table {
  return {
    id: 't1',
    head: { cells: ['text', 'weight'] },
    body: {
      rows: rows.map(([text, weight], i) => ({ id: String(i), cells: [text, weight] }))
    }
  }
}

// Pins the fix for the no-constant-binary-expression lint finding at
// prepareTextData.ts:47: `Number(values[i]) ?? 1` never falls back to 1,
// because Number() never returns null/undefined (only NaN for unparsable
// input). Combined with the `if (!isNaN(v))` guard just below, a non-numeric
// weight silently contributed 0 instead of the intended fallback weight 1.
describe('prepareTextData non-numeric value handling', () => {
  it('falls back to a weight of 1 for a non-numeric value cell', async () => {
    const table = makeTable([
      ['alpha', '3'],
      ['beta', 'not-a-number']
    ])
    const visualization: TextVisualization = {
      title: {},
      type: 'wordcloud',
      textColumn: 'text',
      valueColumn: 'weight'
    }

    const result = await prepareTextData(table, visualization)
    const alpha = result.topTerms.find((t) => t.text === 'alpha')
    const beta = result.topTerms.find((t) => t.text === 'beta')

    expect(alpha?.value).toBe(3)
    expect(beta?.value).toBe(1)
  })
})
