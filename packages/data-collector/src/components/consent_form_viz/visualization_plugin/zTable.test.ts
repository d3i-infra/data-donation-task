import { zTable, zTableRow, isRowArray } from './types'

function makeTable () {
  return {
    id: 't1',
    head: { cells: ['a', 'b'] },
    body: {
      rows: [
        { id: 'r1', cells: ['1', '2'] },
        { id: 'r2', cells: ['3', '4'] }
      ]
    },
    originalBody: { rows: [] },
    deletedRows: []
  }
}

describe('zTable', () => {
  it('passes rows through by reference (no deep clone)', () => {
    const input = makeTable()
    const result = zTable.safeParse(input)
    expect(result.success).toBe(true)
    if (result.success) {
      expect(result.data.body.rows).toBe(input.body.rows)
      expect(result.data.body.rows[0]).toBe(input.body.rows[0])
    }
  })

  it('strips unknown keys from the outer object', () => {
    const result = zTable.safeParse(makeTable())
    expect(result.success).toBe(true)
    if (result.success) {
      expect(result.data).not.toHaveProperty('originalBody')
      expect(result.data).not.toHaveProperty('deletedRows')
    }
  })

  it('rejects a non-string row id', () => {
    const input = makeTable()
    ;(input.body.rows[1] as any).id = 7
    expect(zTable.safeParse(input).success).toBe(false)
  })

  it('rejects non-array cells', () => {
    const input = makeTable()
    ;(input.body.rows[0] as any).cells = 'not-an-array'
    expect(zTable.safeParse(input).success).toBe(false)
  })

  it('rejects a non-string cell', () => {
    const input = makeTable()
    ;(input.body.rows[0] as any).cells = ['1', 2]
    expect(zTable.safeParse(input).success).toBe(false)
  })

  it('rejects non-array rows', () => {
    const input = makeTable()
    ;(input.body as any).rows = { r1: {} }
    expect(zTable.safeParse(input).success).toBe(false)
  })

  it('isRowArray conforms to zTableRow for representative rows', () => {
    const cases: unknown[] = [
      [{ id: 'r1', cells: ['a', 'b'] }],
      [{ id: 'r1', cells: [] }],
      [],
      [{ id: 7, cells: ['a'] }],
      [{ id: 'r1', cells: 'nope' }],
      [{ id: 'r1', cells: ['a', 2] }],
      [null],
      'not-an-array'
    ]
    for (const rows of cases) {
      expect(isRowArray(rows)).toBe(zTableRow.array().safeParse(rows).success)
    }
  })
})
