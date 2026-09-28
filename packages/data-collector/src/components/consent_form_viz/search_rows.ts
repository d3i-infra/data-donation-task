import { PropsUITableRow } from './types'
import { CellDisplay } from './cell_display'

/** Ids of the rows where a cell matches the search, in its raw value or in the text the
 *  participant sees for it; undefined for an empty search. */
export function searchRows (rows: PropsUITableRow[], search: string, cellDisplay: CellDisplay = []): Set<string> | undefined {
  if (search.trim() === '') return undefined

  // Not sure whether it's better to look for one of the words or exact string.
  // Now going for exact string. Note that if you change this, you should also change
  // the highlighting behavior in table.tsx (<Highlighter searchWords.../>)
  // const query = search.trim().split(/\s+/)
  const query = [search.trim()]

  const regexes: RegExp[] = []
  for (const q of query) {
    regexes.push(new RegExp(q.replace(/[-/\\^$*+?.()|[\]{}]/, '\\$&'), 'i'))
  }

  const ids = new Set<string>()
  for (const row of rows) {
    for (const regex of regexes) {
      let anyCellMatches = false
      for (let j = 0; j < row.cells.length; j++) {
        const cell = row.cells[j]
        if (regex.test(cell) || regex.test(cellDisplay[j]?.(cell) ?? '')) {
          anyCellMatches = true
          break
        }
      }
      if (anyCellMatches) ids.add(row.id)
    }
  }

  return ids
}
