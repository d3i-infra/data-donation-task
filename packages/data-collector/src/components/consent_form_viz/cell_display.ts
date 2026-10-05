import { TableWithContext } from './types'
import { displayTimestamp } from './visualization_plugin/visualizationDataFunctions/util'
import { interpretTimestamp } from './visualization_plugin/visualizationDataFunctions/interpretTimestamp'

/** Per column, the function that turns a raw cell into its displayed text, or undefined for a
 *  column with no declared date encoding. A function returns null when the raw cell is shown
 *  as it is. */
export type CellDisplay = Array<((raw: string) => string | null) | undefined>

export function buildCellDisplay (
  table: Pick<TableWithContext, 'head' | 'dateColumns' | 'dateLocale'>,
  zone: string,
  locale: string
): CellDisplay {
  const ctx = { locale: table.dateLocale }
  return table.head.cells.map((column) => {
    const spec = table.dateColumns?.[column]
    if (spec === undefined) return undefined
    return (raw: string) => displayTimestamp(interpretTimestamp(raw, spec, ctx), zone, locale)
  })
}
