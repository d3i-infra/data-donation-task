import { resolveText } from "../../locale/text"
import { zTable } from "./visualization_plugin/types"
import {
  TableContext,
  PropsUITable,
  PropsUITableBody,
  PropsUITableHead,
  PropsUIPromptConsentFormTableViz,
  PropsUITableRow,
  PandasColumnsFrame,
} from "./types"

// Reuses zTable's own field schemas (rather than rebuilding `date_columns`/`date_locale`/
// `display_timezone` as separate schemas) so the table path validates them the same way the
// chart path's `zTable.safeParse` would (ADR-0043; ts-idiom-merged T25/C5), instead of trusting
// untyped JSON under a TS type nothing checks (final review S1). One parse per table (ADR-0035).
const zDateColumns = zTable.shape.dateColumns
const zDateLocale = zTable.shape.dateLocale
const zDisplayTimezone = zTable.shape.displayTimezone

/** Checks only the top-level shape of a decoded `data_frame` — is it an object whose values
 *  are themselves objects — in O(columns); it never inspects a row, so it stays cheap on a
 *  65k-row table (ADR-0035). A column's own malformed cell contents still surface through
 *  `rowCell`'s `String(...)` coercion (ADR-0042); this only rejects a frame that isn't shaped
 *  like `PandasColumnsFrame` at all (ts-idiom-merged T18/B3). No zod: the frame is the one
 *  boundary this module deliberately keeps off the per-row zod-parsing path (ADR-0035's check
 *  forbids `.safeParse`/`.parse` in the visualization pipeline; `parse_table.ts` itself already
 *  parses `date_columns` etc. once per table, but the row payload is large enough that even a
 *  single top-level zod pass is worth avoiding in favour of this direct check).
 */
function isDataFrameRecord(value: unknown): value is PandasColumnsFrame {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false
  return Object.values(value).every((column) => typeof column === "object" && column !== null && !Array.isArray(column))
}

function rowCell(dataFrame: PandasColumnsFrame, column: string, row: number): string {
  const text = String(dataFrame[column][`${row}`])
  return text
}

function columnNames(dataFrame: PandasColumnsFrame): string[] {
  return Object.keys(dataFrame)
}

function columnCount(dataFrame: PandasColumnsFrame): number {
  return columnNames(dataFrame).length
}

function rowCount(dataFrame: PandasColumnsFrame): number {
  if (columnCount(dataFrame) === 0) {
    return 0
  } else {
    const firstColumn = dataFrame[columnNames(dataFrame)[0]]
    return Object.keys(firstColumn).length - 1
  }
}

function rows(data: PandasColumnsFrame): PropsUITableRow[] {
  const result: PropsUITableRow[] = []
  const n = rowCount(data)
  for (let row = 0; row <= n; row++) {
    const id = `${row}`
    const cells = columnNames(data).map((column: string) => rowCell(data, column, row))
    result.push({ id, cells })
  }
  return result
}

function loadDataFrame(dataFrame: string | PandasColumnsFrame, tableId: string): PandasColumnsFrame {
  // Unlike an invalid date_columns/date_locale/display_timezone (dropped, logged, table still
  // renders), a malformed data_frame is not recoverable: silently rendering an empty table would
  // let a corrupted export donate less data than the participant saw, without either of them
  // knowing (ADR-0031 — memory/display work must never shrink the donated dataset, silently or
  // otherwise). The old code already failed loudly here, by crashing on the first `Object.keys`
  // call; this throws the same way, with a message, rather than trading a crash for a silent
  // data loss (ts-fix2-review check 2 / item 3).
  let parsed: unknown
  try {
    parsed = typeof dataFrame === "string" ? JSON.parse(dataFrame) : dataFrame
  } catch (cause) {
    throw new Error(`consent_form_viz: table "${tableId}" has a data_frame that is not valid JSON`, { cause })
  }
  if (!isDataFrameRecord(parsed)) {
    throw new Error(`consent_form_viz: table "${tableId}" has a malformed data_frame (expected an object of column -> {row: value})`)
  }
  return parsed
}

export function parseTables(tablesData: PropsUIPromptConsentFormTableViz[], locale: string): Array<PropsUITable & TableContext> {
  return tablesData.map((table) => parseTable(table, locale))
}

export function parseTable(tableData: PropsUIPromptConsentFormTableViz, locale: string): PropsUITable & TableContext {
  const id = tableData.id
  const title = resolveText(tableData.title, locale)
  const description =
    tableData.description !== undefined ? resolveText(tableData.description, locale) : ""
  const deletedRowCount = 0
  const dataFrame = loadDataFrame(tableData.data_frame, id)
  const headCells = columnNames(dataFrame).map((column: string) => column)
  const head: PropsUITableHead = {
    cells: headCells,
  }
  const body: PropsUITableBody = {
    rows: rows(dataFrame),
  }

  // Translate column headers if provided. The headers dict maps DataFrame
  // column names to Translatable objects. We resolve them to the current
  // locale for display, while head.cells retains the raw DataFrame column
  // names for visualization data lookups.
  let translatedHeaders: Record<string, string> | undefined
  if (tableData.headers != null) {
    translatedHeaders = {}
    for (const [column, text] of Object.entries(tableData.headers)) {
      translatedHeaders[column] = resolveText(text, locale)
    }
  }

  const dateColumnsResult = zDateColumns.safeParse(tableData.date_columns)
  if (!dateColumnsResult.success) {
    // Drop the declaration: the table renders raw text and the charts behave as for an
    // undeclared column (S1), rather than throwing during render (parseTable/PARSERS lookup
    // in table.tsx) or silently dropping every chart on that table (zTable.safeParse).
    console.error(`consent_form_viz: table "${id}" has an invalid date_columns declaration, ignoring it`, dateColumnsResult.error)
  }
  const dateColumns = dateColumnsResult.success ? dateColumnsResult.data : undefined

  const dateLocaleResult = zDateLocale.safeParse(tableData.date_locale)
  if (!dateLocaleResult.success) {
    // Consistent policy with date_columns above: log every dropped declaration, never drop one
    // silently (bug fix: N4/C7 — this used to be silent).
    console.error(`consent_form_viz: table "${id}" has an invalid date_locale declaration, ignoring it`, dateLocaleResult.error)
  }
  const dateLocale = dateLocaleResult.success ? dateLocaleResult.data : undefined

  const displayTimezoneResult = zDisplayTimezone.safeParse(tableData.display_timezone)
  if (!displayTimezoneResult.success) {
    console.error(`consent_form_viz: table "${id}" has an invalid display_timezone declaration, ignoring it`, displayTimezoneResult.error)
  }
  const displayTimezone = displayTimezoneResult.success ? displayTimezoneResult.data : undefined

  return {
    __type__: "PropsUITable",
    id,
    head,
    body,
    title,
    description,
    deletedRowCount,
    annotations: [],
    originalBody: body,
    deletedRows: [],
    visualizations: tableData.visualizations,
    headers: translatedHeaders,
    folded: tableData.folded || false,
    deleteOption: tableData.delete_option,
    dateColumns,
    dateLocale,
    displayTimezone,
  }
}
