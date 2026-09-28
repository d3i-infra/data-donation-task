// Matching feldspar's Text/Translatable (duplicated like visualization_plugin/types.ts
// to keep the plugin self-contained). Previously `Text` silently resolved to the DOM
// global Text node type.
export interface Translatable {
  translations: { [locale: string]: string }
}
export type Text = Translatable | string

// The zod-inferred DateColumnSpec in visualization_plugin/types.ts is the single source of
// truth (ADR-0043); re-exported here so consent_form_viz has no second copy of the shape.
// (DateEncoding was re-exported too but had no consumer here — final review M6.)
export type { DateColumnSpec } from "./visualization_plugin/types"
import type { DateColumnSpec } from "./visualization_plugin/types"
import type { z } from "zod"
import type { zDateColumnSpec } from "./visualization_plugin/types"

// The wire shape of `pandas.DataFrame.to_json()` with its default orient, "columns"
// (`d3i_props.py`'s `translate_data_frame`): outer keys are column names, inner keys are
// stringified row indices ("0", "1", ...), and values are the export's own JSON-native cell
// types (ADR-0042 — this only names pandas' shape, it doesn't re-type a cell). `parse_table.ts`
// checks a frame against this shape once, at the top level only (ts-idiom-merged T18/B3).
export type PandasColumnsFrame = Readonly<Record<string, Readonly<Record<string, string | number | boolean | null>>>>

export interface PropsUIPromptConsentFormTableViz {
  __type__: "PropsUIPromptConsentFormTableViz"
  id: string
  title: Text
  description: Text
  data_frame: string | PandasColumnsFrame
  visualizations: any
  headers?: Record<string, Text>
  folded: boolean
  delete_option: boolean
  // The host's wire shape, not the validated one: a `DateColumnSpec`'s `encoding` is zod
  // *output* (an already-normalised non-empty array — see zDateColumnSpec's `.transform` in
  // visualization_plugin/types.ts), but the host sends the *input* shape (a single encoding
  // string, or an array). `z.input<>` names that honestly; `parse_table.ts`'s
  // `zDateColumns.safeParse` is the only place the output type (`DateColumnSpec`) appears
  // (ts-fix2-review check 1 / T33).
  date_columns?: Record<string, z.input<typeof zDateColumnSpec>>
  date_locale?: string
  display_timezone?: string
}

export interface PropsUIPromptConsentFormViz {
  __type__: "PropsUIPromptConsentFormViz"
  description?: Text
  donateQuestion?: Text
  donateButton?: Text
  tables: PropsUIPromptConsentFormTableViz[]
}

export interface Annotation {
  row_id: string
  [key: string]: any
}

export interface TableContext {
  title: string
  description: string
  deletedRowCount: number
  annotations: Annotation[]
  originalBody: PropsUITableBody
  deletedRows: string[][]
  visualizations?: any[]
  headers?: Record<string, string>
  folded: boolean
  deleteOption: boolean
  dateColumns?: Record<string, DateColumnSpec>
  dateLocale?: string
  displayTimezone?: string
}

export type TableWithContext = TableContext & PropsUITable

export interface PropsUICheckBox {
  id: string
  selected: boolean
  size: string
  onSelect: () => void
}

// TABLE

export interface PropsUITable {
  __type__: "PropsUITable"
  id: string
  head: PropsUITableHead
  body: PropsUITableBody
  pageSize?: number
}

export interface PropsUITableHead {
  cells: string[]
}

export interface PropsUITableBody {
  rows: PropsUITableRow[]
}

// KW: removed __type__ for rows and cells, because it inflates the table memory size
export interface PropsUITableRow {
  id: string
  cells: string[]
}

export interface PropsUISearchBar {
  search: string
  onSearch: (search: string) => void
  placeholder?: string
  debounce?: number
}
