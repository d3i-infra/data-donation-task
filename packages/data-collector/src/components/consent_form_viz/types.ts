export interface PropsUIPromptConsentFormTableViz {
  __type__: "PropsUIPromptConsentFormTableViz"
  id: string
  title: Text
  description: Text
  data_frame: any
  visualizations: any
  folded: boolean
  delete_option: boolean
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
  folded: boolean
  deleteOption: boolean
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
