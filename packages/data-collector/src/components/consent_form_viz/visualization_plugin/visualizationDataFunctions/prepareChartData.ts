import { formatDate, getTableColumn, resolveDisplayTimezone } from './util'
import { interpretTimestamp, wallClockInZone, isEmptyCell, Interpretation } from './interpretTimestamp'
import { Table, TickerFormat, ChartVisualizationData, ChartVisualization, AxisSettings } from '../types'

export async function prepareChartData (
  table: Table,
  visualization: ChartVisualization
): Promise<ChartVisualizationData> {
  if (table.body.rows.length === 0) return { type: visualization.type, xKey: '', xLabel: '', yKeys: {}, data: [], unplotted: 0 }

  const { aggregate, unplotted } = aggregateData(table, visualization)
  return createVisualizationData(table, visualization, aggregate, unplotted)
}

function createVisualizationData (
  table: Table,
  visualization: ChartVisualization,
  aggregate: Record<string, PrepareAggregatedData>,
  unplotted: number
): ChartVisualizationData {
  const visualizationData = initializeVisualizationData(table, visualization)
  visualizationData.unplotted = unplotted

  visualizationData.data = Object.values(aggregate)
    .sort((a: any, b: any) => (a.sortBy < b.sortBy ? -1 : b.sortBy < a.sortBy ? 1 : 0))
    .map((d) => {
      for (const key of Object.keys(d.values)) d.values[key] = Math.round(d.values[key] * 100) / 100

      return {
        ...d.values,
        [d.xKey]: d.xValue,
        __rowIds: d.rowIds,
        __sortBy: d.sortBy
      }
    })

  return visualizationData
}

function initializeVisualizationData (table: Table, visualization: ChartVisualization): ChartVisualizationData {
  const yKeys: Record<string, AxisSettings> = {}
  for (const value of visualization.values) {
    let tickerFormat: TickerFormat = 'default'
    if (value.aggregate === 'pct' || value.aggregate === 'count_pct') tickerFormat = 'percent'

    if (value.group_by === undefined) {
      const label = value.label !== undefined ? value.label : value.column
      yKeys[value.column] = { id: value.column, label, tickerFormat }
    } else {
      const uniqueValues = Array.from(new Set(getTableColumn(table, value.group_by)))
      for (const uniqueValue of uniqueValues) {
        const id = `${value.column}.GROUP_BY.${uniqueValue}`
        yKeys[id] = { id, label: uniqueValue, tickerFormat }
      }
    }
  }

  return {
    type: visualization.type,
    xKey: visualization.group.column,
    xLabel: visualization.group.label,
    yKeys,
    data: [],
    unplotted: 0
  }
}

function aggregateData (table: Table, visualization: ChartVisualization): { aggregate: Record<string, PrepareAggregatedData>, unplotted: number } {
  const aggregate: Record<string, PrepareAggregatedData> = {}

  const { groupBy, xSortable, unplotted, placed } = prepareX(table, visualization)
  const rowIds = table.body.rows.map((row) => row.id)
  const xKey = visualization.group.column

  const anyAddZeroes = visualization.values.some((value) => value.addZeroes === true)
  if (anyAddZeroes && xSortable != null) {
    for (const [uniqueValue, sortby] of Object.entries(xSortable)) {
      aggregate[uniqueValue] = {
        sortBy: sortby,
        rowIds: {},
        xKey,
        xValue: uniqueValue,
        values: {}
      }
    }
  }

  for (const value of visualization.values) {
    // loop over all y values

    const aggFun = value.aggregate !== undefined ? value.aggregate : 'count'

    const yValues = getTableColumn(table, value.column)
    if (yValues.length === 0) throw new Error(`Y column ${table.id}.${value.column} not found`)

    // If group_by column is specified, the columns in the aggregated data will be the unique group_by
    // column values. As suffix we use the value column, separated with .GROUP_BY.. This is used
    // so that we can relate the aggregated data back to the value specification
    let yGroup: null | string[] = null
    if (value.group_by !== undefined) { yGroup = getTableColumn(table, value.group_by).map((v) => `${value.column}.GROUP_BY.${v}`) }

    // if missing values should be treated as zero, we need to add the missing values after knowing all groups
    const addZeroes = value.addZeroes ?? false
    const groupSummary: Record<string, { n: number, sum: number }> = {}

    for (let i = 0; i < rowIds.length; i++) {
      // loop over rows of table
      if (placed[i] === false) continue

      const xValue = groupBy[i]

      if (visualization.group.range !== undefined) {
        if (Number(xValue) < visualization.group.range[0] || Number(xValue) > visualization.group.range[1]) {
          continue
        }
      }

      const yValue = yValues[i]
      // Number() never returns null/undefined (only NaN for unparsable input), so
      // `Number(yValue) ?? 0` never actually falls back -- a non-numeric cell poisons
      // the running sum with NaN. Guard against NaN explicitly instead.
      const numericYValue = Number(yValue)
      const yValueOrZero = Number.isNaN(numericYValue) ? 0 : numericYValue
      const group = yGroup != null ? yGroup[i] : value.column

      const sortBy = xSortable != null ? xSortable[xValue] : groupBy[i]

      // calculate group summary statistics. This is used for the mean, pct and count_pct aggregations
      if (groupSummary[group] === undefined) groupSummary[group] = { n: 0, sum: 0 }
      if (aggFun === 'count_pct' || aggFun === 'mean') groupSummary[group].n += 1
      if (aggFun === 'pct') groupSummary[group].sum += yValueOrZero

      if (aggregate[xValue] === undefined) {
        aggregate[xValue] = {
          sortBy: sortBy,
          rowIds: {},
          xKey,
          xValue: String(xValue),
          values: {}
        }
      }

      if (aggregate[xValue].rowIds[group] === undefined) aggregate[xValue].rowIds[group] = []
      aggregate[xValue].rowIds[group].push(rowIds[i])

      if (aggregate[xValue].values[group] === undefined) aggregate[xValue].values[group] = 0
      if (aggFun === 'count' || aggFun === 'count_pct') aggregate[xValue].values[group] += 1
      if (aggFun === 'sum' || aggFun === 'mean' || aggFun === 'pct') {
        aggregate[xValue].values[group] += yValueOrZero
      }
    }

    // use groupSummary to calculate the mean, pct and count_pct aggregations
    Object.keys(groupSummary).forEach((group) => {
      for (const xValue of Object.keys(aggregate)) {
        if (aggregate[xValue].values[group] === undefined) {
          if (addZeroes) aggregate[xValue].values[group] = 0
          else continue
        }
        if (aggFun === 'mean') {
          aggregate[xValue].values[group] = Number(aggregate[xValue].values[group]) / groupSummary[group].n
        }
        if (aggFun === 'count_pct') {
          aggregate[xValue].values[group] = (100 * Number(aggregate[xValue].values[group])) / groupSummary[group].n
        }
        if (aggFun === 'pct') {
          aggregate[xValue].values[group] = (100 * Number(aggregate[xValue].values[group])) / groupSummary[group].sum
        }
      }
    })
  }

  return { aggregate, unplotted }
}

function prepareX (
  table: Table,
  visualization: ChartVisualization
): { groupBy: string[], xSortable: Record<string, string | number> | null, unplotted: number, placed: boolean[] } {
  const column = visualization.group.column
  const raw = getTableColumn(table, column)
  if (raw.length === 0) {
    throw new Error(`X column ${table.id}.${column} not found`)
  }
  let groupBy: string[] = raw
  // let xSortable: Array<string | number> | null = null // separate variable allows using epoch time for sorting dates
  let xSortable: Record<string, string | number> | null = null // map x values to sortable values
  let unplotted = 0
  const placed = new Array<boolean>(raw.length).fill(true)

  // ADD CODE TO TRANSFORM TO DATE, BUT THEN ALSO KEEP AN INDEX BASED ON THE DATE ORDER
  if (visualization.group.dateFormat !== undefined) {
    // A column with no declared spec is never guessed at (ADR-0043): every non-empty row is
    // unplotted, handled explicitly here (empty cells still report `empty`, via the same check
    // interpretTimestamp itself uses) rather than by fabricating a `{ encoding: [] }` sentinel
    // spec the schema forbids (ts-idiom-merged T49).
    const spec = table.dateColumns?.[column]
    const zone = resolveDisplayTimezone(table.displayTimezone)
    const ctx = { locale: table.dateLocale }
    const wall: Array<number | null> = new Array(raw.length)
    for (let i = 0; i < raw.length; i++) {
      const r: Interpretation = spec !== undefined
        ? interpretTimestamp(raw[i], spec, ctx)
        : (isEmptyCell(raw[i]) ? { status: 'empty' } : { status: 'unsupported' })
      if (r.status === 'instant') {
        wall[i] = wallClockInZone(r.epochMs, zone)
      } else if (r.status === 'local') {
        wall[i] = r.wallClockMs
      } else {
        wall[i] = null
        placed[i] = false
        if (r.status !== 'empty') unplotted++
      }
    }
    ;[groupBy, xSortable] = formatDate(wall, visualization.group.dateFormat)
  }

  if (visualization.group.levels !== undefined) {
    xSortable = {}

    for (let i = 0; i < visualization.group.levels.length; i++) {
      const level = visualization.group.levels[i]
      xSortable[level] = i
    }
  }

  return { groupBy, xSortable, unplotted, placed }
}

export interface PrepareAggregatedData {
  xKey: string
  xValue: string
  values: Record<string, number>
  rowIds: Record<string, string[]>
  sortBy: number | string
  tickerFormat?: TickerFormat
}
