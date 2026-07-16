import { ChartVisualization, TextVisualization, VisualizationType, Table } from '../types'

/**
 * Project a table down to only the columns the visualization reads, so that
 * postMessage structured-clones a fraction of the table into the worker
 * (issue #122). The worker resolves columns by name (getTableColumn), so the
 * projection is transparent to it. Display and donation data are unaffected.
 */
export function selectVisualizationColumns (table: Table, visualization: VisualizationType): Table {
  const columns = visualizationColumns(visualization).filter((column) => table.head.cells.includes(column))
  const indices = columns.map((column) => table.head.cells.indexOf(column))
  return {
    id: table.id,
    head: { cells: columns },
    body: {
      rows: table.body.rows.map((row) => ({
        id: row.id,
        cells: indices.map((index) => row.cells[index])
      }))
    }
  }
}

function visualizationColumns (visualization: VisualizationType): string[] {
  const columns = new Set<string>()

  if (['line', 'bar', 'area'].includes(visualization.type)) {
    const chart = visualization as ChartVisualization
    columns.add(chart.group.column)
    for (const value of chart.values) {
      if (value.column !== undefined) columns.add(value.column)
      if (value.group_by !== undefined) columns.add(value.group_by)
      if (value.z !== undefined) columns.add(value.z)
    }
  }

  if (visualization.type === 'wordcloud') {
    const text = visualization as TextVisualization
    columns.add(text.textColumn)
    if (text.valueColumn !== undefined) columns.add(text.valueColumn)
  }

  return Array.from(columns)
}
