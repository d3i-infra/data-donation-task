import { translate } from '../translate'
import { AxisSettings, TickerFormat, ChartVisualizationData } from '../types'

import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  BarChart,
  Bar,
  AreaChart,
  Area,
  Label
} from 'recharts'

interface Props {
  visualizationData: ChartVisualizationData
  locale: string
}

const margin = { top: 5, right: 5, left: 5, bottom: 15 }

export default function RechartsGraph ({ visualizationData, locale }: Props): JSX.Element | null {
  const xLabel = translate(visualizationData.xLabel ?? visualizationData.xKey, locale)
  const tickFormatter = getTickFormatter(Object.values(visualizationData.yKeys))

  function tooltip (): JSX.Element {
    return (
      <Tooltip
        allowEscapeViewBox={{ x: false, y: false }}
        labelStyle={{ marginBottom: '0.5rem' }}
        formatter={tickFormatter}
        labelFormatter={(value: string) => `${xLabel}: ${value}`}
        contentStyle={{
          fontSize: '0.8rem',
          lineHeight: '0.8rem',
          background: '#fff8',
          backdropFilter: 'blur(3px)'
        }}
      />
    )
  }

  function axes (minTickGap: number): JSX.Element | null {
    const hasVisualizationData = Boolean(visualizationData)
    if (!hasVisualizationData) return null

    return (
      <>
        <XAxis dataKey={visualizationData.xKey} minTickGap={minTickGap} fontSize={12}>
          <Label className=' font-bold text-sm' value={xLabel} offset={-6} position='insideBottom' />
        </XAxis>
        <YAxis yAxisId='left' tickFormatter={tickFormatter} fontSize={12} />
      </>
    )
  }

  function legend (): JSX.Element {
    return (
      <Legend
        margin={{ left: 10 }}
        align='right'
        verticalAlign='top'
        iconType='plainline'
        wrapperStyle={{ fontSize: '0.8rem' }}
      />
    )
  }

  let chart: JSX.Element | null = null

  function getYLabel (yKey: AxisSettings): string {
    return translate(yKey.label ?? yKey.id, locale)
  }

  if (visualizationData.type === 'line') {
    chart = (
      <LineChart data={visualizationData.data} margin={margin}>
        {axes(20)}
        {tooltip()}
        {legend()}
        {Object.values(visualizationData.yKeys).map((yKey: AxisSettings, i: number) => {
          const { color, dash } = getLineStyle(i)
          return (
            <Line
              key={yKey.id}
              yAxisId='left'
              type='monotone'
              name={getYLabel(yKey)}
              dataKey={yKey.id}
              dot={false}
              strokeWidth={2}
              stroke={color}
              strokeDasharray={dash}
            />
          )
        })}
      </LineChart>
    )
  }

  if (visualizationData.type === 'bar') {
    chart = (
      <BarChart data={visualizationData.data} margin={margin}>
        {axes(0)}
        {tooltip()}
        {legend()}
        {Object.values(visualizationData.yKeys).map((yKey: AxisSettings, i: number) => {
          const { color } = getLineStyle(i)
          return <Bar key={yKey.id} yAxisId='left' dataKey={yKey.id} name={getYLabel(yKey)} fill={color} />
        })}
      </BarChart>
    )
  }

  if (visualizationData.type === 'area') {
    chart = (
      <AreaChart data={visualizationData.data} margin={margin}>
        {axes(20)}
        {tooltip()}
        {legend()}
        {Object.values(visualizationData.yKeys).map((yKey: AxisSettings, i: number) => {
          const { color } = getLineStyle(i)
          return (
            <Area key={yKey.id} yAxisId='left' dataKey={yKey.id} name={getYLabel(yKey)} fill={color} type='monotone' />
          )
        })}
      </AreaChart>
    )
  }

  if (chart == null) return null
  return (
    <ResponsiveContainer width='100%' height='100%'>
      {chart}
    </ResponsiveContainer>
  )
}

function getLineStyle (index: number): { color: string, dash: string } {
  const COLORS = ['#4272EF', '#FF5E5E', '#FFCF60', '#1E3FCC', '#CC3F3F', '#CC9F3F']
  const DASHES = ['1', '5 5', '10 10', '5 5 10 10']

  const cell = index % (COLORS.length * DASHES.length)
  const row = index % COLORS.length
  const column = Math.floor(cell / COLORS.length)

  return { color: COLORS[row], dash: DASHES[column] }
}

function getTickFormatter (yKeys: AxisSettings[]): ((value: number) => string) | undefined {
  let tickerFormat: TickerFormat | undefined

  for (const yKey of yKeys) {
    if (tickerFormat === undefined) tickerFormat = yKey.tickerFormat
    if (tickerFormat !== yKey.tickerFormat) tickerFormat = 'default'
  }

  return tickFormatter(tickerFormat ?? 'default')
}

function tickFormatter (type: TickerFormat): undefined | ((value: number) => string) {
  if (type === 'percent') return (value: number) => `${value}%`
  return undefined
}
