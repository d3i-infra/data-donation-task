import { VisualizationData, ChartVisualizationData, TextVisualizationData, Table, zVisualizationType } from './types'
import { memo, useEffect, useMemo, useState, ReactElement } from 'react'

import useVisualizationData from './visualizationDataFunctions/useVisualizationData'

import RechartsGraph from './figures/recharts_graph'
import VisxWordcloud from './figures/d3_wordcloud'
import { zoomInIcon, zoomOutIcon } from './zoom_icons'
import { z } from 'zod'
import { Loader } from './ui/loader'
import { resolveFlatText } from '../../../locale/text'

const doubleTypes = ['wordcloud']
type ShowStatus = 'hidden' | 'visible' | 'double'

export interface FigureProps {
  tableInput: Table
  visualizationInput: any
  locale: string
  handleDelete: (rowIds: string[]) => void
  handleUndo: () => void
}

export const Figure = ({
  tableInput,
  visualizationInput,
  locale,
  handleDelete,
  handleUndo
}: FigureProps): ReactElement => {
  const visualizationValidator = useMemo(() => zVisualizationType.safeParse(visualizationInput), [visualizationInput])

  if (!visualizationValidator.success) {
    console.error(visualizationValidator.error)
    return <div />
  }

  return (
    <FigureComponent
      table={tableInput}
      visualization={visualizationValidator.data}
      locale={locale}
      handleDelete={handleDelete}
      handleUndo={handleUndo}
    />
  )
}

export interface ValidatedFigureProps {
  table: Table
  visualization: z.infer<typeof zVisualizationType>
  locale: string
  handleDelete: (rowIds: string[]) => void
  handleUndo: () => void
}

export const FigureComponent = ({
  table,
  visualization,
  locale
}: ValidatedFigureProps): ReactElement => {
  const [visualizationData, status] = useVisualizationData(table, visualization)
  const [longLoading, setLongLoading] = useState<boolean>(false)
  const [showStatus, setShowStatus] = useState<ShowStatus>('visible')
  const [resizeLoading, setResizeLoading] = useState<boolean>(false)

  // Reset longLoading as soon as status leaves 'loading', without a synchronous
  // setState in an effect body (which would cause an extra cascading render).
  // This is the React-documented "adjusting state when a prop changes" pattern:
  // detect the transition during render and update state immediately, instead
  // of doing it in a useEffect after commit.
  const [prevStatus, setPrevStatus] = useState(status)
  if (status !== prevStatus) {
    setPrevStatus(status)
    if (status !== 'loading') setLongLoading(false)
  }

  useEffect(() => {
    if (status !== 'loading') return
    const timer = setTimeout((): void => {
      setLongLoading(true)
    }, 1000)

    return () => clearTimeout(timer)
  }, [status])

  function toggleDouble (): void {
    setResizeLoading(true)
    if (showStatus === 'visible') {
      setShowStatus('double')
    } else {
      setShowStatus('visible')
    }
    setTimeout(() => {
      setResizeLoading(false)
    }, 150)
  }

  const canDouble = doubleTypes.includes(visualization.type)
  const { errorMsg, noDataMsg } = useMemo(() => prepareTexts(locale), [locale])

  if (visualizationData == null && status === 'loading') {
    if (longLoading) return <Loader />
    return <div />
  }
  if (status === 'error') {
    return <div className='flex justify-center items-center text-error'>{errorMsg}</div>
  }

  let height = visualization.height ?? 250
  if (showStatus === 'double') height = height * 2

  return (
    <div className=' max-w overflow-hidden  bg-grey6 rounded-md border-[0.2rem] border-grey4'>
      <div className='flex justify-between'>
        <div className='font-bold p-3'>{resolveFlatText(visualization.title, locale)}</div>
        <button onClick={toggleDouble} className={showStatus !== 'hidden' && canDouble ? 'text-primary' : 'hidden'}>
          {showStatus === 'double' ? zoomOutIcon : zoomInIcon}
        </button>
      </div>
      <div className='w-full overflow-auto'>
        <div className='flex flex-col '>
          <div
            // ref={ref}
            className='grid relative z-50 w-full pr-1  min-w-[250px]'
            style={{ gridTemplateRows: String(height) + 'px' }}
          >
            <RenderVisualization
              visualizationData={visualizationData}
              fallbackMessage={noDataMsg}
              loading={resizeLoading}
              locale={locale}
            />
          </div>
        </div>
      </div>
    </div>
  )
}

export const RenderVisualization = memo(
  ({
    visualizationData,
    fallbackMessage,
    loading,
    locale
  }: {
    visualizationData: VisualizationData | undefined
    fallbackMessage: string
    loading: boolean
    locale: string
  }): ReactElement | null => {
    if (visualizationData == null) return null

    const fallback = <div className='m-auto font-bodybold text-4xl text-grey2 '>{fallbackMessage}</div>

    if (loading) return null

    if (['line', 'bar', 'area'].includes(visualizationData.type)) {
      const chartVisualizationData: ChartVisualizationData = visualizationData as ChartVisualizationData
      if (chartVisualizationData.data.length === 0) return fallback
      return <RechartsGraph visualizationData={chartVisualizationData} locale={locale} />
    }

    if (visualizationData.type === 'wordcloud') {
      const textVisualizationData: TextVisualizationData = visualizationData
      if (textVisualizationData.topTerms.length === 0) return fallback
      return <VisxWordcloud visualizationData={textVisualizationData} />
    }

    return null
  }
)

function prepareTexts (locale: string): Record<string, string> {
  const texts = {
    errorMsg: {
      en: 'Could not create visualization',
      nl: 'Kon visualisatie niet maken',
      de: 'Visualisierung konnte nicht erstellt werden',
      it: 'Impossibile creare la visualizzazione',
      es: 'No se ha podido crear la visualización'
    },
    noDataMsg: {
      en: 'No data',
      nl: 'Geen data',
      de: 'Keine Daten',
      it: 'Nessun dato',
      es: 'Sin datos'
    }
  }

  return {
    errorMsg: resolveFlatText(texts.errorMsg, locale),
    noDataMsg: resolveFlatText(texts.noDataMsg, locale)
  }
}
