import { VisualizationType, VisualizationData, Table } from '../types'
import { useEffect, useState } from 'react'
import { selectVisualizationColumns } from './selectVisualizationColumns'

type Status = 'loading' | 'success' | 'error'

export default function useVisualizationData (
  table: Table,
  visualization: VisualizationType
): [VisualizationData | undefined, Status] {
  const [visualizationData, setVisualizationData] = useState<VisualizationData>()
  const [status, setStatus] = useState<Status>('loading')

  useEffect(() => {
    if (window.Worker === undefined) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- PENDING_ISSUES "lint hygiene" entry 2026-08-26: useVisualizationData status resets are tied to the ephemeral-worker spawn/terminate sequence this effect owns (ADR-0032); splitting the status resets out into a render-phase pattern risks a redesign of the worker lifecycle in the exact file the ADR-0032 checks scrutinize (selectVisualizationColumns-before-postMessage, worker.terminate() on every path).
      setStatus('error')
      return
    }
    setStatus('loading')
    // Spawn a worker per computation and terminate it as soon as it answers,
    // instead of keeping a persistent worker holding a clone of the table
    // alive for the lifetime of the figure (issue #122).
    const worker = new Worker(
      new URL('./visualizationDataWorker.ts', import.meta.url), { type: 'module' })
    worker.onmessage = (e: MessageEvent<{ status: Status, visualizationData: VisualizationData }>) => {
      setVisualizationData(e.data.visualizationData)
      setStatus(e.data.status)
      worker.terminate()
    }
    worker.onerror = () => {
      setStatus('error')
      worker.terminate()
    }
    worker.postMessage({ table: selectVisualizationColumns(table, visualization), visualization })
    return () => {
      worker.terminate()
    }
  }, [table, visualization])

  return [visualizationData, status]
}
