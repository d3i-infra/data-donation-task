import { VisualizationType, VisualizationData, Table } from '../types'
import { useEffect, useState } from 'react'

type Status = 'loading' | 'success' | 'error'

export default function useVisualizationData (
  table: Table,
  visualization: VisualizationType
): [VisualizationData | undefined, Status] {
  const [visualizationData, setVisualizationData] = useState<VisualizationData>()
  const [status, setStatus] = useState<Status>('loading')
  const [worker, setWorker] = useState<Worker>()

  useEffect(() => {
    const worker = new Worker(new URL('./visualizationDataWorker.ts', import.meta.url))
    setWorker(worker)
    return () => {
      worker.terminate()
    }
  }, [])

  useEffect(() => {
    if (worker != null && window.Worker !== undefined) {
      setStatus('loading')
      worker.onmessage = (e: MessageEvent<{ status: Status, visualizationData: VisualizationData }>) => {
        try {
          setVisualizationData(e.data.visualizationData)
          setStatus(e.data.status)
        } catch (e) {
          setVisualizationData(undefined)
          setStatus('error')
        }
      }
      worker.postMessage({ table, visualization })
    }
  }, [table, visualization, worker])

  return [visualizationData, status]
}
