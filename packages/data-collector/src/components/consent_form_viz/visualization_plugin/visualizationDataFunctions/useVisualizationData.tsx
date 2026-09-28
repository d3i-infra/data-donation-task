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
    // Keep the previous result visible while this computation runs (stale-while-revalidate):
    // react.dev, "Synchronizing with Effects" > "Fetching data" documents exactly this shape —
    // an `ignore` flag set in the cleanup and checked before committing a result — for a race
    // between an old and a new async computation. `table`/`visualization` can change on every
    // debounced search or delete/undo (table_container.tsx), so a slower stale computation must
    // never overwrite a newer one; without the flag, `worker.terminate()` alone would have to be
    // trusted to always beat an already-queued `onmessage`, which is a timing assumption, not a
    // guarantee. (ts-fix2-review check 5 — the earlier "clear on new computation" version of
    // this effect traded stale-while-revalidate for a blank-and-reflow on every keystroke/delete,
    // which react.dev's own "Showing stale content while fresh content is loading" guidance
    // argues against; reverted.)
    let ignore = false
    // Spawn a worker per computation and terminate it as soon as it answers,
    // instead of keeping a persistent worker holding a clone of the table
    // alive for the lifetime of the figure (issue #122).
    const worker = new Worker(
      new URL('./visualizationDataWorker.ts', import.meta.url), { type: 'module' })
    worker.onmessage = (e: MessageEvent<{ status: Status, visualizationData: VisualizationData }>) => {
      if (ignore) return
      setVisualizationData(e.data.visualizationData)
      setStatus(e.data.status)
      worker.terminate()
    }
    worker.onerror = () => {
      if (ignore) return
      setStatus('error')
      worker.terminate()
    }
    worker.postMessage({ table: selectVisualizationColumns(table, visualization), visualization })
    return () => {
      ignore = true
      worker.terminate()
    }
  }, [table, visualization])

  return [visualizationData, status]
}
