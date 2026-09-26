import { useEffect, useRef, useState } from 'react'
import type { OverviewResult } from './overview'
import type { OverviewRequest, OverviewResponse } from './overview.worker'
import type { PlanGraph, PlannerFilters } from './types'

// Обзор «наборов городов», посчитанный в воркере. overview — последний готовый
// результат (null, пока по графу не досчитан ни один); pending — идёт пересчёт
// под текущие фильтры. Каждое изменение фильтров прерывает предыдущий расчёт.
export function useOverview(graph: PlanGraph, filters: PlannerFilters) {
  const workerRef = useRef<Worker | null>(null)
  const requestId = useRef(0)
  const [overview, setOverview] = useState<OverviewResult | null>(null)
  const [pending, setPending] = useState(true)

  useEffect(() => {
    const worker = new Worker(new URL('./overview.worker.ts', import.meta.url), { type: 'module' })
    worker.onmessage = (e: MessageEvent<OverviewResponse>) => {
      if (e.data.id !== requestId.current) return // ответ на устаревшие фильтры
      setOverview(e.data.result)
      setPending(false)
    }
    const init: OverviewRequest = { type: 'graph', graph }
    worker.postMessage(init)
    workerRef.current = worker
    setOverview(null)
    return () => {
      worker.terminate()
      workerRef.current = null
    }
  }, [graph])

  // Эффекты выполняются по порядку объявления: воркер под новый граф уже создан.
  useEffect(() => {
    const id = ++requestId.current
    const request: OverviewRequest = { type: 'compute', id, filters }
    setPending(true)
    workerRef.current?.postMessage(request)
  }, [graph, filters])

  return { overview, pending }
}
