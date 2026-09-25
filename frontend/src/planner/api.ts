// Реальный сбор цепочки через бэк: POST /api/plan/gather заводит джобу, затем
// поллим GET /api/plan/jobs/{id} до готовности и отдаём собранные Itinerary.
// Сигнатура повторяет прежний runMockCollection — императивный запуск с функцией
// отмены (на случай размонтирования / правки маршрута).

import type { Itinerary, PlannerBounds, PlannerStop } from './types'

const API_BASE = import.meta.env.VITE_API_BASE ?? '/api'
const POLL_MS = 1000

interface GatherResponse {
  status: 'collecting' | 'invalid' | 'too_wide'
  job_id?: string
  total?: number
  message?: string
}

interface PlanJob {
  status: 'pending' | 'running' | 'done' | 'error' | 'not_found'
  progress: number
  total: number
  error?: string | null
  itineraries?: Itinerary[]
}

// Бэку нужны только коды городов (kind/window). Имена/флаги он подставит из справочника.
function stopsPayload(stops: PlannerStop[]) {
  return stops.map((s) => ({ kind: s.kind, codes: s.airports.map((a) => a.code), window: s.window }))
}

export function runCollection(
  stops: PlannerStop[],
  bounds: PlannerBounds,
  onProgress: (progress: number, total: number) => void,
  onDone: (itineraries: Itinerary[]) => void,
  onError: (message: string) => void,
): () => void {
  let cancelled = false
  let timer: ReturnType<typeof setTimeout> | undefined

  const poll = async (jobId: string) => {
    if (cancelled) return
    try {
      const res = await fetch(`${API_BASE}/plan/jobs/${jobId}`)
      if (!res.ok) throw new Error(String(res.status))
      const job = (await res.json()) as PlanJob
      if (cancelled) return

      if (job.status === 'done') {
        onProgress(job.total, job.total)
        onDone(job.itineraries ?? [])
        return
      }
      if (job.status === 'error' || job.status === 'not_found') {
        onError(job.error || 'Сбор не удался — попробуйте ещё раз.')
        return
      }
      onProgress(job.progress, job.total || 1)
      timer = setTimeout(() => poll(jobId), POLL_MS)
    } catch {
      if (!cancelled) onError('Потеряна связь с сервером во время сбора.')
    }
  }

  ;(async () => {
    try {
      const res = await fetch(`${API_BASE}/plan/gather`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          stops: stopsPayload(stops),
          max_results: bounds.maxResults,
          max_cost: bounds.maxCost,
        }),
      })
      if (!res.ok) throw new Error(String(res.status))
      const data = (await res.json()) as GatherResponse
      if (cancelled) return

      if (data.status !== 'collecting' || !data.job_id) {
        onError(data.message || 'Не удалось запустить сбор.')
        return
      }
      onProgress(0, data.total ?? 1)
      timer = setTimeout(() => poll(data.job_id as string), POLL_MS)
    } catch {
      if (!cancelled) onError('Не удалось связаться с сервером.')
    }
  })()

  return () => {
    cancelled = true
    if (timer) clearTimeout(timer)
  }
}
