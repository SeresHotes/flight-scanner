// Статус джобы сбора — GET /api/jobs/{id}.

// Текущий этап сбора (api/worker.py StageReporter). null — у джоб, заведённых до
// появления этапов.
export type JobStageKey = 'queued' | 'fetch' | 'build' | 'save'

export interface JobStage {
  key: JobStageKey
  stages: { key: JobStageKey; label: string }[]
  // Шаг загрузки: переход i из count и сколько его запросов уже сделано.
  step: { index: number; count: number; label: string; done: number; total: number } | null
  cached: number // сколько ответов взято из кэша (без обращения к API)
  flights: number | null // сколько рейсов загружено (после этапа загрузки)
}

export interface JobStatus {
  status: 'pending' | 'running' | 'done' | 'error' | 'not_found'
  progress: number
  total: number
  error?: string | null
  stage?: JobStage | null
}

const API_BASE = import.meta.env.VITE_API_BASE ?? '/api'

export async function fetchJob(jobId: string): Promise<JobStatus> {
  const res = await fetch(`${API_BASE}/jobs/${jobId}`)
  if (!res.ok) throw new Error(`Не удалось получить статус джобы: ${res.status}`)
  return (await res.json()) as JobStatus
}
