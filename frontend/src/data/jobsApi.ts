// Статус джобы планировщика — GET /api/plan/jobs/{id} (api/worker.py StageReporter).

export type JobStageKey = 'queued' | 'fetch' | 'build' | 'combos'

export interface JobStage {
  key: JobStageKey
  stages: { key: JobStageKey; label: string }[]
  // Шаг загрузки: переход i из count и сколько его страниц уже получено.
  step: { index: number; count: number; label: string; done: number; total: number } | null
  cached: number // сколько серий взято из кэша (без обращения к источнику)
  flights: number | null // сколько рейсов загружено (после этапа загрузки)
  // Стыковка цепочек: сколько уже найдено из лимита и сколько вариантов перебрано.
  build?: { found: number; limit: number | null; explored: number } | null
}

export interface JobStatus {
  status: 'pending' | 'running' | 'done' | 'error' | 'not_found'
  progress: number
  total: number
  error?: string | null
  stage?: JobStage | null
}
