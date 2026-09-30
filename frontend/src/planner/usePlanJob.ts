import { useEffect, useState } from 'react'
import type { JobStage } from '../data/jobsApi'
import { fetchPlanJob } from './api'
import { filtersParam, type PlanQuery } from './query'

const POLL_MS = 1000

export interface PlanJobState {
  status: 'loading' | 'running' | 'done' | 'error'
  progress: number
  total: number
  stage?: JobStage | null
  error?: string
  summary?: { count: number; combos: number; totalCount: number }
}

// Поллинг статуса джобы планировщика до готовности (или ошибки): сбор рейсов, затем
// стыковка под фильтры query (у готовой джобы с новыми фильтрами — только стыковка).
export function usePlanJob(jobId: string | undefined, query: PlanQuery | null): PlanJobState {
  const [state, setState] = useState<PlanJobState>({ status: 'loading', progress: 0, total: 1 })
  const fkey = query ? filtersParam(query) : ''

  useEffect(() => {
    if (!jobId || !query) return
    let cancelled = false
    let timer: ReturnType<typeof setTimeout> | undefined
    setState({ status: 'loading', progress: 0, total: 1 })

    const poll = async () => {
      try {
        const job = await fetchPlanJob(jobId, query)
        if (cancelled) return
        if (job.status === 'done') {
          setState({ status: 'done', progress: job.total, total: job.total, stage: job.stage, summary: job.summary })
          return
        }
        if (job.status === 'error' || job.status === 'not_found') {
          setState({ status: 'error', progress: 0, total: 1, error: job.error || 'Сбор не удался — попробуйте ещё раз.' })
          return
        }
        setState({ status: 'running', progress: job.progress, total: job.total || 1, stage: job.stage })
        timer = setTimeout(poll, POLL_MS)
      } catch {
        if (!cancelled) setState({ status: 'error', progress: 0, total: 1, error: 'Потеряна связь с сервером.' })
      }
    }
    void poll()
    return () => {
      cancelled = true
      if (timer) clearTimeout(timer)
    }
  }, [jobId, fkey]) // eslint-disable-line react-hooks/exhaustive-deps

  return state
}
