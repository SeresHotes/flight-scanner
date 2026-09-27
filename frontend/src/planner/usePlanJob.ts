import { useEffect, useState } from 'react'
import type { JobStage } from '../data/jobsApi'
import { fetchPlanJob } from './api'

const POLL_MS = 1000

export interface PlanJobState {
  status: 'loading' | 'running' | 'done' | 'error'
  progress: number
  total: number
  stage?: JobStage | null
  error?: string
  summary?: { count: number; combos: number; totalCount: number }
}

// Поллинг статуса джобы планировщика до готовности (или ошибки).
export function usePlanJob(jobId: string | undefined): PlanJobState {
  const [state, setState] = useState<PlanJobState>({ status: 'loading', progress: 0, total: 1 })

  useEffect(() => {
    if (!jobId) return
    let cancelled = false
    let timer: ReturnType<typeof setTimeout> | undefined
    setState({ status: 'loading', progress: 0, total: 1 })

    const poll = async () => {
      try {
        const job = await fetchPlanJob(jobId)
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
  }, [jobId])

  return state
}
