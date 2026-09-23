// Статус джобы сбора — GET /api/jobs/{id}.

export interface JobStatus {
  status: 'pending' | 'running' | 'done' | 'error' | 'not_found'
  progress: number
  total: number
  error?: string | null
}

const API_BASE = import.meta.env.VITE_API_BASE ?? '/api'

export async function fetchJob(jobId: string): Promise<JobStatus> {
  const res = await fetch(`${API_BASE}/jobs/${jobId}`)
  if (!res.ok) throw new Error(`Не удалось получить статус джобы: ${res.status}`)
  return (await res.json()) as JobStatus
}
