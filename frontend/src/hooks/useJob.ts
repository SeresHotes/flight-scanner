import { useQuery } from '@tanstack/react-query'
import { fetchJob, type JobStatus } from '../data/jobsApi'

// Пуллинг статуса джобы, пока она не завершилась.
export function useJob(jobId: string | null) {
  return useQuery({
    queryKey: ['job', jobId],
    queryFn: () => fetchJob(jobId as string),
    enabled: !!jobId,
    refetchInterval: (query) => {
      const s = (query.state.data as JobStatus | undefined)?.status
      return s === 'done' || s === 'error' || s === 'not_found' ? false : 1500
    },
  })
}
