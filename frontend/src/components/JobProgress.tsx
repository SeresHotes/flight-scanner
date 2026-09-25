import { useEffect } from 'react'
import { useJob } from '../hooks/useJob'
import { StageProgress } from './StageProgress'

// Прогресс фонового сбора данных. По завершении дёргает onDone (пере-запрос /search).
export function JobProgress({ jobId, onDone }: { jobId: string; onDone: () => void }) {
  const { data } = useJob(jobId)

  useEffect(() => {
    if (data?.status === 'done') onDone()
  }, [data?.status, onDone])

  if (data?.status === 'error') {
    return (
      <div className="backend-note">
        <div className="bn-title">Ошибка сбора данных</div>
        <div>{data.error ?? 'Неизвестная ошибка'}</div>
      </div>
    )
  }

  return (
    <StageProgress
      title="Собираем данные с Aviasales…"
      progress={data?.progress ?? 0}
      total={data?.total ?? 0}
      stage={data?.stage}
      stepWord="Шаг"
    />
  )
}
