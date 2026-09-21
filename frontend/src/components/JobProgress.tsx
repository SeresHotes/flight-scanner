import { useEffect } from 'react'
import { useJob } from '../hooks/useJob'

// Прогресс фонового сбора данных. По завершении дёргает onDone (пере-запрос /search).
export function JobProgress({ jobId, onDone }: { jobId: string; onDone: () => void }) {
  const { data } = useJob(jobId)

  useEffect(() => {
    if (data?.status === 'done') onDone()
  }, [data?.status, onDone])

  const total = data?.total ?? 0
  const progress = data?.progress ?? 0
  const pct = total ? Math.round((100 * progress) / total) : 0

  if (data?.status === 'error') {
    return (
      <div className="backend-note">
        <div className="bn-title">Ошибка сбора данных</div>
        <div>{data.error ?? 'Неизвестная ошибка'}</div>
      </div>
    )
  }

  return (
    <div className="backend-note">
      <div className="bn-title">⏳ Собираем данные с Aviasales…</div>
      <div>
        Выполнено запросов: <b>{progress}</b> из <b>{total || '?'}</b>
      </div>
      <div className="progressbar">
        <div className="progressbar-fill" style={{ width: `${pct}%` }} />
      </div>
      <div style={{ marginTop: 8, fontSize: 12 }}>
        Собираем прямые и стыковочные плечи в обе стороны — это займёт до минуты.
      </div>
    </div>
  )
}
